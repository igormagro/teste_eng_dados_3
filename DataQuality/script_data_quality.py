import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_KEY = "s3://bucket-silver/tb_cliente"
DQ_METRICS_KEY = "s3://bucket-gold/data_quality_clientes"
PHONE_PATTERN = r"^\([0-9]{2}\)[0-9]{5}-[0-9]{4}$"


def generate_data_quality_report(df: DataFrame) -> DataFrame:
    """
    Gera relatório de qualidade dos dados com base nas métricas:
    - Completude
    - Validade
    - Consistência
    - Atualidade
    """
    df_dq = (
        df
        # Completude
        .withColumn("is_nome_preenchido", F.col("nm_cliente").isNotNull())
        .withColumn(
            "is_endereco_minimo",
            F.col("nm_cidade_cliente").isNotNull()
            & F.col("nm_rua_cliente").isNotNull(),
        )
        .withColumn("has_contato", F.col("num_telefone_cliente").isNotNull())
        # Validade
        .withColumn(
            "is_telefone_valido",
            F.when(F.col("num_telefone_cliente").isNull(), True).otherwise(
                F.col("num_telefone_cliente").rlike(PHONE_PATTERN)
            ),
        )
        .withColumn(
            "is_dt_nascimento_valida",
            F.when(F.col("dt_nascimento_cliente").isNull(), True).otherwise(
                F.col("dt_nascimento_cliente") <= F.current_date()
            ),
        )
        .withColumn("is_tp_pessoa_valido", F.col("tp_pessoa").isin("PF", "PJ"))
        # Consistência
        .withColumn(
            "is_consistente",
            F.when(
                F.col("tp_pessoa") == "PF", F.col("dt_nascimento_cliente").isNotNull()
            ).otherwise(True),
        )
        # Atualidade
        .withColumn(
            "is_atual",
            F.when(F.col("dt_atualizacao").isNull(), False).otherwise(
                F.col("dt_atualizacao") <= F.current_date()
            ),
        )
        # Score simples de qualidade
        .withColumn(
            "quality_score",
            F.expr(
                """
            int(is_nome_preenchido) +
            int(is_endereco_minimo) +
            int(has_contato) +
            int(is_telefone_valido) +
            int(is_dt_nascimento_valida) +
            int(is_tp_pessoa_valido) +
            int(is_consistente) +
            int(is_atual)
        """
            ),
        )
        # Registro considerado "bom"
        .withColumn(
            "is_good_record",
            F.expr(
                """
            is_nome_preenchido AND
            is_endereco_minimo AND
            has_contato AND
            is_telefone_valido AND
            is_dt_nascimento_valida AND
            is_tp_pessoa_valido AND
            is_consistente AND
            is_atual
        """
            ),
        )
    )
    return df_dq


def aggregate_data_quality_metrics(df_dq: DataFrame) -> DataFrame:
    """
    Agrega métricas de qualidade dos dados em nível de dataset.
    """
    metrics = df_dq.agg(
        F.count("*").alias("total_registros"),
        F.sum(F.col("is_good_record").cast("int")).alias("registros_validos"),
        F.sum(F.when(F.col("is_good_record"), 0).otherwise(1)).alias(
            "registros_invalidos"
        ),
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg(F.col("is_telefone_valido").cast("int")).alias("pct_telefone_valido"),
        F.avg(F.col("is_endereco_minimo").cast("int")).alias("pct_endereco_completo"),
        F.avg(F.col("has_contato").cast("int")).alias("pct_com_contato"),
        F.current_timestamp().alias("dt_referencia"),
    )
    return metrics


def run_job(spark: SparkSession, silver_path: str):
    df_silver = spark.read.parquet(silver_path)

    df_dq = generate_data_quality_report(df_silver)
    dq_metrics = aggregate_data_quality_metrics(df_dq)

    dq_metrics.show(truncate=False)

    # opcional: persiste métricas no S3
    # dq_metrics.write.mode("overwrite").parquet(DQ_METRICS_KEY)

# -----------------------------
# Entrypoint Glue
# -----------------------------
def main():
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    run_job(
        spark,
        silver_path=SILVER_KEY,
    )

    job.commit()


if __name__ == "__main__":
    main()
