import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_KEY = "s3://bucket-silver/tb_cliente"
phone_pattern = r'^\([0-9]{2}\)[0-9]{5}-[0-9]{4}$'

df = spark.read.parquet(SILVER_KEY)

# Regras de qualidade por dimensão
df_dq = (
    df
    # Completude
    .withColumn("is_nome_preenchido", F.col("nm_cliente").isNotNull())
    .withColumn(
        "is_endereco_minimo",
        F.col("nm_cidade_cliente").isNotNull() & F.col("nm_rua_cliente").isNotNull()
    )
    .withColumn("has_contato", F.col("num_telefone_cliente").isNotNull())

    # Validade
    .withColumn(
        "is_telefone_valido",
        F.when(
            F.col("num_telefone_cliente").isNull(), True
        ).otherwise(F.col("num_telefone_cliente").rlike(phone_pattern))
    )
    .withColumn(
        "is_dt_nascimento_valida",
        F.when(
            F.col("dt_nascimento_cliente").isNull(), True
        ).otherwise(F.col("dt_nascimento_cliente") <= F.current_date())
    )
    .withColumn(
        "is_tp_pessoa_valido",
        F.col("tp_pessoa").isin("PF", "PJ")
    )

    # Consistência
    .withColumn(
        "is_consistente",
        F.when(
            F.col("tp_pessoa") == "PF",
            F.col("dt_nascimento_cliente").isNotNull()
        ).otherwise(True)
    )

    # Atualidade
    .withColumn(
        "is_atual",
        F.when(
            F.col("dt_atualizacao").isNull(), False
        ).otherwise(F.col("dt_atualizacao") <= F.current_date())
    )

    # Score simples de qualidade
    .withColumn(
        "quality_score",
        F.expr("""
            int(is_nome_preenchido) +
            int(is_endereco_minimo) +
            int(has_contato) +
            int(is_telefone_valido) +
            int(is_dt_nascimento_valida) +
            int(is_tp_pessoa_valido) +
            int(is_consistente) +
            int(is_atual)
        """)
    )

    # Registro considerado "bom"
    .withColumn(
        "is_good_record",
        F.expr("""
            is_nome_preenchido AND
            is_endereco_minimo AND
            has_contato AND
            is_telefone_valido AND
            is_dt_nascimento_valida AND
            is_tp_pessoa_valido AND
            is_consistente AND
            is_atual
        """)
    )
)

# Métricas agregadas de qualidade
metrics = (
    df_dq.agg(
        F.count("*").alias("total_registros"),
        F.sum(F.col("is_good_record").cast("int")).alias("registros_validos"),
        F.sum(F.when(F.col("is_good_record"), 0).otherwise(1)).alias("registros_invalidos"),
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg(F.col("is_telefone_valido").cast("int")).alias("pct_telefone_valido"),
        F.avg(F.col("is_endereco_minimo").cast("int")).alias("pct_endereco_completo"),
        F.avg(F.col("has_contato").cast("int")).alias("pct_com_contato"),
        F.current_timestamp().alias("dt_referencia")
    )
)

metrics.show(truncate=False)

job.commit()