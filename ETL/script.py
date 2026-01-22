import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
)

# -----------------------------
# Configuração de schema
# -----------------------------
SCHEMA = StructType(
    [
        StructField("cod_cliente", IntegerType(), nullable=False),
        StructField("nm_cliente", StringType(), nullable=True),
        StructField("nm_pais_cliente", StringType(), nullable=True),
        StructField("nm_cidade_cliente", StringType(), nullable=True),
        StructField("nm_rua_cliente", StringType(), nullable=True),
        StructField("num_casa_cliente", IntegerType(), nullable=True),
        StructField("telefone_cliente", StringType(), nullable=True),
        StructField("dt_nascimento_cliente", DateType(), nullable=True),
        StructField("dt_atualizacao", DateType(), nullable=True),
        StructField("tp_pessoa", StringType(), nullable=True),
        StructField("vl_renda", DoubleType(), nullable=True),
    ]
)

PHONE_PATTERN = r"^\([0-9]{2}\)[0-9]{5}-[0-9]{4}$"


def to_bronze(df: DataFrame) -> DataFrame:
    """
    Aplica transformações da camada Bronze.
    """
    return (
        df.withColumn("nm_cliente", F.upper(F.col("nm_cliente")))
        .withColumnRenamed("telefone_cliente", "num_telefone_cliente")
        .withColumn("anomesdia", F.date_format(F.current_date(), "yyyyMMdd"))
    )


def to_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Aplica regras da camada Silver:
    - deduplicação por cod_cliente + dt_atualizacao
    - validação de telefone
    """

    #  na leitura, utilizar dt_atualizacao como particao
    window = Window.partitionBy("cod_cliente").orderBy(F.col("dt_atualizacao").desc())

    df = (
        df_bronze.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df = df.withColumn(
        "num_telefone_cliente",
        F.when(
            F.col("num_telefone_cliente").rlike(PHONE_PATTERN),
            F.col("num_telefone_cliente"),
        ).otherwise(None),
    )

    return df



def run_job(
    spark: SparkSession,
    file_path: str,
    bronze_path: str,
    silver_path: str,
):
    df_raw = spark.read.schema(SCHEMA).option("header", "true").csv(file_path)

    df_bronze = to_bronze(df_raw)
    # criar checkpoint 
    df_silver = to_silver(df_bronze)

    df_bronze.write.mode("append").partitionBy("anomesdia").parquet(bronze_path) # <- overwrite

    df_silver.write.mode("append").partitionBy("anomesdia").parquet(silver_path)


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

    FILE_PATH = "file:///mnt/notebooks/clientes_sinteticos.csv"
    BRONZE_KEY = "s3://bucket-bronze/tabela_cliente_landing"
    SILVER_KEY = "s3://bucket-silver/tb_cliente"

    run_job(
        spark=spark,
        file_path=FILE_PATH,
        bronze_path=BRONZE_KEY,
        silver_path=SILVER_KEY,
    )

    job.commit()


if __name__ == "__main__":
    main()
