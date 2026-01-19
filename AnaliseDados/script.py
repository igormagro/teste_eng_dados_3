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


def most_updated_clients(df: DataFrame, n: int = 5) -> DataFrame:
    """
    Retorna os n clientes com mais atualizações.
    """
    return (
        df.groupBy("cod_cliente")
        .agg(F.count(F.lit(1)).alias("num_atualizacoes"))
        .orderBy(F.col("num_atualizacoes").desc())
        .limit(n)
    )
    

def average_client_age(df: DataFrame) -> float:
    """
    Calcula a idade média dos clientes.
    """
    w = (
        Window
        .partitionBy("cod_cliente")
        .orderBy(F.col("dt_atualizacao").desc())
    )

    df_current = (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    df_with_age = (
        df_current
        .filter(F.col("dt_nascimento_cliente").isNotNull())
        .withColumn(
            "idade_cliente",
            F.floor(F.datediff(F.current_date(), F.col("dt_nascimento_cliente")) / 365.25)
        )
    )

    avg_age = df_with_age.agg(F.avg("idade_cliente")).first()[0]
    return avg_age
    



def run_job(
    spark: SparkSession,
    file_path: str,
):
    df_raw = spark.read.schema(SCHEMA).option("header", "true").csv(file_path)
    
    top_clients = most_updated_clients(df_raw, n=5)
    avg_age = average_client_age(df_raw)
    
    print("Top 5 clientes com mais atualizações:")
    top_clients.show()
    
    print(f"Idade média dos clientes: {avg_age:.2f} anos")
    


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

    run_job(
        spark=spark,
        file_path=FILE_PATH,
    )

    job.commit()


if __name__ == "__main__":
    main()
