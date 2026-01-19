from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

from ETL.script import to_bronze, to_silver


SCHEMA = StructType([
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
])


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("dq-tests")
        .getOrCreate()
    )


def test_happy_path_dedup_and_valid_phone(spark):
    """
    Testa o caminho feliz para deduplicação e validação de telefone.
    """
    data = [
        (1, "Ana", None, "SP", "Rua A", 10, "(11)99999-1234", date(2020, 1, 1), date(2024, 1, 1), "PF", 1000.0),
        (1, "Ana", None, "SP", "Rua A", 10, "(11)88888-0000", date(2020, 1, 1), date(2023, 1, 1), "PF", 900.0),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    bronze = to_bronze(df)
    silver = to_silver(bronze)

    rows = silver.collect()

    assert len(rows) == 1
    assert rows[0]["cod_cliente"] == 1
    assert rows[0]["num_telefone_cliente"] == "(11)99999-1234"


def test_invalid_phone_becomes_null(spark):
    """
    Testa que um telefone inválido se torna None.
    """
    data = [
        (2, "Bruno", None, "RJ", "Rua B", 20, "11999991234", date(1995, 5, 5), date(2024, 1, 1), "PF", 2000.0),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    bronze = to_bronze(df)
    silver = to_silver(bronze)

    row = silver.collect()[0]
    assert row["num_telefone_cliente"] is None


def test_edge_case_same_update_date(spark):
    """
    Testa o caso onde há duplicatas com a mesma data de atualização.
    Deve manter apenas um registro.
    """
    data = [
        (3, "Carla", None, "MG", "Rua C", 30, "(31)99999-0000", date(1990, 1, 1), date(2024, 1, 1), "PF", 3000.0),
        (3, "Carla", None, "MG", "Rua C", 30, "(31)99999-1111", date(1990, 1, 1), date(2024, 1, 1), "PF", 3100.0),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    bronze = to_bronze(df)
    silver = to_silver(bronze)

    rows = silver.collect()

    assert len(rows) == 1
    assert rows[0]["cod_cliente"] == 3


def test_null_phone_is_preserved(spark):
    """
    Testa que um telefone None permanece None.
    """
    data = [
        (4, "Daniela", None, "SP", "Rua D", 40, None, date(1980, 1, 1), date(2024, 1, 1), "PF", 4000.0),
    ]

    df = spark.createDataFrame(data, SCHEMA)
    bronze = to_bronze(df)
    silver = to_silver(bronze)

    row = silver.collect()[0]
    assert row["num_telefone_cliente"] is None
