from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date, current_date, trim
from dotenv import load_dotenv
from pathlib import Path
import os
import glob
import shutil
import pandas as pd

def criar_spark_session():
    """
    Cria e retorna uma sessão Spark
    """
    return SparkSession.builder \
        .appName("TMDb ETL - FIlmes") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.python.worker.faulthandler.enabled", "true") \
        .config("spark.hadoop.io.nativeio.NativeIO.disable.load", "true") \
        .getOrCreate()
        
        
        
def salvar_filmes(df, nome_arquivo: str, mode: str = "overwrite") -> bool:
    """
    Salva um Spark DataFrame em formato Parquet.

    Args:
        df: DataFrame do Spark contendo os dados
        nome_arquivo: Caminho de saída (ex: 'dados/filmes.parquet' ou 'dados/filmes/')
        mode: Modo de gravação ('overwrite', 'append', 'ignore', 'error')
    """
    if df is None:
        print("Nenhum filme para salvar!")
        return False

    try:
        caminho_arquivo = Path(nome_arquivo)
        caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)

        # Salva o DataFrame em Parquet
        df.write.mode(mode).parquet(str(caminho_arquivo))

        print(f"✅ {df.count()} filmes salvos em {nome_arquivo}")
        return True

    except Exception as e:
        print(f" Erro ao salvar filmes em Parquet: {e}")
        return False
    
    # Exemplo de dados
dados = [
    {"titulo": "Matrix", "ano": 1999, "nota": 8.7},
    {"titulo": "O Senhor dos Anéis", "ano": 2001, "nota": 8.9},
    {"titulo": "Inception", "ano": 2010, "nota": 8.8}
]

spark = criar_spark_session()

prefixo_diretorio = "data/silver/"
caminho_completo = prefixo_diretorio + "tabela_filmes.parquet"
# Cria o DataFrame Spark
df = spark.createDataFrame(dados)

# Chama a função
salvar_filmes(df, caminho_completo, mode="overwrite")