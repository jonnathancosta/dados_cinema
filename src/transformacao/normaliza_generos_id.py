from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

def criar_spark_session():
    """
    Cria e retorna uma sessão Spark
    """
    return SparkSession.builder \
        .appName("TMDb ETL - Generos") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

def ler_generos_json(spark, caminho_arquivo):
    """
    Lê o arquivo JSON de gêneros e retorna um DataFrame
    """
    # Lê o arquivo JSON deixando o Spark inferir o schema
    df = spark.read \
        .option("multiline", "true") \
        .json(caminho_arquivo)
    
    return df

def executar_ingestao_generos_ids():
    """
    Função principal que coordena o processo de ingestão
    """
    load_dotenv()  # Carrega variáveis de ambiente do .env
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE")
    try:
        # Cria a sessão Spark
        spark = criar_spark_session()
        
        # Caminho para o arquivo JSON
        caminho_arquivo = "data/bronze/generos.json"
        
        # Lê os gêneros
        df_generos = ler_generos_json(spark, caminho_arquivo)
        
        print("[INFO] Esquema do DataFrame:")
        df_generos.printSchema()
        
        print("\n[INFO] Amostra dos dados:")
        df_generos.show(5, truncate=False)
        df_generos.write.mode("overwrite").parquet("data/silver/tabela_generos")
        # Configuração do MySQL
        url = f"jdbc:mysql://{host}:{port}/{database}"
        properties = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        try:
            # Grava no MySQL
            df_generos.write \
                .mode("append") \
                .jdbc(
                    url=url,
                    table="generos",
                    properties=properties
                )
            print("[INFO] ✅ Dados de generos carregados no MySQL com sucesso!")
            
        except Exception as e:
            print(f"[ERRO] ❌ Falha ao gravar no MySQL: {str(e)}")
            
    except Exception as e:
        print(f"[ERRO] ❌ Falha durante o processamento: {str(e)}")
        
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\n[INFO] ✨ Sessão Spark encerrada")

