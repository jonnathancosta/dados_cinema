from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_date, current_date, trim
from dotenv import load_dotenv
import os
import glob



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
        .config("spark.hadoop.io.nativeio.NativeIO.disable.load", "true") \
        .getOrCreate()

os.environ["HADOOP_OPTS"] = "-Djava.library.path="

def limpar_dados(df):
    count_inicial = df.count()
    df = df.dropDuplicates(["id"])
    cond = (
        col("release_date").isNotNull() & (trim(col("release_date")) != "") &
        col("title").isNotNull() & (trim(col("title")) != "") &
        col("genre_ids").isNotNull() &
        (col("release_date") <= current_date())
    )
    df_filtrado = df.filter(cond).fillna(0, subset=["vote_average", "vote_count"])

    count_final = df_filtrado.count()
    print(f"🧹 Registros removidos: {count_inicial - count_final}")

    return df_filtrado


def ler_filmes_json(spark, path):
    """
    Lê o arquivo JSON de gêneros e retorna um DataFrame
    """
    arquivos_jsonl = glob.glob(os.path.join(path, "*.jsonl"))
            
    if not arquivos_jsonl:
        print(f"Nenhum arquivo JSONL encontrado em: {path}")
        return None
    
    # Lê o primeiro arquivo para criar o DataFrame inicial
    df = spark.read.json(arquivos_jsonl[0])
    
    # Se houver mais arquivos, faz union com os demais
    for arquivo in arquivos_jsonl[1:]:
        df_temp = spark.read.json(arquivo)
        df = df.union(df_temp)
            
    df = limpar_dados(df)    
        
    df = df.selectExpr(
        "id as id",
        "title as titulo",
        "original_language as idioma_original",
        "release_date as data_lancamento",
        "popularity as popularidade",
        "vote_average as nota_media",
        "vote_count as total_votos"
    )
    

    return df



def ler_generos_validos(spark, url, properties):
    """
    Lê os IDs de gêneros válidos da tabela 'generos'
    """
    try:
        df_generos = spark.read.jdbc(
            url=url,
            table="generos",
            properties=properties
        )
         
        generos = [row.id for row in df_generos.select("id").toLocalIterator()]
        
        return generos

    except Exception as e:
        print(f"[AVISO] ⚠️ Não foi possível ler os gêneros: {str(e)}")
        return []



def ler_filmes_generos_json(spark, path):
    
    """
    Lê o arquivo JSON de filmes e retorna um DataFrame apenas com IDs e gêneros
    """
    arquivos_jsonl = glob.glob(os.path.join(path, "*.jsonl"))
            
    if not arquivos_jsonl:
        print(f"Nenhum arquivo JSONL encontrado em: {path}")
        return None
    
    # Lê o primeiro arquivo para criar o DataFrame inicial
    df = spark.read.json(arquivos_jsonl[0])
    # Se houver mais arquivos, faz union com os demais
    for arquivo in arquivos_jsonl[1:]:
        df_temp = spark.read.json(arquivo)
        df = df.union(df_temp)
    
    df = limpar_dados(df)
    
    df = df.select("id", "genre_ids") \
        .withColumn("id_genero", explode("genre_ids")) \
        .selectExpr("id as id_filme", "id_genero") 
    
    df = df.drop_duplicates()
    return df

def executar_ingestao_filmes():
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
        
        # Encontra todos os arquivos JSONL
        path = "data/bronze/dados_brutos_filmes"
        df_filmes = ler_filmes_json(spark, path)
        df_filmes_generos = ler_filmes_generos_json(spark, path)
        # Configuração do MySQL
        url = f"jdbc:mysql://{host}:{port}/{database}"
        properties = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }


        if df_filmes is None:
            print("[AVISO] ⚠️ Nenhum dado para processar")
            return

        # Conta total de registros antes do processamento
        total_registros = df_filmes.count()
        print(f"[INFO] 📊 Total de linhas sobre filmes a processar: {total_registros}")

        # Configura propriedades adicionais
        properties.update({
            "batchsize": "1000",
            "rewriteBatchedStatements": "true"
        })

        # Coalesce para uma única partição
        df_filmes = df_filmes.coalesce(1)

        try:
            print("[INFO] 📝 Iniciando inserção dos dados...")
            
            # Cria uma visão temporária do DataFrame
            
            df_filmes.write \
                .mode("append") \
                .option("createTableColumnTypes", "id BIGINT PRIMARY KEY") \
                .option("queryTimeout", "3600") \
                .jdbc(
                    url=url,
                    table="filmes",
                    properties=properties
                )
            
            print("[INFO] ✅ Dados de filmes inseridos com sucesso!")
            
            try:
            # Lê os gêneros válidos do MySQL
                generos_validos = ler_generos_validos(spark, url, properties)
                
                if not generos_validos:
                    print("[AVISO] ⚠️ Não há gêneros cadastrados na tabela generos")
                    return
                else:
                    print(f"[INFO] ✅ Total de gêneros válidos carregados: {len(generos_validos)}")
                # Filtra apenas os gêneros que existem na tabela generos
                if df_filmes_generos is not None and df_filmes_generos.count() > 0:
                    
                    # Filtra apenas gêneros válidos
                    df_filmes_generos = df_filmes_generos.filter(col("id_genero").isin(generos_validos))
                    total_relacoes = df_filmes_generos.count()
                    print(f"[INFO] 📈 Total de relações filme-gênero: {total_relacoes}")
                    
                    if total_relacoes > 0:
                        # Insere os gêneros válidos
                        df_filmes_generos.write \
                            .mode("append") \
                            .option("createTableColumnTypes", "id BIGINT, id_genero INT") \
                            .option("queryTimeout", "3600") \
                            .jdbc(
                                url=url,
                                table="filmes_generos",
                                properties=properties
                            )      
                            
                        print("[INFO] ✅ Dados de filmes_geneos inseridos com sucesso!")  
            
            except Exception as e:
                print(f"[ERRO] ❌ Falha ao processar filmes_generos: {str(e)}")

        except Exception as e:
            print(f"[ERRO] ❌ Falha ao inserir dados de filmes: {str(e)}")

    except Exception as e:
        print(f"[ERRO] ❌ Falha durante o processamento: {str(e)}")
        
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\n[INFO] ✨ Sessão Spark encerrada")
