from dotenv import load_dotenv
from sqlalchemy.engine.base import Engine
import os
import sqlalchemy as sa
import pandas as pd

load_dotenv()  # Carrega variáveis de ambiente do .env
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")



engine = sa.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")


def querys_basicas(engine: sa.engine.base.Engine):
    
    query = 'SELECT * FROM filmes LIMIT 20;' 
    query1 = 'SELECT COUNT(*) FROM filmes;'
    query2 ='SELECT * FROM generos LIMIT 20;'
    query3 ='SELECT COUNT(*) FROM generos;'
    query4 ='SELECT * FROM filmes_generos LIMIT 20;'
    query5 = 'SELECT COUNT(*) FROM filmes_generos;'
    
    try:
        # 1. CORREÇÃO: engine.connect() deve ser chamado com parênteses
        with engine.connect() as connection:
            
            # --- Tabela Filmes ---
            print("--- Verificando dados da tabela filmes ---")

            result = connection.execute(sa.text(query))
            for row in result:
                print(row)
            
            # Execute e leia a contagem
            result_count = connection.execute(sa.text(query1)).fetchone()
            print(f"Total de filmes: {result_count[0]}")
            
            # --- Tabela Generos ---
            print('\n--- Verificando dados da tabela generos ---')
            
            result = connection.execute(sa.text(query2))
            for row in result:
                print(row)
                
            result_count = connection.execute(sa.text(query3)).fetchone()
            print(f"Total de generos: {result_count[0]}")

            # --- Tabela Filmes_Generos ---
            print('\n--- Verificando dados da tabela filmes_generos ---')
            result = connection.execute(sa.text(query4))
            for row in result:
                print(row)
                
            result_count = connection.execute(sa.text(query5)).fetchone()
            print(f"Total de relações: {result_count[0]}")
            
            
            return True 
            
    except Exception as e:
        print(f'Erro ao executar as querys: {e}')
        return False 

def testando_joins(engine: Engine):
    """
    Testa a conexão com o banco de dados MySQL
    """
    try:
        
        # Tenta conectar
        with engine.connect() as connection:
            print("Conexão bem-sucedida!")
            query = """
            SELECT f.id AS id_filme,
                   f.titulo,
                   g.genero,
                  fg.id_genero
            FROM filmes f
            JOIN filmes_generos fg ON f.id = fg.id_filme
            JOIN generos g ON fg.id_genero = g.id
            where f.titulo = "Guerra nas Estrelas";
            """
            result = connection.execute(sa.text(query))
            for row in result:
                print(row)
            return  
            
    except Exception as e:  
        return print(f"Erro ao conectar ao banco de dados: {e}")
    

query_padrao_avaliacao = """
WITH MediaAnualPorGenero AS (
    -- 1. Calcula a Média simples e ponderada Anual para cada Gênero.
    SELECT
        YEAR(f.data_lancamento) AS ano,
        COUNT(f.id) AS filmes_por_genero,
        g.genero,
        ROUND(SUM(f.nota_media*f.total_votos)/SUM(f.total_votos), 2) AS media_ponderada_anual,
        ROUND(AVG(f.nota_media),2) AS media_simples_anual
    FROM
        filmes f
    JOIN
        filmes_generos fg ON fg.id_filme = f.id
    JOIN
        generos g ON g.id = fg.id_genero
	WHERE
		f.total_votos > 0
    GROUP BY
        YEAR(f.data_lancamento), g.genero
)
-- 2. Calcula a Média das Médias Anuais (Consistência ao longo do tempo).
SELECT
    genero,
    SUM(filmes_por_genero) AS numero_filmes,
    MIN(ano) AS ano_inicial,
    MAX(ano) AS ano_final,
    ROUND(SUM(media_ponderada_anual * filmes_por_genero) / SUM(filmes_por_genero), 2) AS media_ponderada_geral,
    ROUND(AVG(media_simples_anual),2) AS media_simples_geral,
	ROUND(
	  (SUM(media_ponderada_anual * filmes_por_genero) / SUM(filmes_por_genero))
	  - AVG(media_simples_anual),
	  2
	) AS diferenca_medias
FROM
    MediaAnualPorGenero
GROUP BY
    genero
ORDER BY
    media_ponderada_geral DESC, media_simples_geral DESC;
    
"""
query_rank_generos = """
WITH MediaAnualPorGenero AS (
    -- 1. Calcula a média da nota para cada Ano e Gênero
    SELECT
        YEAR(f.data_lancamento) AS ano,
        g.genero,
        ROUND(AVG(f.nota_media), 1) AS media_nota_ano_genero
    FROM
        filmes f
    JOIN
        filmes_generos fg ON fg.id_filme = f.id
    JOIN
        generos g ON g.id = fg.id_genero
    GROUP BY
        ano, g.genero
),
ClassificacaoPorAno AS (
    -- 2. Classifica cada gênero DENTRO de cada ano
    SELECT
        ano,
        genero,
        media_nota_ano_genero,
        -- RANK() atribui uma classificação. PARTITION BY ano reseta a contagem para cada novo ano.
        -- ORDER BY media_nota_ano_genero DESC garante que a melhor nota receba o Rank 1.
        RANK() OVER (
            PARTITION BY ano
            ORDER BY media_nota_ano_genero DESC
        ) AS rank_do_genero
    FROM
        MediaAnualPorGenero
)
-- 3. Seleciona apenas os 5 melhores de cada ano
SELECT
    ano,
    rank_do_genero,
    genero,
    media_nota_ano_genero
FROM
    ClassificacaoPorAno
WHERE
    rank_do_genero <= 5
ORDER BY
    ano DESC,
    rank_do_genero;
"""
df = pd.read_sql_query(query_padrao_avaliacao, engine)
df1 =pd.read_sql_query(query_rank_generos, engine)
print(df)
print('\n______________________________________________\n')
print(df1)