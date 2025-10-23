from dotenv import load_dotenv
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
    query6 = """SELECT ROUND(AVG(total_votos),2) AS media_votos,
                idioma_original
                FROM filmes
                GROUP BY idioma_original
                ORDER BY media_votos DESC;
             """
    
    
    try:
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
            
            print("Entendendo padrão de votação através de idioma original dos filmes ")
            
            result = connection.execute(sa.text(query6))
            for row in result:
                print(row)
          
            return True 
            
    except Exception as e:
        print(f'Erro ao executar as querys: {e}')
        return False 

def testando_joins(engine: sa.engine.base.Engine):
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
            return True
            
    except Exception as e:  
         print(f"Erro ao conectar ao banco de dados: {e}")
         return False
    
def view_filmes_lancados_por_ano(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW FILMES_LANCADOS_POR_ANO AS
                WITH filmes_ano AS (
                    SELECT 
                        COUNT(*) AS numero_filmes,
                        YEAR(data_lancamento) AS ano
                    FROM filmes
                    GROUP BY YEAR(data_lancamento)
                )
                SELECT *
                FROM filmes_ano;
            """
    try:
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            print("View FILMES_LANCADOS_POR_ANO criada com sucesso!")
        return True
    
    except Exception as e:
        print(f"Erro ao tentar cria a view FILMES_LANCADOS_POR_ANO {e}")
        return False
                       
def view_padrao_avaliacao(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW padrao_avaliacao AS
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
                                      
            """
    try:
         with engine.connect() as connection:
             connection.execute(sa.text(query))
             
             print("View padrao_avaliacao criada comm sucesso!")
             return True
    
    except Exception as e: 
        print(f"Não foi possivel criar a view padrao_avaliacao {e}")              
        return False

def view_top_filmes_por_genero(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW top_filmes_por_genero AS
                WITH RankingFilmes AS (
                    SELECT
                        g.genero AS genero,
                        f.titulo,
                        f.nota_media,
                        RANK() OVER (
                            PARTITION BY g.genero
                            ORDER BY f.nota_media DESC
                        ) AS ranking
                    FROM
                        filmes f
                    JOIN
                        filmes_generos fg ON f.id = fg.id_filme
                    JOIN
                        generos g ON g.id = fg.id_genero
                    WHERE
                        f.total_votos > 500 -- C
                )
                SELECT
                    genero,
                    titulo,
                    round(nota_media,2) nota_media,
                    ranking
                FROM
                    RankingFilmes
                WHERE
                    ranking <= 3;
            """
    try:
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            
            print("View top_filmes_por_genero criada com sucesso!")
            return True
    
    except Exception as e:
        print(f"Erro ao criar a view top_filmes_por_genero{e}")
        return False

def view_top_generos_por_ano(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW top_generos_por_ano AS
                WITH MediaAnualPorGenero AS (
                    -- 1. Calcula a média da nota para cada Ano e Gênero
                    SELECT
                        YEAR(f.data_lancamento) AS ano,
                        g.genero,
                        ROUND(AVG(f.nota_media), 1) AS media_nota_ano
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
                        media_nota_ano,
                        -- RANK() atribui uma classificação. PARTITION BY ano reseta a contagem para cada novo ano.
                        -- ORDER BY media_nota_ano_genero DESC garante que a melhor nota receba o Rank 1.
                        RANK() OVER (
                            PARTITION BY ano
                            ORDER BY media_nota_ano DESC
                        ) AS rank_do_genero
                    FROM
                        MediaAnualPorGenero
                )
                -- 3. Seleciona apenas os 3 melhores de cada ano
                SELECT
                    ano,
                    rank_do_genero,
                    genero,
                    media_nota_ano
                FROM
                    ClassificacaoPorAno
                WHERE
                    rank_do_genero <= 3;
            """
            
    try:
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            print("View top_generos_por_ano criada com sucesso!")
            return True
        
    except Exception as e:
        print(f"Erro ao criar a view top_generos_por_ano: {e}")

def view_popularidade_vs_nota(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW popularidade_vs_nota_media AS
                    SELECT 
                        f.titulo,
                        f.popularidade,
                        f.nota_media,
                        GROUP_CONCAT(g.genero SEPARATOR ', ') AS generos
                    FROM filmes f
                    JOIN filmes_generos fg ON f.id = fg.id_filme
                    JOIN generos g ON g.id = fg.id_genero
                    GROUP BY f.id, f.titulo, f.popularidade, f.nota_media
                    ORDER BY f.popularidade DESC;
            """
            
            
            
    try: 
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            print("View popularidade_vs_nota_media criada com sucesso!")
            return True
        
    except Exception as e:
        print(f'Erro ao criar a view popularidade_vs_nota_media: {e}')
        return False
    
def view_popularidade_por_ano(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW popularidade_por_ano AS
                    SELECT YEAR(f.data_lancamento) AS ano,
                        ROUND(AVG(f.popularidade), 2) AS media_popularidade,
                        COUNT(*) AS qtd_filmes
                    FROM filmes f
                    GROUP BY YEAR(f.data_lancamento)
                    ORDER BY media_popularidade DESC;
            """
            
    try: 
        with engine.connect() as connection: 
            connection.execute(sa.text(query))
            print('View popularidade_por_ano criada com sucesso!')
            return True
        
    except Exception as e:
        print(f" Erro ao criar a view popularidade_por_ano: {e}")
        return False
        

def view_generos_por_popularidade(engine: sa.engine.base.Engine):
    
    query = """
                CREATE VIEW generos_por_popularidade AS
                    SELECT g.genero,
                        ROUND(AVG(f.popularidade), 2) AS media_popularidade,
                        COUNT(*) AS qtd_filmes
                    FROM filmes f
                    JOIN filmes_generos fg ON f.id = fg.id_filme
                    JOIN generos g ON g.id = fg.id_genero
                    GROUP BY g.genero
                    ORDER BY media_popularidade DESC;
            """
            
    try: 
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            print('View generos_por_popularidade criada com sucesso!')
            return True
        
    except Exception as e:
        print(f'Erro ao criar a view generos_por_popularidade: {e}')
        
def executar_todas_querys(engine: sa.engine.base.Engine):
    """
    Executa todas as querys de verificação e criação de views no banco de dados.
    Retorna um dicionário com o status de cada execução.
    """

    # Lista de funções a serem executadas 
    funcoes = [
        ("Querys básicas", querys_basicas),
        ("Teste de JOINs", testando_joins),
        ("View: Filmes lançados por ano", view_filmes_lancados_por_ano),
        ("View: Padrão de avaliação", view_padrao_avaliacao),
        ("View: Top filmes por gênero", view_top_filmes_por_genero),
        ("View: Top gêneros por ano", view_top_generos_por_ano),
        ("View: Popularidade vs nota média", view_popularidade_vs_nota),
        ("View: Popularidade por ano", view_popularidade_por_ano),
        ("View: Generos por popularidade", view_generos_por_popularidade)
    ]

    resultados = {}

    print("\n=== Iniciando execução de todas as funções ===\n")

    for nome, func in funcoes:
        try:
            print(f"➡️ Executando: {nome}")
            resultado = func(engine)
            if resultado:
                print(f"✅ {nome} concluída com sucesso.\n")
                resultados[nome] = "Sucesso"
            else:
                print(f"⚠️ {nome} retornou False.\n")
                resultados[nome] = "Falhou (retornou False)"
        except Exception as e:
            print(f"❌ Erro ao executar {nome}: {e}\n")
            resultados[nome] = f"Erro: {e}"

    print("\n=== Execução concluída ===\n")

    # Resumo final
    for nome, status in resultados.items():
        print(f"{nome}: {status}")

    return resultados