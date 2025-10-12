from dotenv import load_dotenv
import os
import sqlalchemy as sa
load_dotenv()  # Carrega variáveis de ambiente do .env
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")




def testar_conexao(engine: sa.engine.base.Engine) -> bool:
    """
    Testa a conexão com o banco de dados MySQL
    """
    try:
        
        # Tenta conectar
        with engine.connect() as connection:
            print("Conexão bem-sucedida!")
            return True
            
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return False

def criar_tabela_filmes(engine: sa.engine.base.Engine) -> bool:
    """
    Cria a tabela 'filmes' no banco de dados MySQL
    """
    try:
        # Define o comando SQL para criar a tabela
        query = """
        CREATE TABLE filmes (
            id int not null  primary key,
            titulo VARCHAR(255),
            idioma_original CHAR(2),
            data_lancamento date,
            nota_media double,
            total_votos double,
            popularidade double
        );
        """
        
        # Executa o comando SQL
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            connection.commit()  # Confirma as alterações no banco de dados
            print("Tabela 'filmes' criada com sucesso!")
            return True
            
    except Exception as e:
        print(f"Erro ao criar a tabela 'filmes': {e}")
        return False
    
def criar_tabela_generos(engine: sa.engine.base.Engine) -> bool:
    """Cria a tabela 'generos' no banco de dados MySQL
    """
    try:
        # Define o comando SQL para criar a tabela
        
        query = """
        create table generos(
        id int not null primary key,
        genero varchar(25)
        );
        """
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            connection.commit()  # Confirma as alterações no banco de dados
            print("Tabela 'generos' criada com sucesso!")
            return True
        
    except Exception as e:
        print(f"Erro ao criar a tabela 'generos': {e}")
        return False

def criar_tabela_filmes_generos(engine: sa.engine.base.Engine) -> bool:
    """Cria a tabela 'filmes_generos' no banco de dados MySQL
    """
    try:
        # Define o comando SQL para criar a tabela
        query = """
        CREATE TABLE filmes_generos (
        id_filme INT NOT NULL,
        id_genero INT NOT NULL,
        PRIMARY KEY (id_filme, id_genero)
        );
        """
        with engine.connect() as connection:
            connection.execute(sa.text(query))
            connection.commit()  # Confirma as alterações no banco de dados
            print("Tabela 'filmes_generos' criada com sucesso!")
            return True
        
    except Exception as e:
        print(f"Erro ao criar a tabela 'filmes_generos': {e}")
        return False
    
def estruturar_tabela_filmes_generos(engine: sa.engine.base.Engine) -> bool:
    """Adiciona chaves estrangeiras à tabela 'filmes_generos' e cria constraints de integridade
    """

    # Instrução 1: Adicionar FK para generos
    query1 = "ALTER TABLE filmes_generos ADD CONSTRAINT fk_id_genero FOREIGN KEY (id_genero) REFERENCES generos(id)"
    
    # Instrução 2: Adicionar FK para filmes
    query2 = "ALTER TABLE filmes_generos ADD CONSTRAINT fk_id_filme FOREIGN KEY (id_filme) REFERENCES filmes(id)"

    try:
        with engine.connect() as connection:
            # Executa a primeira instrução
            connection.execute(sa.text(query1))
            
            # Executa a segunda instrução
            connection.execute(sa.text(query2))
            
            # Confirma as alterações no banco de dados
            connection.commit() 
            
            print("✅ Chaves estrangeiras e constraints adicionadas com sucesso à tabela 'filmes_generos'!")
            return True
        
    except Exception as e:
        # A exceção OperationalError pode ocorrer se a constraint já existir
        print(f"❌ Erro ao adicionar chaves estrangeiras e constraints à tabela 'filmes_generos': {e}")
        return False
engine = sa.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


