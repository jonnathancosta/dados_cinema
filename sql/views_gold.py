from dotenv import load_dotenv
import pandas as pd
import os
import sqlalchemy as sa

load_dotenv()  # Carrega variáveis de ambiente do .env
host = os.getenv("MYSQL_HOST")
port = os.getenv("MYSQL_PORT")
user = os.getenv("MYSQL_USER")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DATABASE")

engine = sa.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")

def exportar_views_para_gold(engine: sa.engine.base.Engine):
    """
    Exporta todas as views do banco para a camada Gold.
    Cada view é lida em um DataFrame e salva como arquivo CSV com o nome da view.
    """
    pasta_gold = "data/gold"
    
    try:
        # Garante que a pasta gold exista
        os.makedirs(pasta_gold, exist_ok=True)

        with engine.connect() as connection:
            # 1️⃣ Buscar todas as views do schema atual
            query_views = """
                SELECT TABLE_NAME
                FROM information_schema.views
                WHERE TABLE_SCHEMA = DATABASE();
            """
            views = pd.read_sql(query_views, connection)
            
            if views.empty:
                print("⚠️ Nenhuma view encontrada no banco.")
                return False
            
            print(f"🔍 {len(views)} views encontradas: {', '.join(views['TABLE_NAME'])}")
            
            # 2️⃣ Iterar sobre cada view e exportar
            for view_name in views["TABLE_NAME"]:
                print(f"\n➡️ Exportando view: {view_name}")
                try:
                    df = pd.read_sql(f"SELECT * FROM {view_name};", connection)
                    
                    # Caminho completo
                    path = os.path.join(pasta_gold, f"{view_name}.csv")
                    
                    # Salva em formato parquet
                    df.to_csv(path, index=False)
                    print(f"✅ View '{view_name}' salva em: {path}")
                except Exception as e:
                    print(f"❌ Erro ao exportar '{view_name}': {e}")
                    
        print("\n🏁 Exportação concluída com sucesso!")
        return True

    except Exception as e:
        print(f"Erro ao exportar views: {e}")
        return False
