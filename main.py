from sql.criando_tabelas import engine, testar_conexao, criar_tabela_filmes, criar_tabela_generos, criar_tabela_filmes_generos, estruturar_tabela_filmes_generos 
from src.ingestao.tmdb_coleta_filmes import executar_coleta as exc
from src.transformacao.normaliza_generos_id import executar_ingestao_generos_ids as excg
from src.transformacao.normaliza_filmes_generos import executar_ingestao_filmes as excf

        
if __name__ == "__main__":
    testar_conexao(engine)
    criar_tabela_filmes(engine)
    criar_tabela_generos(engine)
    criar_tabela_filmes_generos(engine)
    estruturar_tabela_filmes_generos(engine)
    excg()
    excf()


