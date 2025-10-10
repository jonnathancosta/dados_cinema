from src.ingestao.tmdb_coleta_filmes import executar_coleta as exc
from src.transformacao.normaliza_generos_id import executar_ingestao_generos_ids as excg
from src.transformacao.normaliza_filmes import executar_ingestao_filmes as excf

        
if __name__ == "__main__":
    
    excg()
    excf()
