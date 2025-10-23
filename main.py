from sql.schema import engine, executar_querys as excq
from src.ingestao.tmdb_coleta_filmes import executar_coleta as exci
from src.transformacao.normaliza_generos_id import executar_ingestao_generos_ids as excg
from src.transformacao.normaliza_filmes_generos import executar_ingestao_filmes as excf
from sql.analises_exploratorias import engine, executar_todas_querys as excq1
from sql.views_gold import engine, exportar_views_para_gold as evg
        
if __name__ == "__main__":
    excq(engine)
    exci()
    excg()
    excf()
    excq1(engine)
    evg(engine)