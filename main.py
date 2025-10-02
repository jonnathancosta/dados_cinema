import os
from dotenv import load_dotenv
import utils.tmdb_coleta_filmes as tc

def main():
    
    load_dotenv()  # Carrega variáveis de ambiente do arquivo .env
    TOKEN = os.getenv("TMDB_API_TOKEN")
    coletor = tc.TMDBCollector(TOKEN)
    
    # Configuração da coleta para cada tipo de ordenação
    coletas = [
        {
            "sort_by": tc.SortBy.POPULARITY_DESC,
            "arquivo": "filmes_por_popularidade_desc.jsonl"
        },
        {
            "sort_by": tc.SortBy.POPULARITY_ASC,
            "arquivo": "filmes_por_popularidade_asc.jsonl"
        },
        {
            "sort_by": tc.SortBy.VOTE_AVERAGE_DESC,
            "arquivo": "filmes_por_avaliacao_desc.jsonl"
        },
        {
            "sort_by": tc.SortBy.VOTE_AVERAGE_ASC,
            "arquivo": "filmes_por_avaliacao_asc.jsonl"
        },
        {
            "sort_by": tc.SortBy.PRIMARY_RELEASE_DATE_DESC,
            "arquivo": "filmes_por_data_desc.jsonl"
        },
        {
            "sort_by": tc.SortBy.PRIMARY_RELEASE_DATE_ASC,
            "arquivo": "filmes_por_data_asc.jsonl"
        },
        {
            "sort_by": tc.SortBy.REVENUE_DESC,
            "arquivo": "filmes_por_receita_desc.jsonl"
        },
        {
            "sort_by": tc.SortBy.REVENUE_ASC,
            "arquivo": "filmes_por_receita_asc.jsonl"
        },
        {
            "sort_by": tc.SortBy.VOTE_COUNT_DESC,
            "arquivo": "filmes_por_num_votos_desc.jsonl"
        },
        {
            "sort_by": tc.SortBy.VOTE_COUNT_ASC,
            "arquivo": "filmes_por_num_votos_asc.jsonl"
        }
    ]
    
    # Executa a coleta para cada configuração
    for coleta in coletas:
        print(f"\n{'='*50}")
        print(f"Coletando filmes ordenados por: {coleta['sort_by'].value}")
        print(f"{'='*50}\n")
        
        filmes = coletor.coletar_filmes(
            sort_by=coleta['sort_by'],
            pagina_inicial=1,
            quantidade_paginas=500
        )
        
        if filmes:
            # Salva os resultados
            tc.salvar_filmes(
                filmes,
                nome_arquivo=coleta['arquivo'],
                formatado=True  # Usar True para formato mais legível
            )
            

    # Coleta e salva os gêneros
    generos = coletor.coletar_ids_generos()
    if generos:
        tc.salvar_generos(generos)
        
if __name__ == "__main__":
    main()