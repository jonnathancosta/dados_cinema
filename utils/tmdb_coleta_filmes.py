import requests
import json
import time
from typing import List, Dict, Optional
from enum import Enum
from pathlib import Path

class SortBy(Enum):
    """
    Enum com todas as opções de ordenação disponíveis na API do TMDB
    Documentação: https://developer.themoviedb.org/reference/discover-movie
    """
    POPULARITY_DESC = "popularity.desc"
    POPULARITY_ASC = "popularity.asc"
    REVENUE_DESC = "revenue.desc"
    REVENUE_ASC = "revenue.asc"
    PRIMARY_RELEASE_DATE_DESC = "primary_release_date.desc"
    PRIMARY_RELEASE_DATE_ASC = "primary_release_date.asc"
    VOTE_AVERAGE_DESC = "vote_average.desc"
    VOTE_AVERAGE_ASC = "vote_average.asc"
    VOTE_COUNT_DESC = "vote_count.desc"
    VOTE_COUNT_ASC = "vote_count.asc"

class TMDBCollector:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}"
        }
        self.delay = 0.25  # 250ms entre requisições para evitar rate limiting
        
    def _make_request(self, url: str, params: dict) -> Optional[dict]:
        """Faz uma requisição para a API com rate limiting e tratamento de erros"""
        try:
            time.sleep(self.delay)  # Rate limiting
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            if response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get('Retry-After', 5))
                print(f"Rate limit atingido. Aguardando {retry_after} segundos...")
                time.sleep(retry_after)
                return self._make_request(url, params)  # Tenta novamente
            return None
        except json.JSONDecodeError as e:
            print(f"Erro ao processar JSON: {e}")
            return None

    def coletar_filmes(self, 
                      sort_by: SortBy,
                      pagina_inicial: int = 1,
                      quantidade_paginas: int = 500) -> List[Dict]:
        """
        Coleta filmes da API do TMDB com a ordenação especificada
        
        Args:
            sort_by: Critério de ordenação (use a enum SortBy)
            pagina_inicial: Página inicial para coleta
            quantidade_paginas: Quantidade de páginas a coletar
        """
        url = f"{self.base_url}/discover/movie"
        params = {
            "language": "pt-BR",
            "page": "1",
            "sort_by": sort_by.value
        }

        todos_filmes = []

        try:
            for p in range(pagina_inicial, pagina_inicial + quantidade_paginas):
                params["page"] = str(p)
                dados = self._make_request(url, params)
                
                if not dados:
                    print(f"Erro ao coletar página {p}")
                    break

                filmes_pagina = dados.get("results", [])
                if not filmes_pagina:
                    print("Nenhum filme encontrado na página. Finalizando...")
                    break

                todos_filmes.extend(filmes_pagina)

                print(f"Página {p} de {dados.get('total_pages', 0)} "
                      f"(Total filmes: {len(todos_filmes)})")

            return todos_filmes

        except KeyboardInterrupt:
            print("\nInterrompido pelo usuário.")
            return todos_filmes
        
    def coletar_ids_generos(self) -> Dict[int, str]:
        """
        Coleta a lista de gêneros disponíveis na API do TMDB
        Retorna um dicionário com ID do gênero como chave e nome como valor
        """
        url = f"{self.base_url}/genre/movie/list"
        params = {
            "language": "pt-BR"
        }
        
        dados = self._make_request(url, params)
        if not dados:
            print("Erro ao coletar gêneros")
            return {}
        
        generos = dados.get("genres", [])
        generos_formatados = [{"id": str(genero["id"]), "genero": genero["name"]} for genero in generos]
        
        print(f"✅ {len(generos_formatados)} gêneros coletados")
        return generos_formatados

def salvar_filmes(filmes: List[Dict], nome_arquivo: str, formatado: bool = False) -> bool:
    """
    Salva os filmes em um arquivo JSONL
    
    Args:
        filmes: Lista de filmes para salvar
        nome_arquivo: Nome do arquivo de saída
        formatado: Se True, salva em formato mais legível
    """
    if not filmes:
        print("Nenhum filme para salvar!")
        return False

    try:
        with open(nome_arquivo, "a", encoding="utf-8") as f:
            for filme in filmes:
                if formatado:
                    # Formato mais legível
                    json_str = json.dumps(filme, ensure_ascii=False, indent=2)
                    # Remove quebras de linha mas mantém indentação
                    json_str = ' '.join(json_str.splitlines())
                    f.write(json_str + "\n")
                else:
                    # Formato compacto
                    f.write(json.dumps(filme, ensure_ascii=False) + "\n")
        
        print(f"✅ {len(filmes)} filmes salvos em {nome_arquivo}")
        return True

    except Exception as e:
        print(f"Erro ao salvar arquivo: {e}")
        return False

def salvar_generos(generos: List[Dict[str, str]], nome_arquivo: str = "generos.json") -> bool:
    """
    Salva os gêneros em um arquivo JSON
    
    Args:
        generos: Lista de dicionários contendo id e genero
        nome_arquivo: Nome do arquivo de saída
    """
    if not generos:
        print("Nenhum gênero para salvar!")
        return False

    try:
        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(generos, f, ensure_ascii=False, indent=2)
        
        print(f"✅ {len(generos)} gêneros salvos em {nome_arquivo}")
        return True

    except Exception as e:
        print(f"Erro ao salvar arquivo: {e}")
        return False