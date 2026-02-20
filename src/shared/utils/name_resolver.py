"""
Resolver de nomes de colaboradores SAC.

Define o nome canônico de cada colaborador e todas as variações conhecidas
(vindas de sistemas diferentes: Omnichannel, Ligações, Voalle).

Como funciona:
  - COLABORADORES_SAC é o dicionário de verdade: nome canônico → lista de variações
  - _RESOLVER é construído automaticamente no carregamento do módulo,
    mapeando cada variação normalizada → nome canônico normalizado
  - A função resolver_nome() é o ponto de entrada: recebe qualquer variação
    e retorna o nome canônico, ou o próprio nome normalizado se não houver mapeamento

Como adicionar um novo colaborador ou nova variação:
  1. Se o colaborador já existe, adicione a variação na lista dele
  2. Se é um novo colaborador, adicione uma nova entrada no dicionário
  3. O nome canônico (chave) é o que será exibido no frontend
"""

import unicodedata
import re
from typing import Optional


# ================================================================
# DICIONÁRIO PRINCIPAL
# chave   = nome canônico (como será exibido no sistema)
# valores = todas as variações conhecidas vindas dos sistemas externos
# ================================================================

COLABORADORES_SAC: dict[str, list[str]] = {

    # --- Variações com nome completamente diferente entre sistemas ---
    "Placido Junior": [
        "Plácido Júnior",
        "PLÁCIDO JÚNIOR",
        "Placido Portal De Sousa Junior",
        "PLACIDO PORTAL DE SOUSA JUNIOR",
    ],

    # --- Variações com nome truncado no sistema de Ligações ---
    "Marcia Regina Ventura Rodrigues": [
        "MARCIA REGINA VENTURA RODRIGUE",   # truncado em 30 chars
        "Marcia Regina Ventura Rodrigue",
    ],
    "Plinio Vinicius Ryu Miyata Koiama": [
        "PLINIO VINICIUS RYU MIYATA KOI",   # truncado em 30 chars
        "Plinio Vinicius Ryu Miyata Koi",
    ],
    "Giselle Almeida Rodrigues Da Silva": [
        "GISELLE ALMEIDA RODRIGUES DA S",   # truncado em 30 chars
        "Giselle Almeida Rodrigues Da S",
    ],

    # --- Colaboradores sem variações conhecidas (lista vazia = só ramal é removido) ---
    "Ana Beatriz De Oliveira Franco": [],
    "Ana Carolina Ribeiro Miranda": [],
    "Carlos Cesar Soares Junior": [],
    "Daniele De Araujo Rodrigues": [],
    "Eliezer Abner Paggi Oliveira": [],
    "Gabriel Tavares Dos Santos": [],
    "Giovana Virginia Ferreira De Amorim": [],
    "Giovanna Alves Aranega": [],
    "Giselle Almeida Rodrigues Da Silva": [],
    "Giselma Caldeira Goncalves": [],
    "Gustavo Andrade Da Silva": [],
    "Gustavo Lanza": [],
    "Hagatta Thaynara De Freitas Martins": [],
    "Heros Henrique Delecrode": [],
    "Joao Pedro Cavani Meireles": [],
    "Joao Pedro Da Silva Pereira": [],
    "Julio Augusto Bueno Mariano": [],
    "Lauanda Lisboa Vaz": [],
    "Luana Vitoria Da Silva Lima": [],
    "Lucas Da Rocha Silva": [],
    "Luiz Bonfa Dos Santos": [],
    "Marcus Vinicius Mendes Jacomel": [],
    "Matheus Mazali Maeda": [],
    "Maycon Lins De Oliveira": [],
    "Pedro Emanuel Ferreira De Andrade": [],
    "Pedro Paulo Santos De Oliveira Mansur De Carvalho": [],
    "Pietro Pasqual Silva": [],
    "Queise Elen Santos Santana": [],
    "Rebecca De Assis Nezio": [],
    "Ricardo De Oliveira Geraldo": [],
    "Ricardo Luciano De Araujo": [],
    "Rodrigo Pereira Alves Timotio Junior": [],
    "Roselene Patricio De Souza": [],
    "Vitor Eduardo Bueno De Oliveira": [],
    "Vitor Nercessian": [],
    "Wellington Silva De Souza": [],
}


# ================================================================
# CONSTRUÇÃO AUTOMÁTICA DO RESOLVER (não editar)
# ================================================================

def _normalizar(nome: str) -> str:
    """Chave canônica: sem ramal, sem acento, maiúsculo, espaços colapsados."""
    nome = re.sub(r'\s*-?\s*\d{4,5}\s*$', '', nome).strip()
    nfkd = unicodedata.normalize("NFKD", nome)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.upper().split())


# Monta o dicionário reverso: variação normalizada → canônico normalizado
_RESOLVER: dict[str, str] = {}

for _canonico, _variacoes in COLABORADORES_SAC.items():
    _chave_canonica = _normalizar(_canonico)
    # O próprio canônico aponta para si mesmo
    _RESOLVER[_chave_canonica] = _chave_canonica
    # Cada variação aponta para o canônico
    for _v in _variacoes:
        _RESOLVER[_normalizar(_v)] = _chave_canonica


# ================================================================
# FUNÇÃO PÚBLICA
# ================================================================

def resolver_nome(nome: str) -> str:
    """
    Recebe qualquer variação de nome e retorna o nome canônico normalizado.

    - Se o nome (após normalização) estiver no mapeamento → retorna o canônico
    - Se não estiver → retorna o próprio nome normalizado
      (será criado como novo colaborador no banco)

    Exemplos:
      resolver_nome("PLÁCIDO JÚNIOR")              → "PLACIDO JUNIOR"
      resolver_nome("Placido Portal De Sousa Junior") → "PLACIDO JUNIOR"
      resolver_nome("Wellington Silva de Souza - 6373") → "WELLINGTON SILVA DE SOUZA"
    """
    chave = _normalizar(nome)
    return _RESOLVER.get(chave, chave)


def is_sac(nome: str) -> bool:
    """
    Retorna True se o nome (ou qualquer variação dele) pertence a um
    colaborador SAC cadastrado no resolver.
    """
    chave = _normalizar(nome)
    return chave in _RESOLVER