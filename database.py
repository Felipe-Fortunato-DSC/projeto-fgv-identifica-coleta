"""
Camada de acesso a dados — consultas ao SQLite com filtros dinâmicos
e busca textual via FTS5.
Cod_insumo e insumo_informado são buscados via LEFT JOIN em coleta_cadastrado_BP.
"""

import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent / "data" / "database.db"

# Colunas visíveis ao usuário (ordem de exibição)
VISIBLE_COLUMNS = [
    "data_coleta",
    "plataforma",
    "cod_informante",
    "nome_informante",
    "periodicidade",
    "tipo_preco",
    "cod_insumo",        # vem de coleta_cadastrado_BP
    "ean",
    "sku",
    "insumo_informado",  # vem de coleta_cadastrado_BP
    "url",
    "descricao",
    "marca",
    "uf",
    "moeda",
    "preco",
    "preco_promocional",
    "id_produto",
    "id_coleta",
    "id_imagem",
]

# SELECT base com JOIN — garante cod_insumo e insumo_informado da tabela de cadastro
_BASE_SELECT = """
    SELECT
        cp.data_coleta,
        cp.plataforma,
        cp.cod_informante,
        cp.nome_informante,
        cp.periodicidade,
        cp.tipo_preco,
        cb.cod_insumo,
        cp.ean,
        cp.sku,
        cb.insumo_informado,
        cp.url,
        cp.descricao,
        cp.marca,
        cp.uf,
        cp.moeda,
        cp.preco,
        cp.preco_promocional,
        cp.id_produto,
        cp.id_coleta,
        cp.id_imagem
    FROM coleta_produtos cp
    LEFT JOIN coleta_cadastrado_BP cb
        ON cb.cod_informante = cp.cod_informante
        AND cb.id_produto    = cp.id_produto
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Opções para widgets de filtro
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
    """Retorna listas de valores únicos para os filtros de seleção."""
    with get_conn() as conn:
        options: dict = {}
        for col in ("cod_informante", "nome_informante", "marca", "tipo_preco", "uf"):
            df = pd.read_sql_query(
                f"SELECT DISTINCT {col} FROM coleta_produtos "
                f"WHERE {col} IS NOT NULL ORDER BY {col}",
                conn,
            )
            options[col] = df[col].tolist()

        row = pd.read_sql_query(
            "SELECT MIN(data_coleta) AS mn, MAX(data_coleta) AS mx "
            "FROM coleta_produtos",
            conn,
        ).iloc[0]
        options["data_min"] = row["mn"]
        options["data_max"] = row["mx"]

    return options


# ---------------------------------------------------------------------------
# Construção de query com filtros
# ---------------------------------------------------------------------------

def _sanitize_fts(text: str) -> str:
    """Remove caracteres especiais do FTS5 e gera query de prefixo."""
    cleaned = re.sub(r"[^\w\sÀ-úÃ-ũ]", " ", text, flags=re.UNICODE)
    terms = [t for t in cleaned.split() if len(t) >= 2]
    if not terms:
        return ""
    return " ".join(f"{t}*" for t in terms)


def _build_query(filters: dict) -> tuple[str, list]:
    """
    Monta a query SQL (com JOIN) para os filtros informados.
    Retorna (sql, params).
    """
    conditions: list[str] = []
    params: list = []

    if filters.get("cod_informante"):
        conditions.append("cp.cod_informante = ?")
        params.append(filters["cod_informante"])

    if filters.get("nome_informante"):
        conditions.append("cp.nome_informante = ?")
        params.append(filters["nome_informante"])

    if filters.get("marca"):
        conditions.append("cp.marca = ?")
        params.append(filters["marca"])

    if filters.get("tipo_preco"):
        conditions.append("cp.tipo_preco = ?")
        params.append(filters["tipo_preco"])

    if filters.get("uf"):
        conditions.append("cp.uf = ?")
        params.append(filters["uf"])

    if filters.get("data_apos"):
        conditions.append("cp.data_coleta >= ?")
        params.append(str(filters["data_apos"]))

    if filters.get("ean_sku"):
        term = filters["ean_sku"].strip()
        conditions.append("(cp.ean = ? OR cp.sku = ?)")
        params.extend([term, term])

    if filters.get("busca_texto"):
        fts = _sanitize_fts(filters["busca_texto"])
        if fts:
            conditions.append(
                "cp.id IN (SELECT rowid FROM coleta_fts WHERE coleta_fts MATCH ?)"
            )
            params.append(fts)

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return _BASE_SELECT + where, params


# ---------------------------------------------------------------------------
# Consultas públicas
# ---------------------------------------------------------------------------

def get_total_count(filters: dict) -> int:
    sql, params = _build_query(filters)
    with get_conn() as conn:
        cur = conn.execute(f"SELECT COUNT(*) FROM ({sql})", params)
        return cur.fetchone()[0]


def get_page_data(filters: dict, page: int, page_size: int = 10) -> pd.DataFrame:
    sql, params = _build_query(filters)
    sql += " ORDER BY cp.data_coleta DESC LIMIT ? OFFSET ?"
    params += [page_size, (page - 1) * page_size]

    with get_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df


def get_all_filtered_ids(filters: dict) -> list[str]:
    """Retorna todos os id_produto correspondentes aos filtros (sem paginação)."""
    sql, params = _build_query(filters)
    id_sql = f"SELECT id_produto FROM ({sql})"
    with get_conn() as conn:
        cur = conn.execute(id_sql, params)
        return [row[0] for row in cur.fetchall()]


def get_data_by_ids(ids: list[str]) -> pd.DataFrame:
    """Retorna dados completos das linhas selecionadas para exportação."""
    if not ids:
        return pd.DataFrame(columns=VISIBLE_COLUMNS)

    placeholders = ",".join("?" * len(ids))
    sql = (
        _BASE_SELECT
        + f" WHERE cp.id_produto IN ({placeholders})"
        + " ORDER BY cp.data_coleta DESC"
    )
    with get_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=ids)

    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df
