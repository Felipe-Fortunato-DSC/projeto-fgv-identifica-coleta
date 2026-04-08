"""
Camada de acesso a dados — consultas ao AWS Athena com filtros dinâmicos.
Cod_insumo e insumo_informado são buscados via LEFT JOIN em tb_teste_bp.
"""

import os
import re
from contextlib import contextmanager

import pandas as pd
from pyathena import connect

ATHENA_S3_STAGING_DIR = os.environ["ATHENA_S3_STAGING_DIR"]
ATHENA_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

TABLE_COLETA   = "db_scraping_spdo.tbl_ecommerce_spdo"
TABLE_CADASTRO = "db_scraping_spdo.tb_teste_bp"

# Colunas visíveis ao usuário (ordem de exibição)
VISIBLE_COLUMNS = [
    "data_coleta",
    "plataforma",
    "cod_informante",
    "nome_informante",
    "periodicidade",
    "tipo_preco",
    "cod_insumo",        # vem de tb_teste_bp
    "ean",
    "sku",
    "insumo_informado",  # vem de tb_teste_bp
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
_BASE_SELECT = f"""
    SELECT
        cp.data_coleta,
        cp.plataforma,
        element_at(cp.cod_informante, 1) AS cod_informante,
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
    FROM {TABLE_COLETA} cp
    LEFT JOIN {TABLE_CADASTRO} cb
        ON CAST(cb.cod_informante AS VARCHAR) = element_at(cp.cod_informante, 1)
        AND cb.id_produto_site                = cp.id_produto
"""


@contextmanager
def get_conn():
    conn = connect(
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
        region_name=ATHENA_REGION,
    )
    try:
        yield conn
    finally:
        conn.close()


def _run_query(sql: str, params: list | None = None) -> pd.DataFrame:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=cols)


# ---------------------------------------------------------------------------
# Opções para widgets de filtro
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
    """Retorna listas de valores únicos para os filtros de seleção."""
    options: dict = {}
    for col in ("nome_informante", "marca", "tipo_preco", "uf"):
        df = _run_query(
            f"SELECT DISTINCT {col} FROM {TABLE_COLETA} "
            f"WHERE {col} IS NOT NULL ORDER BY {col}"
        )
        options[col] = df[col].tolist()

    df = _run_query(
        f"SELECT DISTINCT element_at(cod_informante, 1) AS cod_informante "
        f"FROM {TABLE_COLETA} WHERE cod_informante IS NOT NULL "
        f"ORDER BY cod_informante"
    )
    options["cod_informante"] = df["cod_informante"].tolist()

    row = _run_query(
        f"SELECT MIN(data_coleta) AS mn, MAX(data_coleta) AS mx FROM {TABLE_COLETA}"
    ).iloc[0]
    options["data_min"] = str(row["mn"])
    options["data_max"] = str(row["mx"])

    return options


# ---------------------------------------------------------------------------
# Construção de query com filtros
# ---------------------------------------------------------------------------

def _build_query(filters: dict) -> tuple[str, list]:
    """
    Monta a query SQL (com JOIN) para os filtros informados.
    Retorna (sql, params). Usa %s como placeholder (pyformat do PyAthena).
    """
    conditions: list[str] = []
    params: list = []

    if filters.get("cod_informante"):
        conditions.append("element_at(cp.cod_informante, 1) = %s")
        params.append(filters["cod_informante"])

    if filters.get("nome_informante"):
        conditions.append("cp.nome_informante = %s")
        params.append(filters["nome_informante"])

    if filters.get("marca"):
        conditions.append("cp.marca = %s")
        params.append(filters["marca"])

    if filters.get("tipo_preco"):
        conditions.append("cp.tipo_preco = %s")
        params.append(filters["tipo_preco"])

    if filters.get("uf"):
        conditions.append("cp.uf = %s")
        params.append(filters["uf"])

    if filters.get("data_apos"):
        conditions.append("cp.data_coleta >= %s")
        params.append(str(filters["data_apos"]))

    if filters.get("ean_sku"):
        term = filters["ean_sku"].strip()
        conditions.append("(cp.ean = %s OR cp.sku = %s)")
        params.extend([term, term])

    if filters.get("busca_texto"):
        term = filters["busca_texto"].strip()
        conditions.append("LOWER(cp.descricao) LIKE LOWER(%s)")
        params.append(f"%{term}%")

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return _BASE_SELECT + where, params


# ---------------------------------------------------------------------------
# Consultas públicas
# ---------------------------------------------------------------------------

def get_total_count(filters: dict) -> int:
    sql, params = _build_query(filters)
    df = _run_query(f"SELECT COUNT(*) AS cnt FROM ({sql})", params)
    return int(df["cnt"].iloc[0])


def get_page_data(filters: dict, page: int, page_size: int = 10) -> pd.DataFrame:
    sql, params = _build_query(filters)
    offset = (page - 1) * page_size
    sql = (
        f"SELECT * FROM ("
        f"  SELECT t.*, ROW_NUMBER() OVER (ORDER BY t.data_coleta DESC) AS _rn"
        f"  FROM ({sql}) t"
        f") WHERE _rn > {offset} AND _rn <= {offset + page_size}"
    )

    df = _run_query(sql, params)
    df = df.drop(columns=["_rn"], errors="ignore")
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df


def get_all_filtered_ids(filters: dict) -> list[str]:
    """Retorna todos os id_produto correspondentes aos filtros (sem paginação)."""
    sql, params = _build_query(filters)
    df = _run_query(f"SELECT id_produto FROM ({sql}) t", params)
    return df["id_produto"].tolist()


def get_data_by_ids(ids: list[str]) -> pd.DataFrame:
    """Retorna dados completos das linhas selecionadas para exportação."""
    if not ids:
        return pd.DataFrame(columns=VISIBLE_COLUMNS)

    # IDs vêm do próprio banco — formatação direta é segura
    escaped = ", ".join(f"'{id_}'" for id_ in ids)
    sql = (
        _BASE_SELECT
        + f" WHERE cp.id_produto IN ({escaped})"
        + " ORDER BY cp.data_coleta DESC"
    )
    df = _run_query(sql)
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df
