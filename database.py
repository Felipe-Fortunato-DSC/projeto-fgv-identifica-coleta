"""
Camada de acesso a dados — consultas ao AWS Athena com filtros dinâmicos.
Cod_insumo e insumo_informado são buscados via LEFT JOIN em tb_teste_bp.
"""

import os
from contextlib import contextmanager

import pandas as pd
from pyathena import connect

ATHENA_S3_STAGING_DIR = os.environ["ATHENA_S3_STAGING_DIR"]
ATHENA_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

TABLE_COLETA   = "db_scraping_spdo.tbl_ecommerce_spdo"
TABLE_CADASTRO = "db_scraping_spdo.tb_teste_bp"

VISIBLE_COLUMNS = [
    "data_coleta", "plataforma", "cod_informante", "nome_informante",
    "periodicidade", "tipo_preco", "cod_insumo", "ean", "sku",
    "insumo_informado", "url", "descricao", "marca", "uf", "moeda",
    "preco", "preco_promocional", "id_produto", "id_coleta", "id_imagem",
]

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
        AND cb.id_produto_site = cp.id_produto
"""


@contextmanager
def get_conn():
    conn = connect(
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
        region_name=ATHENA_REGION,
        work_group="primary",
    )
    try:
        yield conn
    finally:
        conn.close()


def _escape(val: str) -> str:
    return "'" + str(val).replace("'", "''") + "'"


def _run_query(sql: str) -> pd.DataFrame:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=cols)


# ---------------------------------------------------------------------------
# Opções para widgets de filtro
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
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

def _build_where(filters: dict) -> str:
    conditions: list[str] = []

    if filters.get("cod_informante"):
        conditions.append(
            f"element_at(cp.cod_informante, 1) = {_escape(filters['cod_informante'])}"
        )
    if filters.get("nome_informante"):
        conditions.append(f"cp.nome_informante = {_escape(filters['nome_informante'])}")
    if filters.get("marca"):
        conditions.append(f"cp.marca = {_escape(filters['marca'])}")
    if filters.get("tipo_preco"):
        conditions.append(f"cp.tipo_preco = {_escape(filters['tipo_preco'])}")
    if filters.get("uf"):
        conditions.append(f"cp.uf = {_escape(filters['uf'])}")
    if filters.get("data_apos"):
        conditions.append(f"cp.data_coleta >= {_escape(str(filters['data_apos']))}")
    if filters.get("ean_sku"):
        term = filters["ean_sku"].strip()
        conditions.append(f"(cp.ean = {_escape(term)} OR cp.sku = {_escape(term)})")
    if filters.get("busca_texto"):
        term = filters["busca_texto"].strip()
        conditions.append(f"LOWER(cp.descricao) LIKE LOWER({_escape('%' + term + '%')})")

    return (" WHERE " + " AND ".join(conditions)) if conditions else ""


def _build_query(filters: dict) -> str:
    return _BASE_SELECT + _build_where(filters)


# ---------------------------------------------------------------------------
# Consultas públicas
# ---------------------------------------------------------------------------

def get_total_count(filters: dict) -> int:
    sql = _build_query(filters)
    df = _run_query(f"SELECT COUNT(*) AS cnt FROM ({sql}) t")
    return int(df["cnt"].iloc[0])


def get_page_data(filters: dict, page: int, page_size: int) -> pd.DataFrame:
    sql = _build_query(filters)
    offset = (page - 1) * page_size
    paginated = f"""
        SELECT * FROM (
            SELECT t.*, ROW_NUMBER() OVER (ORDER BY t.data_coleta DESC) AS _rn
            FROM ({sql}) t
        ) WHERE _rn > {offset} AND _rn <= {offset + page_size}
    """
    df = _run_query(paginated)
    df = df.drop(columns=["_rn"], errors="ignore")
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df


def get_all_filtered_ids(filters: dict) -> list[str]:
    sql = _build_query(filters)
    df = _run_query(f"SELECT cp.id_produto FROM ({sql}) t")
    return df["id_produto"].tolist()


def get_data_by_ids(ids: list[str]) -> pd.DataFrame:
    if not ids:
        return pd.DataFrame(columns=VISIBLE_COLUMNS)
    escaped = ", ".join(f"'{id_}'" for id_ in ids)
    sql = (
        _BASE_SELECT
        + f" WHERE cp.id_produto IN ({escaped})"
        + " ORDER BY cp.data_coleta DESC"
    )
    df = _run_query(sql)
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df
