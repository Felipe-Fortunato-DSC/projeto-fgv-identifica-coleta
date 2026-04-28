"""
Camada de acesso a dados — consultas ao AWS Athena com filtros dinâmicos.
Cod_insumo e insumo_informado são buscados via LEFT JOIN em tbl_cadastrados_bp.
"""

import os
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyathena import connect

load_dotenv(Path(__file__).parent / ".env")

TABLE_COLETA   = "db_scraping_spdo.tbl_ecommerce_spdo"
TABLE_CADASTRO = "db_scraping_spdo.tbl_cadastrados_bp"

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
        s3_staging_dir=os.environ["ATHENA_S3_STAGING_DIR"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
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
    for col in ("marca", "tipo_preco", "uf"):
        df = _run_query(
            f"SELECT DISTINCT {col} FROM {TABLE_COLETA} "
            f"WHERE {col} IS NOT NULL ORDER BY {col}"
        )
        options[col] = df[col].tolist()

    # Informantes: cod, nome, última coleta — tudo em uma query
    df_inf = _run_query(
        f"SELECT element_at(cod_informante, 1) AS cod_informante, "
        f"MAX(nome_informante) AS nome_informante, "
        f"MAX(data_coleta) AS ultima_coleta "
        f"FROM {TABLE_COLETA} WHERE cod_informante IS NOT NULL "
        f"GROUP BY element_at(cod_informante, 1) "
        f"ORDER BY cod_informante"
    )
    options["cod_informante"] = df_inf["cod_informante"].tolist()
    options["nome_informante"] = sorted(df_inf["nome_informante"].dropna().tolist())
    options["ultima_coleta_by_informante"] = {
        row["cod_informante"]: str(row["ultima_coleta"])
        for _, row in df_inf.iterrows()
    }
    options["cod_to_nome"] = {
        row["cod_informante"]: row["nome_informante"]
        for _, row in df_inf.iterrows()
        if pd.notna(row["nome_informante"])
    }
    options["nome_to_cod"] = {v: k for k, v in options["cod_to_nome"].items()}

    row = _run_query(
        f"SELECT MIN(data_coleta) AS mn, MAX(data_coleta) AS mx FROM {TABLE_COLETA}"
    ).iloc[0]
    options["data_min"] = str(row["mn"])
    options["data_max"] = str(row["mx"])

    # Opções de insumo (tabela pode estar vazia)
    for col in ("cod_insumo", "insumo_informado"):
        try:
            df_bp = _run_query(
                f"SELECT DISTINCT {col} FROM {TABLE_CADASTRO} "
                f"WHERE {col} IS NOT NULL ORDER BY {col}"
            )
            options[col] = df_bp[col].tolist()
        except Exception:
            options[col] = []

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
    if filters.get("data_exata"):
        conditions.append(f"cp.data_coleta = {_escape(str(filters['data_exata']))}")
    if filters.get("cod_insumo"):
        conditions.append(f"cb.cod_insumo = {_escape(filters['cod_insumo'])}")
    if filters.get("insumo_informado"):
        conditions.append(f"cb.insumo_informado = {_escape(filters['insumo_informado'])}")
    if filters.get("cadastrados_bp"):
        conditions.append(
            f"EXISTS ("
            f"SELECT 1 FROM {TABLE_CADASTRO} cb2 "
            f"WHERE CAST(cb2.cod_informante AS VARCHAR) = element_at(cp.cod_informante, 1) "
            f"AND cb2.id_produto_site = cp.id_produto "
            f"AND cb2.cod_insumo IS NOT NULL"
            f")"
        )
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

def get_page_data_with_count(
    filters: dict, page: int, page_size: int
) -> tuple[pd.DataFrame, int]:
    base_sql = _build_query(filters)
    offset = (page - 1) * page_size
    sql = f"""
        SELECT * FROM (
            SELECT t.*,
                   ROW_NUMBER() OVER (ORDER BY t.data_coleta DESC) AS _rn,
                   COUNT(*) OVER ()                                 AS _total
            FROM ({base_sql}) t
        ) WHERE _rn > {offset} AND _rn <= {offset + page_size}
    """
    df = _run_query(sql)
    total = int(df["_total"].iloc[0]) if not df.empty else 0
    df = df.drop(columns=["_rn", "_total"], errors="ignore")
    df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df, total


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


def save_cadastrado_bp(
    cod_informante: str,
    id_produto_site: str,
    cod_insumo: str,
    insumo_informado: str,
) -> None:
    sql = f"""
        INSERT INTO {TABLE_CADASTRO} (cod_informante, cod_insumo, insumo_informado, id_produto_site)
        VALUES ({_escape(cod_informante)}, {_escape(cod_insumo)}, {_escape(insumo_informado)}, {_escape(id_produto_site)})
    """
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
