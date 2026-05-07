"""
Camada de acesso a dados — consultas ao AWS Athena com filtros dinâmicos.
Cod_insumo e insumo_informado são buscados via LEFT JOIN em tbl_cadastrados_bp.
"""

import os
import unicodedata
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pyathena import connect

load_dotenv(Path(__file__).parent / ".env")

TABLE_COLETA        = "db_scraping_spdo.tbl_ecommerce_spdo"
TABLE_CADASTRO      = "db_scraping_spdo.tbl_cadastrados_bp"
TABLE_MONITORAMENTO = "db_scraping_spdo.tbl_monitoramento"

# Janela utilizada para alimentar opções de filtro (evita full-scan histórico).
OPTIONS_LOOKBACK_DAYS = 365
# Reuso de resultado de query do Athena — corta custo/latência em consultas
# repetitivas. O workgroup precisa permitir reuso.
RESULT_REUSE_MINUTES = 60

VISIBLE_COLUMNS = [
    "data_coleta", "ean", "sku", "cod_insumo", "insumo_informado",
    "descricao", "marca", "uf", "moeda", "preco", "preco_promocional",
    "tipo_preco", "periodicidade", "cod_informante", "nome_informante",
    "url", "plataforma", "id_produto", "id_coleta", "id_imagem",
]

_BASE_SELECT = f"""
    SELECT
        cp.data_coleta,
        cp.ean,
        cp.sku,
        cb.cod_insumo,
        cb.insumo_informado,
        cp.descricao,
        cp.marca,
        cp.uf,
        cp.moeda,
        cp.preco,
        cp.preco_promocional,
        cp.tipo_preco,
        cp.periodicidade,
        element_at(cp.cod_informante, 1) AS cod_informante,
        cp.nome_informante,
        cp.url,
        cp.plataforma,
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


def _strip_accents(s: str) -> str:
    nfd = unicodedata.normalize("NFD", s.lower())
    return "".join(c for c in nfd if not unicodedata.combining(c))


def _run_query(sql: str, reuse: bool = True) -> pd.DataFrame:
    with get_conn() as conn:
        cursor = conn.cursor()
        try:
            if reuse:
                cursor.execute(
                    sql,
                    result_reuse_enable=True,
                    result_reuse_minutes=RESULT_REUSE_MINUTES,
                )
            else:
                cursor.execute(sql)
        except TypeError:
            cursor.execute(sql)
        cols = [d[0] for d in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=cols)


# ---------------------------------------------------------------------------
# Opções para widgets de filtro
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
    options: dict = {}
    cutoff = (date.today() - timedelta(days=OPTIONS_LOOKBACK_DAYS)).isoformat()

    # Marca, tipo_preco e UF em uma única query (unpivot via UNION ALL).
    # Dispara um único job Athena em vez de três.
    df_distinct = _run_query(f"""
        SELECT 'marca' AS col, marca AS value FROM {TABLE_COLETA}
            WHERE TRY_CAST(data_coleta AS DATE) >= DATE '{cutoff}' AND marca IS NOT NULL
        UNION
        SELECT 'tipo_preco' AS col, tipo_preco AS value FROM {TABLE_COLETA}
            WHERE TRY_CAST(data_coleta AS DATE) >= DATE '{cutoff}' AND tipo_preco IS NOT NULL
        UNION
        SELECT 'uf' AS col, uf AS value FROM {TABLE_COLETA}
            WHERE TRY_CAST(data_coleta AS DATE) >= DATE '{cutoff}' AND uf IS NOT NULL
    """)
    for col in ("marca", "tipo_preco", "uf"):
        options[col] = sorted(df_distinct.loc[df_distinct["col"] == col, "value"].dropna().tolist())

    # Informantes: cod, nome, última coleta — restrito à janela.
    df_inf = _run_query(f"""
        SELECT element_at(cod_informante, 1) AS cod_informante,
               MAX(nome_informante) AS nome_informante,
               MAX(data_coleta) AS ultima_coleta
        FROM {TABLE_COLETA}
        WHERE cod_informante IS NOT NULL
          AND TRY_CAST(data_coleta AS DATE) >= DATE '{cutoff}'
        GROUP BY element_at(cod_informante, 1)
        ORDER BY 1
    """)
    options["cod_informante"] = df_inf["cod_informante"].tolist()
    options["nome_informante"] = sorted(df_inf["nome_informante"].dropna().tolist())
    options["ultima_coleta_by_informante"] = dict(
        zip(df_inf["cod_informante"], df_inf["ultima_coleta"].astype(str))
    )
    nome_mask = df_inf["nome_informante"].notna()
    options["cod_to_nome"] = dict(zip(
        df_inf.loc[nome_mask, "cod_informante"],
        df_inf.loc[nome_mask, "nome_informante"],
    ))
    options["nome_to_cod"] = {v: k for k, v in options["cod_to_nome"].items()}

    # Janela exposta no widget de data (sem MIN/MAX scan).
    options["data_min"] = cutoff
    options["data_max"] = date.today().isoformat()

    # Opções de insumo — globais e por informante.
    try:
        df_bp = _run_query(f"""
            SELECT DISTINCT CAST(cod_informante AS VARCHAR) AS cod_informante,
                   cod_insumo, insumo_informado
            FROM {TABLE_CADASTRO}
            WHERE cod_insumo IS NOT NULL OR insumo_informado IS NOT NULL
        """)
        options["cod_insumo"] = sorted(df_bp["cod_insumo"].dropna().unique().tolist())
        options["insumo_informado"] = sorted(df_bp["insumo_informado"].dropna().unique().tolist())

        # Agrupamento vetorizado em vez de iterrows.
        cod_grp = (
            df_bp.dropna(subset=["cod_informante", "cod_insumo"])
                 .groupby("cod_informante")["cod_insumo"]
                 .apply(lambda s: sorted(s.unique()))
                 .to_dict()
        )
        ins_grp = (
            df_bp.dropna(subset=["cod_informante", "insumo_informado"])
                 .groupby("cod_informante")["insumo_informado"]
                 .apply(lambda s: sorted(s.unique()))
                 .to_dict()
        )
        options["cod_insumo_by_informante"] = cod_grp
        options["insumo_informado_by_informante"] = ins_grp
    except Exception:
        options["cod_insumo"] = []
        options["insumo_informado"] = []
        options["cod_insumo_by_informante"] = {}
        options["insumo_informado_by_informante"] = {}

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
        conditions.append(
            f"TRY_CAST(cp.data_coleta AS DATE) = DATE '{str(filters['data_exata'])}'"
        )
    else:
        cutoff = (date.today() - timedelta(days=30)).isoformat()
        today  = date.today().isoformat()
        conditions.append(
            f"TRY_CAST(cp.data_coleta AS DATE) BETWEEN DATE '{cutoff}' AND DATE '{today}'"
        )
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
        tokens = [t for t in term.split() if t]
        if tokens:
            # Normaliza a coluna no Athena: lower + decompõe acentos (NFD) +
            # remove marcas combinantes (\p{M}). Permite buscar "sabao" e
            # casar com "sabão", em qualquer combinação de maiúsculas/acentos.
            norm_col = r"regexp_replace(normalize(LOWER(cp.descricao), NFD), '\p{M}', '')"
            # AND entre tokens => palavras podem aparecer em qualquer ordem
            # e com qualquer texto entre elas.
            like_parts = [
                f"{norm_col} LIKE {_escape('%' + _strip_accents(tok) + '%')}"
                for tok in tokens
            ]
            conditions.append("(" + " AND ".join(like_parts) + ")")

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


def get_monitoramento_data() -> pd.DataFrame:
    """Retorna tbl_monitoramento enriquecida com status, tipo_preco e ultima_coleta do tbl_ecommerce."""
    cutoff = (date.today() - timedelta(days=OPTIONS_LOOKBACK_DAYS)).isoformat()
    sql = f"""
        SELECT
            CAST(m.cod_informante AS VARCHAR) AS cod_informante,
            m.dominio,
            m.frete,
            CASE WHEN e.cod_informante IS NOT NULL THEN 'Ativo' ELSE 'Inativo' END AS status,
            e.tipo_preco,
            e.ultima_coleta
        FROM {TABLE_MONITORAMENTO} m
        LEFT JOIN (
            SELECT
                element_at(cod_informante, 1)       AS cod_informante,
                MAX(tipo_preco)                      AS tipo_preco,
                CAST(MAX(data_coleta) AS VARCHAR)    AS ultima_coleta
            FROM {TABLE_COLETA}
            WHERE TRY_CAST(data_coleta AS DATE) >= DATE '{cutoff}'
            GROUP BY element_at(cod_informante, 1)
        ) e ON CAST(m.cod_informante AS VARCHAR) = e.cod_informante
        ORDER BY CAST(m.cod_informante AS VARCHAR)
    """
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame(columns=["cod_informante", "dominio", "frete", "status", "tipo_preco", "ultima_coleta"])


def get_carga_data() -> pd.DataFrame:
    """Retorna todos os insumos cadastrados BP com cod_insumo e insumo_informado preenchidos."""
    sql = f"""
        SELECT
            CAST(cod_informante AS VARCHAR) AS cod_informante,
            cod_insumo,
            insumo_informado,
            id_produto_site
        FROM {TABLE_CADASTRO}
        WHERE cod_insumo IS NOT NULL
          AND insumo_informado IS NOT NULL
        ORDER BY CAST(cod_informante AS VARCHAR), cod_insumo
    """
    try:
        return _run_query(sql)
    except Exception:
        return pd.DataFrame(columns=["cod_informante", "cod_insumo", "insumo_informado", "id_produto_site"])
