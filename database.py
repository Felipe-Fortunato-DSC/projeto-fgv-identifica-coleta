import os
import unicodedata
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from pyathena import connect

load_dotenv(Path(__file__).parent / ".env")

TABLE_COLETA        = "db_scraping_spdo.tbl_ecommerce_collect_prod"
TABLE_CADASTRO      = "db_scraping_spdo.tbl_ecommerce_registered_ins_inform_prod"
TABLE_MONITORAMENTO = "db_scraping_spdo.tbl_ecommerce_informant_shipping_info_prod"

# Janela utilizada para alimentar opções de filtro (evita full-scan histórico).
OPTIONS_LOOKBACK_DAYS = 365
# Reuso de resultado de query do Athena — corta custo/latência em consultas
# repetitivas. O workgroup precisa permitir reuso.
RESULT_REUSE_MINUTES = 10

VISIBLE_COLUMNS = [
    "data_coleta", "ean", "sku", "cod_insumo", "insumo_informado",
    "descricao", "marca", "uf", "moeda", "preco", "preco_promocional",
    "tipo_preco", "periodicidade", "cod_informante", "nome_informante",
    "url", "plataforma", "id_produto", "id_coleta",
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
        cp.id_coleta
    FROM {TABLE_COLETA} cp
    LEFT JOIN (
        SELECT CAST(cod_informante AS VARCHAR) AS cod_informante,
               id_produto_site,
               MAX(cod_insumo)       AS cod_insumo,
               MAX(insumo_informado) AS insumo_informado
        FROM {TABLE_CADASTRO}
        GROUP BY CAST(cod_informante AS VARCHAR), id_produto_site
    ) cb
        ON cb.cod_informante = element_at(cp.cod_informante, 1)
        AND cb.id_produto_site = cp.id_produto
"""


def _sso_hint() -> str:
    profile = os.environ.get("AWS_PROFILE", "<seu-perfil>")
    return (
        "Falha ao obter credenciais AWS para o Athena.\n"
        "Para testar localmente com AWS SSO, rode no terminal:\n"
        f"    aws sso login --profile {profile}\n"
        "e confirme que AWS_PROFILE e AWS_DEFAULT_REGION estão definidos no .env."
    )


def _is_credential_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = (
        "sso", "token", "expired", "unable to locate credentials",
        "the security token", "credential", "forbidden", "accessdenied",
    )
    return any(n in msg for n in needles)


@contextmanager
def get_conn():
    """Abre conexão com o Athena usando a cadeia de credenciais padrão do boto3.

    - Local (SSO): defina AWS_PROFILE (perfil configurado via `aws configure sso`)
      no .env e rode `aws sso login --profile <perfil>` antes de subir a app.
    - Container/EC2/ECS: usa a role da task/instância (sem AWS_PROFILE no ambiente).
    """
    kwargs = {
        "s3_staging_dir": os.environ["ATHENA_S3_STAGING_DIR"],
        "region_name": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "work_group": os.environ.get("ATHENA_WORKGROUP", "primary"),
    }
    # Passa o perfil explicitamente quando definido — facilita o login SSO local.
    profile = os.environ.get("AWS_PROFILE")
    if profile:
        kwargs["profile_name"] = profile

    try:
        conn = connect(**kwargs)
    except (BotoCoreError, ClientError) as exc:
        if _is_credential_error(exc):
            raise RuntimeError(_sso_hint()) from exc
        raise
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
    try:
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
    except (BotoCoreError, ClientError) as exc:
        if _is_credential_error(exc):
            raise RuntimeError(_sso_hint()) from exc
        raise


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
    if filters.get("ean"):
        conditions.append(f"cp.ean = {_escape(filters['ean'])}")
    if filters.get("sku"):
        conditions.append(f"cp.sku = {_escape(filters['sku'])}")
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
    """Retorna tbl_ecommerce_informant_shipping_info_prod enriquecida com status, tipo_preco e ultima_coleta do tbl_ecommerce."""
    cutoff = (date.today() - timedelta(days=OPTIONS_LOOKBACK_DAYS)).isoformat()
    sql = f"""
        SELECT
            CAST(m.cod_informante AS VARCHAR)                 AS cod_informante,
            array_join(m.dominios, ', ')                      AS dominio,
            CASE WHEN m.possui_frete THEN 'Sim' ELSE 'Não' END AS frete,
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
    return _run_query(sql)


def get_informantes_coletaram(date_ini: date, date_fim: date) -> set[str]:
    """cod_informante distintos com pelo menos uma linha em
    tbl_ecommerce_collect_prod cuja data_coleta caia no intervalo [date_ini, date_fim]."""
    sql = f"""
        SELECT DISTINCT element_at(cod_informante, 1) AS cod_informante
        FROM {TABLE_COLETA}
        WHERE TRY_CAST(data_coleta AS DATE) BETWEEN DATE '{date_ini}' AND DATE '{date_fim}'
          AND cod_informante IS NOT NULL
    """
    df = _run_query(sql)
    return set(df["cod_informante"].dropna().astype(str))


def get_carga_data(filters: dict | None = None) -> pd.DataFrame:
    """Insumos cadastrados (cod_insumo+insumo_informado) que possuam coleta
    correspondente em tbl_ecommerce_collect_prod, únicos por id_produto_site.

    Filtros opcionais:
      - cod_informante: restringe cadastros a esse informante.
      - data_exata: exige que exista coleta nessa data.
    """
    filters = filters or {}
    cad_conds: list[str] = []
    coleta_conds: list[str] = []

    if filters.get("cod_informante"):
        cad_conds.append(f"CAST(cb.cod_informante AS VARCHAR) = {_escape(filters['cod_informante'])}")
        coleta_conds.append(
            f"element_at(cp.cod_informante, 1) = {_escape(filters['cod_informante'])}"
        )
    if filters.get("data_exata"):
        coleta_conds.append(
            f"TRY_CAST(cp.data_coleta AS DATE) = DATE '{filters['data_exata']}'"
        )

    extra_cad = ("\n              AND " + "\n              AND ".join(cad_conds)) if cad_conds else ""
    extra_coleta = ("\n                    AND " + "\n                    AND ".join(coleta_conds)) if coleta_conds else ""

    sql = f"""
        SELECT DISTINCT
            CAST(cb.cod_informante AS VARCHAR) AS cod_informante,
            cb.cod_insumo,
            cb.insumo_informado,
            cb.id_produto_site
        FROM {TABLE_CADASTRO} cb
        WHERE cb.cod_insumo IS NOT NULL
          AND cb.insumo_informado IS NOT NULL{extra_cad}
          AND EXISTS (
              SELECT 1 FROM {TABLE_COLETA} cp
              WHERE element_at(cp.cod_informante, 1) = CAST(cb.cod_informante AS VARCHAR)
                AND cp.id_produto = cb.id_produto_site{extra_coleta}
          )
        ORDER BY 1, 2
    """
    try:
        return _run_query(sql, reuse=True)
    except Exception:
        return pd.DataFrame(columns=["cod_informante", "cod_insumo", "insumo_informado", "id_produto_site"])


def get_prices_for_keys(keys: list[tuple[str, str]]) -> pd.DataFrame:
    """Para cada (cod_informante, id_produto), devolve a coleta mais recente."""
    if not keys:
        return pd.DataFrame(columns=VISIBLE_COLUMNS)
    cods = sorted({k[0] for k in keys})
    prods = sorted({k[1] for k in keys})
    cods_in  = ", ".join(_escape(c) for c in cods)
    prods_in = ", ".join(_escape(p) for p in prods)
    sql = f"""
        SELECT * FROM (
            SELECT
                cp.data_coleta, cp.ean, cp.sku, cp.descricao, cp.marca,
                cp.uf, cp.moeda, cp.preco, cp.preco_promocional,
                cp.tipo_preco, cp.periodicidade,
                element_at(cp.cod_informante, 1) AS cod_informante,
                cp.nome_informante, cp.url, cp.plataforma,
                cp.id_produto, cp.id_coleta,
                ROW_NUMBER() OVER (
                    PARTITION BY element_at(cp.cod_informante, 1), cp.id_produto
                    ORDER BY cp.data_coleta DESC
                ) AS rn
            FROM {TABLE_COLETA} cp
            WHERE element_at(cp.cod_informante, 1) IN ({cods_in})
              AND cp.id_produto IN ({prods_in})
        )
        WHERE rn = 1
    """
    try:
        df = _run_query(sql, reuse=True).drop(columns=["rn"], errors="ignore")
    except Exception:
        return pd.DataFrame(columns=VISIBLE_COLUMNS)

    # Pós-filtro: o IN cruzado pode incluir pares não desejados.
    keys_set = {(c, p) for c, p in keys}
    if not df.empty:
        df = df[df.apply(
            lambda r: (r["cod_informante"], r["id_produto"]) in keys_set, axis=1
        )].reset_index(drop=True)
    return df


def get_prices_by_insumo_informado(pairs: list[tuple[str, str]]) -> pd.DataFrame:
    """Para cada (cod_informante, insumo_informado), devolve o preço mais
    recente coletado em tbl_ecommerce_collect_prod, fazendo a ponte por
    tbl_ecommerce_registered_ins_inform_prod.insumo_informado.

    Usado pela aba Carga para enriquecer a encomenda com o preço fresco do
    scraping. Retorna colunas: cod_informante, insumo_informado, preco, data_coleta.
    Levanta exceção em falha de query — o caller (UI) mostra o erro ao usuário.
    """
    if not pairs:
        return pd.DataFrame(columns=["cod_informante", "insumo_informado", "preco", "data_coleta", "url"])
    cods = sorted({p[0] for p in pairs})
    infs = sorted({p[1] for p in pairs})
    cods_in = ", ".join(_escape(c) for c in cods)
    infs_in = ", ".join(_escape(i) for i in infs)
    sql = f"""
        WITH bridge AS (
            SELECT
                TRIM(CAST(cb.cod_informante AS VARCHAR)) AS cod_informante,
                TRIM(CAST(cb.insumo_informado AS VARCHAR)) AS insumo_informado,
                cb.id_produto_site
            FROM {TABLE_CADASTRO} cb
            WHERE TRIM(CAST(cb.cod_informante AS VARCHAR)) IN ({cods_in})
              AND TRIM(CAST(cb.insumo_informado AS VARCHAR)) IN ({infs_in})
        ),
        prices AS (
            SELECT
                element_at(cp.cod_informante, 1) AS cod_informante,
                cp.id_produto,
                cp.preco,
                cp.data_coleta,
                cp.url,
                ROW_NUMBER() OVER (
                    PARTITION BY element_at(cp.cod_informante, 1), cp.id_produto
                    ORDER BY cp.data_coleta DESC
                ) AS rn
            FROM {TABLE_COLETA} cp
            WHERE element_at(cp.cod_informante, 1) IN ({cods_in})
        )
        SELECT b.cod_informante, b.insumo_informado, p.preco, p.data_coleta, p.url
        FROM bridge b
        LEFT JOIN prices p
          ON b.cod_informante = p.cod_informante
         AND b.id_produto_site = p.id_produto
         AND p.rn = 1
    """
    df = _run_query(sql, reuse=True)

    pairs_set = {(c, i) for c, i in pairs}
    if not df.empty:
        df = df[df.apply(
            lambda r: (r["cod_informante"], r["insumo_informado"]) in pairs_set, axis=1
        )].reset_index(drop=True)
    return df


def find_coletados_by_urls(cod_informante: str, urls: list[str]) -> pd.DataFrame:
    """Para cada URL em `urls`, devolve a coleta mais recente em
    tbl_ecommerce_collect_prod cuja `cp.url` é EXATAMENTE igual à URL solicitada
    para o `cod_informante` dado. Match case-sensitive, sem normalização.

    Devolve colunas: cod_informante, id_produto, url, descricao, data_coleta.
    Usado pelo cadastramento automático para casar URL_DO_INSUMO da encomenda
    contra a URL efetivamente coletada pelo scraping.
    """
    if not urls:
        return pd.DataFrame(
            columns=["cod_informante", "id_produto", "url", "descricao", "data_coleta"]
        )
    urls_in = ", ".join(_escape(u) for u in urls)
    sql = f"""
        SELECT * FROM (
            SELECT
                element_at(cp.cod_informante, 1) AS cod_informante,
                cp.id_produto,
                cp.url,
                cp.descricao,
                cp.data_coleta,
                ROW_NUMBER() OVER (
                    PARTITION BY cp.url
                    ORDER BY cp.data_coleta DESC
                ) AS rn
            FROM {TABLE_COLETA} cp
            WHERE element_at(cp.cod_informante, 1) = {_escape(cod_informante)}
              AND cp.url IN ({urls_in})
        )
        WHERE rn = 1
    """
    df = _run_query(sql, reuse=True).drop(columns=["rn"], errors="ignore")
    if not df.empty:
        df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce").dt.date
    return df


def get_existing_cadastros(cod_informante: str) -> set[tuple[str, str]]:
    """Pares (cod_informante, id_produto_site) já presentes em
    tbl_ecommerce_registered_ins_inform_prod para o informante dado. Usado para
    evitar propor cadastros duplicados no fluxo automático."""
    sql = f"""
        SELECT DISTINCT
            CAST(cb.cod_informante AS VARCHAR) AS cod_informante,
            CAST(cb.id_produto_site AS VARCHAR) AS id_produto_site
        FROM {TABLE_CADASTRO} cb
        WHERE CAST(cb.cod_informante AS VARCHAR) = {_escape(cod_informante)}
          AND cb.id_produto_site IS NOT NULL
    """
    df = _run_query(sql, reuse=True)
    if df.empty:
        return set()
    return set(zip(
        df["cod_informante"].astype(str),
        df["id_produto_site"].astype(str),
    ))


def get_informantes_in_bp(cod_informantes: list[str]) -> set[str]:
    """Subconjunto de `cod_informantes` que possui ao menos um registro em
    tbl_ecommerce_registered_ins_inform_prod (independente do insumo).

    Usado pela crítica geral para distinguir informantes ativos em BP
    (que poderiam ter coletas) de informantes sem qualquer cadastro.
    """
    if not cod_informantes:
        return set()
    cods = sorted({str(c).strip() for c in cod_informantes if str(c).strip()})
    if not cods:
        return set()
    cods_in = ", ".join(_escape(c) for c in cods)
    sql = f"""
        SELECT DISTINCT TRIM(CAST(cb.cod_informante AS VARCHAR)) AS cod_informante
        FROM {TABLE_CADASTRO} cb
        WHERE TRIM(CAST(cb.cod_informante AS VARCHAR)) IN ({cods_in})
    """
    df = _run_query(sql, reuse=True)
    if df.empty:
        return set()
    return set(df["cod_informante"].dropna().astype(str))


def get_cadastrados_bp_sample(cod_informante: str, limit: int = 30) -> pd.DataFrame:
    """Diagnóstico: amostra de tbl_ecommerce_registered_ins_inform_prod para um informante.
    Útil para entender o conteúdo real da coluna insumo_informado quando o
    join contra a encomenda não retorna nada."""
    sql = f"""
        SELECT TRIM(CAST(cb.cod_informante AS VARCHAR)) AS cod_informante,
               TRIM(CAST(cb.insumo_informado AS VARCHAR)) AS insumo_informado,
               cb.cod_insumo,
               cb.id_produto_site
        FROM {TABLE_CADASTRO} cb
        WHERE TRIM(CAST(cb.cod_informante AS VARCHAR)) = {_escape(cod_informante)}
        LIMIT {int(limit)}
    """
    return _run_query(sql, reuse=True)


def get_critica_data() -> pd.DataFrame:
    """Coletas cujo preço atual subiu mais de 30% em relação à coleta
    imediatamente anterior do mesmo (cod_informante, id_produto)."""
    sql = f"""
        WITH ranked AS (
            SELECT
                element_at(cp.cod_informante, 1) AS cod_informante,
                cp.nome_informante,
                cp.id_produto,
                cp.descricao,
                cp.marca,
                cp.data_coleta,
                cp.preco,
                ROW_NUMBER() OVER (
                    PARTITION BY element_at(cp.cod_informante, 1), cp.id_produto
                    ORDER BY cp.data_coleta DESC
                ) AS rn
            FROM {TABLE_COLETA} cp
            WHERE cp.preco IS NOT NULL AND cp.preco > 0
        )
        SELECT
            a.cod_informante,
            a.nome_informante,
            a.id_produto,
            a.descricao,
            a.marca,
            b.data_coleta AS data_anterior,
            b.preco       AS preco_anterior,
            a.data_coleta AS data_atual,
            a.preco       AS preco_atual,
            ((a.preco - b.preco) / b.preco) * 100.0 AS variacao_pct
        FROM ranked a
        JOIN ranked b
          ON a.cod_informante = b.cod_informante
         AND a.id_produto    = b.id_produto
         AND a.rn = 1 AND b.rn = 2
        WHERE a.preco > b.preco * 1.30
        ORDER BY variacao_pct DESC
    """
    try:
        return _run_query(sql, reuse=True)
    except Exception:
        return pd.DataFrame(columns=[
            "cod_informante", "nome_informante", "id_produto", "descricao", "marca",
            "data_anterior", "preco_anterior", "data_atual", "preco_atual", "variacao_pct",
        ])


