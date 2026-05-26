"""
FGV Identifica — Consulta de Coleta de Produtos
Aplicação Streamlit para consulta, filtro e exportação de dados de scraping.
"""

import calendar
import hashlib
import math
import os
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from auth import verify_login
from database import (
    find_coletados_by_urls,
    get_existing_cadastros,
    get_filter_options,
    get_informantes_coletaram,
    get_informantes_in_bp,
    get_monitoramento_data,
    get_page_data_with_count,
    get_prices_by_insumo_informado,
    save_cadastrado_bp,
)
from critica import run_validation
from export import export_carga_real, export_carga_reprovada

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="FGV - Sistema de Indentificador de Coleta",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS customizado
# ---------------------------------------------------------------------------
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Serif:wght@500;600;700&display=swap');

html, body, .stApp {
    font-family: 'IBM Plex Sans', 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 14.5px;
}
h1, h2, h3, h4 {
    font-family: 'IBM Plex Serif', Georgia, 'Times New Roman', serif;
    letter-spacing: 0.1px;
    font-weight: 600;
}

.main-header {
    background: #14365A;
    padding: 1.05rem 1.4rem;
    border-radius: 4px;
    margin-bottom: 1.2rem;
    border-left: 4px solid #C9A24B;
    box-shadow: 0 1px 0 rgba(255,255,255,0.04);
}
.main-header h1 {
    font-family: 'IBM Plex Serif', Georgia, serif;
    color: #FFFFFF;
    margin: 0;
    font-size: 1.30rem;
    font-weight: 600;
    letter-spacing: 0.2px;
}
.main-header p {
    color: #BFCBD7;
    margin: 0.2rem 0 0 0;
    font-size: 0.83rem;
    font-weight: 400;
    font-style: italic;
}

.metric-row {
    display: flex;
    gap: 0.75rem;
    margin: 0.4rem 0 1.1rem 0;
    flex-wrap: wrap;
}
.metric-card {
    background: #11253B;
    border: 1px solid #1F2D3D;
    border-top: 2px solid #1F4E79;
    border-radius: 3px;
    padding: 0.7rem 1rem 0.75rem 1rem;
    min-width: 170px;
    flex: 1;
}
.metric-card .label {
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 1.1px;
    color: #8DA0B5;
    margin: 0;
    font-weight: 600;
}
.metric-card .value {
    font-family: 'IBM Plex Serif', Georgia, serif;
    font-size: 1.55rem;
    font-weight: 600;
    margin: 0.2rem 0 0 0;
    color: #F4F7FB;
    font-feature-settings: 'tnum';
}
.metric-card.dim .value { color: #5E7388; }

.badge {
    display: inline-block;
    padding: 0.15rem 0.6rem;
    border-radius: 2px;
    font-size: 0.70rem;
    font-weight: 600;
    letter-spacing: 0.8px;
    text-transform: uppercase;
}
.badge-success { background: rgba(30,126,52,0.16);  color: #5FB373; border:1px solid #1E7E34; }
.badge-warning { background: rgba(183,121,31,0.16); color: #D9A55C; border:1px solid #B7791F; }
.badge-danger  { background: rgba(155,28,28,0.18);  color: #D87A7A; border:1px solid #9B1C1C; }
.badge-muted   { background: rgba(122,143,163,0.18);color: #BDC9D6; border:1px solid #7A8FA3; }
.badge-info    { background: rgba(31,78,121,0.22);  color: #87B0D6; border:1px solid #1F4E79; }

div[data-testid="stButton"] > button[kind="primary"] {
    background: #14365A;
    color: #fff;
    border: 1px solid #0E2A47;
    font-weight: 500;
    border-radius: 3px;
    letter-spacing: 0.2px;
}
div[data-testid="stButton"] > button[kind="primary"]:hover {
    background: #1F4E79;
    border-color: #14365A;
}
div[data-testid="stButton"] > button[kind="secondary"] {
    border-radius: 3px;
    border: 1px solid #2E4A66;
    background: transparent;
    color: #C8D2DC;
}
div[data-testid="stButton"] > button[kind="secondary"]:hover {
    background: rgba(31,78,121,0.18);
}

div[data-baseweb="tab-list"] {
    gap: 0;
    border-bottom: 1px solid #1F2D3D;
    padding-bottom: 0;
}
button[data-baseweb="tab"] {
    font-family: 'IBM Plex Sans', sans-serif;
    font-weight: 500;
    font-size: 0.92rem;
    color: #8DA0B5 !important;
    padding: 0.6rem 1.2rem !important;
    border-radius: 0 !important;
    background: transparent !important;
    border-bottom: 2px solid transparent;
}
button[data-baseweb="tab"]:hover { color: #E0E7EE !important; }
button[data-baseweb="tab"][aria-selected="true"] {
    color: #FFFFFF !important;
    font-weight: 600;
}
div[data-baseweb="tab-highlight"] {
    background: #C9A24B !important;
    height: 2px !important;
    border-radius: 0;
}

div[data-testid="stExpander"] {
    border: 1px solid #1F2D3D;
    border-left: 3px solid #1F4E79;
    border-radius: 3px;
    background: #0F2237;
}
div[data-testid="stExpander"] summary {
    font-weight: 500;
    font-size: 0.9rem;
    color: #C8D2DC;
    text-transform: uppercase;
    letter-spacing: 0.6px;
}

div[data-baseweb="input"], div[data-baseweb="select"] {
    border-radius: 3px !important;
}

div[data-testid="stDataEditor"], div[data-testid="stDataFrame"] {
    border-radius: 3px;
    overflow: hidden;
    border: 1px solid #1F2D3D;
}
div[data-testid="stDataFrame"] thead tr th {
    background: #11253B !important;
    color: #C8D2DC !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    font-size: 0.74rem !important;
    letter-spacing: 0.6px;
    border-bottom: 1px solid #1F4E79 !important;
}

section[data-testid="stSidebar"] {
    background: #0B1F33;
    border-right: 1px solid #1F2D3D;
}
section[data-testid="stSidebar"] * { color: #E0E7EE !important; }
section[data-testid="stSidebar"] hr {
    border-color: #1F2D3D !important;
    margin: 0.7rem 0;
}

.stCaption, p.caption {
    color: #8DA0B5 !important;
    font-size: 0.78rem !important;
    font-style: italic;
}

div[data-testid="stToggle"] label { font-weight: 500; }

.stApp h4 {
    font-family: 'IBM Plex Serif', Georgia, serif;
    color: #E8EEF4;
    border-bottom: 1px solid #1F2D3D;
    padding-bottom: 0.4rem;
    margin-bottom: 0.9rem;
    font-weight: 600;
}
</style>
    """,
    unsafe_allow_html=True,
)

PAGE_SIZE = 30


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def _init_state() -> None:
    defaults = {
        "user": None,
        "filter_cadastrados_bp": False,
        "page": 1,
        "table_version": 0,
        "filter_suffix": 0,
        "page_cache_key": None,
        "page_cache_df": None,
        "page_cache_total": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Página de Login
# ---------------------------------------------------------------------------

def _login_page() -> None:
    st.markdown(
        """
<style>
.login-wrap { padding-top: 2.4rem; }
.login-title { text-align:center; margin: 1.1rem 0 1.6rem 0; }
.login-title h2 {
    font-family: 'IBM Plex Serif', Georgia, serif;
    color:#E8EEF4;
    font-size:1.30rem;
    margin:0;
    font-weight:600;
    letter-spacing: 0.2px;
}
.login-title .rule {
    width: 56px;
    height: 2px;
    background: #C9A24B;
    margin: 0.6rem auto 0.7rem auto;
}
.login-title p {
    color:#8DA0B5;
    font-size:0.80rem;
    margin: 0;
    font-style: italic;
    letter-spacing: 0.4px;
}
div[data-testid="stForm"] {
    background: #0F2237;
    border: 1px solid #1F2D3D;
    border-top: 3px solid #C9A24B;
    border-radius: 3px;
    padding: 1.5rem 1.8rem !important;
    box-shadow: 0 2px 0 rgba(0,0,0,0.20);
}
</style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-wrap"></div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        logo_path = Path(__file__).parent / "fgv_ibre.png"
        if logo_path.exists():
            _, logo_col, _ = st.columns([1, 2, 1])
            with logo_col:
                st.image(str(logo_path), use_container_width=True)

        st.markdown(
            """
            <div class="login-title">
                <h2>Sistema de Consulta de Coleta de Preços</h2>
                <div class="rule"></div>
                <p>FGV IBRE — Acesso restrito</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.form("login_form"):
            nome = st.text_input("Usuário", placeholder="Usuário")
            senha = st.text_input("Senha", type="password", placeholder="Senha")
            entrar = st.form_submit_button(
                "Entrar", use_container_width=True, type="primary"
            )

            if entrar:
                user = verify_login(nome.strip(), senha)
                if user:
                    st.session_state.user = user
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")


# ---------------------------------------------------------------------------
# Sidebar (após login)
# ---------------------------------------------------------------------------

ROLE_COLOR = {
    "ADMIN":     "info",     # azul institucional
    "DEV":       "muted",    # cinza
    "VALIDADOR": "warning",  # ocre
    "ANALISTA":  "success",  # verde sóbrio
}


def _render_sidebar_authenticated() -> None:
    user = st.session_state.user
    role = user.get("role", "ANALISTA")

    with st.sidebar:
        logo_path = Path(__file__).parent / "fgv_ibre.png"
        if logo_path.exists():
            _, logo_col, _ = st.columns([0.3, 3, 0.3])
            with logo_col:
                st.image(str(logo_path), use_container_width=True)

        st.markdown(
            f"""
            <div style="text-align:center; margin: 0.5rem 0 0.3rem 0;">
                <div style="font-size: 0.78rem; color:#A4C3DB; margin-bottom: 0.2rem;">
                    Bem-vindo(a),
                </div>
                <div style="font-size: 1.1rem; font-weight: 700; color: #fff;">
                    {user['nome']}
                </div>
                <div style="margin-top: 0.5rem;">
                    <span class="badge badge-{ROLE_COLOR.get(role, 'info')}">{role}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.divider()

        if st.button("Encerrar Sessão", use_container_width=True):
            for key in ("user", "page", "table_version", "filter_suffix"):
                st.session_state.pop(key, None)
            st.rerun()

        st.markdown(
            """
            <div style="position:fixed; bottom:1rem; font-size:0.72rem; color:#7A95AA;">
                Desenvolvido por <strong style="color:#A4C3DB;">Felipe Fortunato</strong><br>
                <span style="opacity:0.8;">FGV IBRE</span>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Filtros — área principal
# ---------------------------------------------------------------------------

def _render_filters(options: dict) -> dict:
    sfx = st.session_state.filter_suffix

    def _reset_state():
        st.session_state.pop("last_table_edited", None)
        st.session_state.pop("last_table_original", None)
        st.session_state.filter_cadastrados_bp = False
        st.session_state["toggle_cadastrados_bp"] = False
        st.session_state.page = 1
        st.session_state.page_cache_key = None

    cod_inf = ean = sku = busca_texto = ""
    data_coleta = None

    with st.expander("Filtros", expanded=False):
        th1, th2 = st.columns([9, 1])
        with th2:
            if st.button("Limpar Filtro", key=f"limpar_{sfx}", use_container_width=True):
                st.session_state.filter_suffix += 1
                st.session_state.page = 1
                st.session_state.table_version += 1
                _reset_state()
                st.rerun()

        col1, col2, col3 = st.columns([2, 2, 2])

        with col1:
            cod_inf = st.selectbox(
                "Cód. Informante",
                options=[""] + options["cod_informante"],
                index=0,
                key=f"f_cod_{sfx}",
            )
            busca_texto = st.text_input(
                "Descrição",
                placeholder="Ex: nestle sabao po",
                key=f"f_texto_{sfx}",
                help=(
                    "Busca por palavras em qualquer ordem e ignora acentos/maiúsculas. "
                    "Ex.: 'nestle sabao po' encontra 'Sabão em Pó Nestlé 500g'."
                ),
            )

        with col2:
            ean = st.text_input("EAN", placeholder="Código exato", key=f"f_ean_{sfx}")
            sku = st.text_input("SKU", placeholder="Código exato", key=f"f_sku_{sfx}")

        with col3:
            data_min = date.fromisoformat(options["data_min"])
            data_max = date.fromisoformat(options["data_max"])
            _default_date = max(data_min, min(date.today(), data_max))
            data_coleta = st.date_input(
                "Data da Coleta",
                value=_default_date,
                min_value=data_min,
                max_value=data_max,
                format="DD/MM/YYYY",
                key=f"f_data_{sfx}",
            )

    filters: dict = {}
    if cod_inf:
        filters["cod_informante"] = cod_inf
    if ean.strip():
        filters["ean"] = ean.strip()
    if sku.strip():
        filters["sku"] = sku.strip()
    if busca_texto.strip():
        filters["busca_texto"] = busca_texto.strip()
    if data_coleta:
        filters["data_exata"] = data_coleta

    return filters


# ---------------------------------------------------------------------------
# Cabeçalho e métricas
# ---------------------------------------------------------------------------

def _render_banner() -> None:
    st.markdown(
        """
        <div class="main-header">
            <h1>Sistema de Consulta de Coleta de Preços</h1>
            <p>FGV IBRE — Consulta, crítica e exportação de dados de scraping</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _fmt_int(n: int) -> str:
    """Formato 1.234.567 (separador de milhar pt-BR)."""
    return f"{int(n):,}".replace(",", ".")


def _metric_cards(items: list[tuple[str, int | str]]) -> str:
    """Recebe lista de (label, valor) e devolve HTML com cards."""
    cards = []
    for label, value in items:
        try:
            value_int = int(value)
            dim = "dim" if value_int == 0 else ""
            value_str = _fmt_int(value_int)
        except (TypeError, ValueError):
            dim = ""
            value_str = str(value)
        cards.append(
            f'<div class="metric-card {dim}">'
            f'<p class="label">{label}</p>'
            f'<p class="value">{value_str}</p></div>'
        )
    return f'<div class="metric-row">{"".join(cards)}</div>'


def _render_metrics(placeholder, total: int, n_insumos: int) -> None:
    with placeholder:
        st.markdown(
            _metric_cards([
                ("Produtos Coletados", total),
                ("Insumos Cadastrados", n_insumos),
            ]),
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Toggle — filtrar cadastrados BP
# ---------------------------------------------------------------------------

def _render_cadastrados_bp_toggle() -> None:
    def _on_toggle():
        st.session_state.filter_cadastrados_bp = st.session_state["toggle_cadastrados_bp"]
        st.session_state.page = 1
        st.session_state.page_cache_key = None

    st.toggle(
        "Cadastrados BP",
        value=st.session_state.filter_cadastrados_bp,
        key="toggle_cadastrados_bp",
        on_change=_on_toggle,
        help="Filtrar apenas produtos já cadastrados no Banco de Preços",
    )


# ---------------------------------------------------------------------------
# Tabela interativa
# ---------------------------------------------------------------------------

COLUMN_CONFIG = {
    "cod_insumo": st.column_config.TextColumn("Cód. Insumo", width="small", pinned=True),
    "insumo_informado": st.column_config.TextColumn("Insumo Informado", width="medium", pinned=True),
    "data_coleta": st.column_config.DateColumn(
        "Data Coleta", format="DD/MM/YYYY", width="small", pinned=True,
    ),
    "cod_informante": st.column_config.TextColumn("Cód. Informante", width="small", pinned=True),
    "periodicidade": st.column_config.TextColumn("Periodicidade", width="small"),
    "tipo_preco": st.column_config.TextColumn("Tipo Preço", width="small"),
    "ean": st.column_config.TextColumn("EAN", width="medium"),
    "sku": st.column_config.TextColumn("SKU", width="medium"),
    "url": st.column_config.LinkColumn("URL", width="medium"),
    "descricao": st.column_config.TextColumn("Descrição", width="large"),
    "marca": st.column_config.TextColumn("Marca", width="small"),
    "uf": st.column_config.TextColumn("UF", width="small"),
    "preco": st.column_config.NumberColumn("Preço", format="R$ %.2f", width="small"),
    "preco_promocional": st.column_config.NumberColumn(
        "Preço Promo", format="R$ %.2f", width="small"
    ),
}

COLUMN_ORDER = [
    "cod_insumo", "insumo_informado", "data_coleta", "cod_informante",
    "descricao", "preco", "preco_promocional",
    "url", "marca", "ean", "sku", "uf", "tipo_preco", "periodicidade",
]

DISABLED_COLS = [
    "data_coleta", "cod_informante", "periodicidade", "tipo_preco",
    "ean", "sku", "url", "descricao", "marca", "uf",
    "preco", "preco_promocional",
]


def _table_key(scope: str, page: int, version: int) -> str:
    return f"tbl_{scope}_p{page}_v{version}"


def _render_table(df: pd.DataFrame, scope: str = "consulta") -> None:
    if df.empty:
        st.info("Nenhum produto encontrado com os filtros aplicados.")
        return

    key = _table_key(scope, st.session_state.page, st.session_state.table_version)
    height = 36 + len(df) * 35

    edited = st.data_editor(
        df,
        column_config=COLUMN_CONFIG,
        column_order=COLUMN_ORDER,
        disabled=DISABLED_COLS,
        hide_index=True,
        use_container_width=True,
        key=key,
        height=height,
    )

    st.session_state["last_table_edited"] = edited
    st.session_state["last_table_original"] = df


# ---------------------------------------------------------------------------
# Paginação
# ---------------------------------------------------------------------------

def _render_pagination(total: int, scope: str = "main") -> None:
    n_pages = max(1, math.ceil(total / PAGE_SIZE))
    if n_pages == 1:
        return

    col_prev, col_info, col_next = st.columns([1, 2, 1])

    with col_prev:
        if st.button(
            "◀ Anterior",
            disabled=st.session_state.page <= 1,
            use_container_width=True,
            key=f"prev_{scope}",
        ):
            st.session_state.page -= 1
            st.rerun()

    with col_info:
        st.markdown(
            f"<div style='text-align:center; padding-top:0.4rem; color:#ffffff; font-size:0.9rem;'>"
            f"Página <strong>{st.session_state.page}</strong> de <strong>{n_pages}</strong>"
            f"</div>",
            unsafe_allow_html=True,
        )

    with col_next:
        if st.button(
            "Próxima ▶",
            disabled=st.session_state.page >= n_pages,
            use_container_width=True,
            key=f"next_{scope}",
        ):
            st.session_state.page += 1
            st.rerun()


# ---------------------------------------------------------------------------
# Salvar cadastros
# ---------------------------------------------------------------------------

def _render_save_button() -> None:
    edited = st.session_state.get("last_table_edited")
    original = st.session_state.get("last_table_original")
    if edited is None or original is None:
        return

    changes = []
    for _, erow in edited.iterrows():
        prod_id = erow["id_produto"]
        orig_rows = original[original["id_produto"] == prod_id]
        if orig_rows.empty:
            continue
        orig_row = orig_rows.iloc[0]
        e_cod = str(erow["cod_insumo"]).strip() if pd.notna(erow["cod_insumo"]) else ""
        e_inf = str(erow["insumo_informado"]).strip() if pd.notna(erow["insumo_informado"]) else ""
        o_cod = str(orig_row["cod_insumo"]).strip() if pd.notna(orig_row["cod_insumo"]) else ""
        o_inf = str(orig_row["insumo_informado"]).strip() if pd.notna(orig_row["insumo_informado"]) else ""
        if (e_cod != o_cod or e_inf != o_inf) and (e_cod or e_inf):
            changes.append({
                "cod_informante": str(erow["cod_informante"]),
                "id_produto": prod_id,
                "cod_insumo": e_cod,
                "insumo_informado": e_inf,
            })

    if not changes:
        return

    n = len(changes)
    c1, c2, _ = st.columns([3, 2, 7])
    with c1:
        st.info(f"{n} item(ns) com cadastro pendente")
    with c2:
        if st.button("💾 Salvar Cadastros", type="primary", use_container_width=True):
            errors = []
            saved = 0
            for chg in changes:
                if not chg["cod_insumo"] or not chg["insumo_informado"]:
                    errors.append(f"{chg['id_produto']}: preencha cod_insumo e insumo_informado")
                    continue
                try:
                    save_cadastrado_bp(
                        chg["cod_informante"],
                        chg["id_produto"],
                        chg["cod_insumo"],
                        chg["insumo_informado"],
                    )
                    saved += 1
                except Exception as e:
                    errors.append(f"{chg['id_produto']}: {e}")
            if saved:
                st.success(f"{saved} registro(s) salvo(s) com sucesso.")
                st.session_state.pop("last_table_edited", None)
                st.session_state.pop("last_table_original", None)
                st.session_state.page_cache_key = None
                st.session_state.table_version += 1
                _get_filter_options_cached.clear()
                st.rerun()
            for err in errors:
                st.error(err)


# ---------------------------------------------------------------------------
# Página de Dados
# ---------------------------------------------------------------------------

def _data_page() -> None:
    metrics_placeholder = st.empty()

    options = _get_filter_options_cached()
    filters = _render_filters(options)

    if st.session_state.filter_cadastrados_bp:
        filters["cadastrados_bp"] = True

    filters_hash = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()
    if st.session_state.get("last_filters_hash") != filters_hash:
        st.session_state.page = 1
        st.session_state.last_filters_hash = filters_hash
        st.session_state.page_cache_key = None

    cache_key = f"{filters_hash}_{st.session_state.page}"
    if st.session_state.page_cache_key == cache_key:
        page_df = st.session_state.page_cache_df
        total = st.session_state.page_cache_total
    else:
        try:
            with st.spinner("Carregando dados..."):
                page_df, total = get_page_data_with_count(
                    filters, st.session_state.page, PAGE_SIZE
                )
            st.session_state.page_cache_key = cache_key
            st.session_state.page_cache_df = page_df
            st.session_state.page_cache_total = total
        except Exception as e:
            st.error(f"Erro ao consultar Athena: {e}")
            return

    cod_inf = filters.get("cod_informante", "")
    if cod_inf:
        n_insumos = len(options.get("cod_insumo_by_informante", {}).get(cod_inf, []))
    else:
        n_insumos = len(options.get("cod_insumo", []))
    _render_metrics(metrics_placeholder, total, n_insumos)
    _render_cadastrados_bp_toggle()

    sub_consulta, sub_auto = st.tabs([
        "Consulta", "Cadastramento Automático",
    ])
    with sub_consulta:
        _render_table(page_df, scope="consulta")
        _render_save_button()
        _render_pagination(total, scope="consulta")
    with sub_auto:
        _cadastro_automatico_subtab()


# ---------------------------------------------------------------------------
# Subaba: Cadastramento Automático por URL
# ---------------------------------------------------------------------------

def _cadastro_automatico_subtab() -> None:
    """Casa exatamente `URL_DO_INSUMO` da encomenda contra `cp.url` em
    tbl_ecommerce_collect_prod para o informante escolhido. Cada match vira
    proposta de cadastro (cod_insumo, NR_SEQ_INSINF) em
    tbl_ecommerce_registered_ins_inform_prod."""
    st.markdown("##### Cadastramento Automático por URL")
    st.caption(
        "Cruza `URL_DO_INSUMO` da encomenda com `url` coletada em "
        "tbl_ecommerce_collect_prod (match exato, case-sensitive). Cada par "
        "casado gera uma proposta de cadastro em `tbl_ecommerce_registered_ins_inform_prod`."
    )

    try:
        with st.spinner("Carregando encomenda..."):
            df_enc, fonte_enc, sf_err = _load_encomenda_cached()
    except Exception as e:
        st.error(f"Falha ao ler encomenda: {e}")
        return

    if sf_err:
        st.warning(f"Snowflake indisponível — usando fallback local. Erro: `{sf_err}`")

    if "URL_DO_INSUMO" not in df_enc.columns:
        st.error("Coluna `URL_DO_INSUMO` ausente na encomenda — não há como casar URLs.")
        return

    df_enc = df_enc.copy()
    df_enc["URL_DO_INSUMO"] = df_enc["URL_DO_INSUMO"].astype(str).str.strip()
    df_enc = df_enc[~df_enc["URL_DO_INSUMO"].str.lower().isin(["", "nan", "none", "<na>"])]

    if df_enc.empty:
        st.info("Nenhum insumo da encomenda possui `URL_DO_INSUMO` preenchida.")
        return

    informantes = (
        df_enc[["CD_INFORM", "NM_INFORM"]]
        .dropna(subset=["CD_INFORM"])
        .drop_duplicates(subset=["CD_INFORM"])
        .sort_values("CD_INFORM")
    )
    if informantes.empty:
        st.info("Nenhum informante elegível na encomenda.")
        return

    opcoes = [
        (str(r.CD_INFORM), f"{r.CD_INFORM} — {r.NM_INFORM}")
        for r in informantes.itertuples(index=False)
    ]
    rotulo_por_cod = {cod: lbl for cod, lbl in opcoes}

    cod_inf = st.selectbox(
        "Informante",
        options=[""] + [c for c, _ in opcoes],
        format_func=lambda c: "— selecione —" if c == "" else rotulo_por_cod.get(c, c),
        key="cad_auto_inf",
    )
    if not cod_inf:
        st.info("Selecione um informante para procurar matches.")
        return

    sub = df_enc[df_enc["CD_INFORM"] == cod_inf].copy()
    sub = sub.dropna(subset=["NR_SEQ_INSINF", "CD_INSUMO"])
    sub = sub[sub["URL_DO_INSUMO"].str.len() > 0]
    # Mais de uma linha da encomenda pode declarar a mesma URL — mantém
    # a primeira ocorrência para garantir mapeamento URL → (NR_SEQ, CD_INSUMO).
    sub = sub.drop_duplicates(subset=["URL_DO_INSUMO"])

    if sub.empty:
        st.info(f"Nenhuma URL preenchida na encomenda para o informante {cod_inf}.")
        return

    urls = sub["URL_DO_INSUMO"].astype(str).tolist()

    try:
        with st.spinner(f"Procurando matches em {len(urls)} URL(s) da encomenda..."):
            df_match = _find_coletados_by_urls_cached(cod_inf, tuple(urls))
    except Exception as e:
        st.error(f"Erro ao consultar Athena: {e}")
        return

    if df_match.empty:
        st.markdown(
            _metric_cards([
                ("URLs Candidatas", len(urls)),
                ("Matches Encontrados", 0),
                ("Já Cadastrados", 0),
                ("Novos para Cadastrar", 0),
            ]),
            unsafe_allow_html=True,
        )
        st.info(
            f"Nenhuma URL da encomenda casou exatamente com `cp.url` em "
            f"tbl_ecommerce_collect_prod para o informante {cod_inf}."
        )
        return

    cols_enc = ["URL_DO_INSUMO", "NR_SEQ_INSINF", "CD_INSUMO"]
    if "NM_INSUMO" in sub.columns:
        cols_enc.append("NM_INSUMO")
    proposed = df_match.merge(
        sub[cols_enc],
        left_on="url",
        right_on="URL_DO_INSUMO",
        how="left",
    )
    proposed = proposed.dropna(subset=["NR_SEQ_INSINF", "CD_INSUMO"]).reset_index(drop=True)

    try:
        existentes = _get_existing_cadastros_cached(cod_inf)
    except Exception:
        existentes = set()
    proposed["_existe"] = [
        (str(c), str(p)) in existentes
        for c, p in zip(proposed["cod_informante"], proposed["id_produto"])
    ]
    novos = proposed[~proposed["_existe"]].copy().reset_index(drop=True)
    ja_cad = proposed[proposed["_existe"]].copy()

    st.markdown(
        _metric_cards([
            ("URLs Candidatas", len(urls)),
            ("Matches Encontrados", len(proposed)),
            ("Já Cadastrados", len(ja_cad)),
            ("Novos para Cadastrar", len(novos)),
        ]),
        unsafe_allow_html=True,
    )

    if novos.empty:
        st.success("Todos os matches já estão cadastrados em BP — nada a fazer.")
        return

    nm_insumo = (
        novos["NM_INSUMO"].astype(str).values
        if "NM_INSUMO" in novos.columns
        else [""] * len(novos)
    )
    view = pd.DataFrame({
        "cod_informante":   novos["cod_informante"].astype(str).values,
        "id_produto":       novos["id_produto"].astype(str).values,
        "descricao":        novos["descricao"].astype(str).values,
        "url":              novos["url"].astype(str).values,
        "cod_insumo":       novos["CD_INSUMO"].astype(str).values,
        "insumo_informado": novos["NR_SEQ_INSINF"].astype(str).values,
        "nome_insumo":      nm_insumo,
        "data_coleta":      novos["data_coleta"].values,
    })

    st.dataframe(
        view,
        column_config={
            "cod_informante":   st.column_config.TextColumn("Cód. Informante", width="small"),
            "id_produto":       st.column_config.TextColumn("ID Produto", width="small"),
            "descricao":        st.column_config.TextColumn("Descrição", width="large"),
            "url":              st.column_config.LinkColumn("URL", width="medium"),
            "cod_insumo":       st.column_config.TextColumn("Cód. Insumo", width="small"),
            "insumo_informado": st.column_config.TextColumn("Insumo Informado", width="small"),
            "nome_insumo":      st.column_config.TextColumn("Nome Insumo", width="medium"),
            "data_coleta":      st.column_config.DateColumn("Última Coleta", format="DD/MM/YYYY", width="small"),
        },
        use_container_width=True,
        hide_index=True,
    )

    c1, c2, _ = st.columns([3, 2, 7])
    with c1:
        st.info(f"{len(novos)} cadastro(s) propostos por match exato de URL.")
    with c2:
        if st.button(
            f"💾 Salvar {len(novos)} Cadastro(s)",
            type="primary",
            use_container_width=True,
            key="cad_auto_save",
        ):
            errors: list[str] = []
            saved = 0
            for _, row in novos.iterrows():
                try:
                    save_cadastrado_bp(
                        str(row["cod_informante"]),
                        str(row["id_produto"]),
                        str(row["CD_INSUMO"]),
                        str(row["NR_SEQ_INSINF"]),
                    )
                    saved += 1
                except Exception as e:
                    errors.append(f"{row['id_produto']}: {e}")
            if saved:
                st.success(f"{saved} cadastro(s) salvo(s) com sucesso.")
                _get_filter_options_cached.clear()
                _get_existing_cadastros_cached.clear()
                st.session_state.page_cache_key = None
                st.session_state.table_version += 1
                st.rerun()
            for err in errors:
                st.error(err)


# ---------------------------------------------------------------------------
# Aba Monitoramento
# ---------------------------------------------------------------------------

def _decendio_atual_intervalo(hoje: date | None = None) -> tuple[date, date]:
    """(início, fim) do decêndio em que o dia corrente cai:
      - dias 1-10  → (1, 10)
      - dias 11-20 → (11, 20)
      - dias 21-fim → (21, último dia do mês)
    """
    hoje = hoje or date.today()
    last_day = calendar.monthrange(hoje.year, hoje.month)[1]
    if hoje.day <= 10:
        ini, fim = 1, 10
    elif hoje.day <= 20:
        ini, fim = 11, 20
    else:
        ini, fim = 21, last_day
    return hoje.replace(day=ini), hoje.replace(day=fim)


def _style_status(v):
    if v == "Ativo":   return "color:#5FB373; font-weight:600;"
    if v == "Inativo": return "color:#D87A7A; font-weight:600;"
    return ""


def _style_exec(v):
    if v == "Sucesso": return "color:#5FB373; font-weight:600;"
    if v == "Atraso":  return "color:#D9A55C; font-weight:600;"
    return ""


def _monitoramento_page() -> None:
    st.markdown("#### Monitoramento de Informantes")

    try:
        with st.spinner("Carregando monitoramento..."):
            df = _get_monitoramento_cached()
    except Exception as e:
        st.error(f"Erro ao consultar Athena: {e}")
        return

    if df.empty:
        st.info("Nenhum informante cadastrado na tabela de monitoramento.")
        return

    df = df.copy()

    today = pd.Timestamp.today().normalize()
    parsed_dates = pd.to_datetime(df["ultima_coleta"], errors="coerce").dt.normalize()
    ativos_mask = df["status"] == "Ativo"
    df["execucao"] = ""
    df.loc[ativos_mask & (parsed_dates == today), "execucao"] = "Sucesso"
    df.loc[ativos_mask & (parsed_dates < today), "execucao"] = "Atraso"

    tab_geral, tab_dec = st.tabs(["Visão Geral", "Atraso no Decêndio Corrente"])
    with tab_geral:
        _monitoramento_visao_geral(df)
    with tab_dec:
        _monitoramento_atraso_decendio(df)


def _monitoramento_visao_geral(df: pd.DataFrame) -> None:
    ativos = df[df["status"] == "Ativo"].copy()

    total_ativos = len(ativos)
    coletas_concluidas = int((ativos["execucao"] == "Sucesso").sum())
    coletas_atraso = int((ativos["execucao"] == "Atraso").sum())

    st.markdown(
        _metric_cards([
            ("Informantes Ativos",    total_ativos),
            ("Já Coletaram",          coletas_concluidas),
            ("Ainda Não Coletaram",   coletas_atraso),
        ]),
        unsafe_allow_html=True,
    )

    if ativos.empty:
        st.info("Nenhum informante ativo encontrado.")
        return

    col_cfg = {
        "cod_informante": st.column_config.TextColumn("Cód. Informante"),
        "dominio":        st.column_config.TextColumn("Domínio"),
        "frete":          st.column_config.TextColumn("Frete"),
        "tipo_preco":     st.column_config.TextColumn("Tipo de Preço"),
        "ultima_coleta":  st.column_config.TextColumn("Última Coleta"),
        "execucao":       st.column_config.TextColumn("Coletou?"),
    }

    visible_cols = [
        "cod_informante", "dominio", "frete",
        "tipo_preco", "ultima_coleta", "execucao",
    ]
    styled = (
        ativos[visible_cols].style
        .map(_style_exec, subset=["execucao"])
    )
    st.dataframe(styled, use_container_width=True, column_config=col_cfg, hide_index=True)
    st.caption(f"{total_ativos} informante(s) ativo(s) — {coletas_concluidas} coletaram, {coletas_atraso} ainda não.")


def _monitoramento_atraso_decendio(df_monitor: pd.DataFrame) -> None:
    dec_ini, dec_fim = _decendio_atual_intervalo()
    data_prev = _data_prevista_corrente()

    try:
        with st.spinner("Carregando encomenda..."):
            df_enc, fonte_enc, sf_err = _load_encomenda_cached()
    except Exception as e:
        st.error(f"Falha ao ler encomenda: {e}")
        return
    if sf_err:
        st.warning(f"Snowflake indisponível — usando fallback local. Erro: `{sf_err}`")

    # Informantes esperados no decêndio corrente: DATA_PREVISTA == fim do decêndio.
    esperados = (
        df_enc[df_enc["DATA_PREVISTA"] == data_prev]
        [["CD_INFORM", "NM_INFORM"]]
        .dropna(subset=["CD_INFORM"])
        .drop_duplicates(subset=["CD_INFORM"])
        .sort_values("CD_INFORM")
        .reset_index(drop=True)
    )

    st.caption(
        f"Decêndio corrente: **{dec_ini:%d/%m/%Y}** a **{dec_fim:%d/%m/%Y}** "
        f"(DATA_PREVISTA = {data_prev:%d/%m/%Y}). "
        "Cruzamento entre informantes previstos na encomenda e `cod_informante` "
        "presente em `tbl_ecommerce_collect_prod` dentro do intervalo do decêndio."
    )

    if esperados.empty:
        st.info(
            f"Nenhum informante previsto na encomenda para DATA_PREVISTA = "
            f"{data_prev:%d/%m/%Y}."
        )
        return

    try:
        with st.spinner("Verificando coletas no decêndio..."):
            coletados = _get_informantes_coletaram_cached(
                dec_ini.isoformat(), dec_fim.isoformat()
            )
    except Exception as e:
        st.error(f"Erro ao consultar Athena: {e}")
        return

    esperados["CD_INFORM"] = esperados["CD_INFORM"].astype(str)

    # Enriquece com dados do monitoramento e exclui informantes com frete = "Sim"
    # (primeiro momento da análise foca em informantes sem frete).
    mon_cols = ["cod_informante", "dominio", "frete", "status", "tipo_preco", "ultima_coleta"]
    df_mon_sel = df_monitor[mon_cols].copy()
    df_mon_sel["cod_informante"] = df_mon_sel["cod_informante"].astype(str)
    esperados = esperados.merge(
        df_mon_sel, left_on="CD_INFORM", right_on="cod_informante", how="left"
    )
    n_com_frete = int((esperados["frete"] == "Sim").sum())
    esperados = esperados[esperados["frete"] != "Sim"].reset_index(drop=True)

    n_nao_ativos = int((esperados["status"] != "Ativo").sum())
    esperados = esperados[esperados["status"] == "Ativo"].reset_index(drop=True)

    esperados["coletou"] = esperados["CD_INFORM"].isin(coletados)
    em_atraso = esperados[~esperados["coletou"]].copy()

    n_esperados = len(esperados)
    n_coletaram = int(esperados["coletou"].sum())
    n_atraso = len(em_atraso)
    pct = (n_atraso / n_esperados * 100) if n_esperados else 0.0

    if n_com_frete:
        st.caption(f"{n_com_frete} informante(s) com frete = 'Sim' ocultados nesta visão.")
    if n_nao_ativos:
        st.caption(f"{n_nao_ativos} informante(s) com status diferente de 'Ativo' ocultados nesta visão.")

    st.markdown(
        _metric_cards([
            ("Previstos no Decêndio", n_esperados),
            ("Já Coletaram",          n_coletaram),
            ("Em Atraso",             n_atraso),
            ("% em Atraso",           f"{pct:.1f}%"),
        ]),
        unsafe_allow_html=True,
    )

    col_cfg = {
        "cod_informante":  st.column_config.TextColumn("Cód. Informante"),
        "nome_informante": st.column_config.TextColumn("Nome Informante"),
        "dominio":         st.column_config.TextColumn("Domínio"),
        "frete":           st.column_config.TextColumn("Frete"),
        "status":          st.column_config.TextColumn("Status"),
        "tipo_preco":      st.column_config.TextColumn("Tipo de Preço"),
        "ultima_coleta":   st.column_config.TextColumn("Última Coleta"),
    }

    def _render_grupo(sub: pd.DataFrame, vazio_msg: str, rodape: str) -> None:
        if sub.empty:
            st.info(vazio_msg)
            return
        visible = pd.DataFrame({
            "cod_informante":  sub["CD_INFORM"].astype(str).values,
            "nome_informante": sub["NM_INFORM"].astype(str).values,
            "dominio":         sub["dominio"].values,
            "frete":           sub["frete"].values,
            "status":          sub["status"].values,
            "tipo_preco":      sub["tipo_preco"].values,
            "ultima_coleta":   sub["ultima_coleta"].values,
        })
        styled = visible.style.map(_style_status, subset=["status"])
        st.dataframe(styled, use_container_width=True, column_config=col_cfg, hide_index=True)
        st.caption(rodape.format(n=len(visible)))

    ja_coletaram = esperados[esperados["coletou"]].copy()

    tab_atraso, tab_coletaram = st.tabs(
        [f"Em Atraso ({n_atraso})", f"Já Coletaram ({n_coletaram})"]
    )
    with tab_atraso:
        _render_grupo(
            em_atraso,
            "Todos os informantes previstos já coletaram no decêndio corrente.",
            "{n} informante(s) em atraso listado(s)",
        )
    with tab_coletaram:
        _render_grupo(
            ja_coletaram,
            "Nenhum informante previsto coletou ainda no decêndio corrente.",
            "{n} informante(s) que já coletaram no decêndio",
        )


# ---------------------------------------------------------------------------
# Aba Carga
# ---------------------------------------------------------------------------

ENCOMENDA_FILE = Path(__file__).parent / "encomenda.xlsx"


def _seq_to_clean_str(series: pd.Series) -> pd.Series:
    """Converte ints/floats/strings de NR_SEQ_INSINF para string sem '.0'.
    'NaN' do Int64 vira '<NA>' — não bate com nenhum código real."""
    return pd.to_numeric(series, errors="coerce").astype("Int64").astype(str)


@st.cache_data(ttl=300)
def _load_encomenda_cached() -> tuple[pd.DataFrame, str, str | None]:
    """Carrega a encomenda do Snowflake (TBL_ENCOMENDA_DEC) ou, em falha,
    do encomenda.xlsx local. Normaliza códigos float → int-string.
    Devolve (df, fonte, snowflake_error) onde fonte ∈ {'snowflake', 'arquivo local'}
    e snowflake_error contém a exceção quando o fallback foi acionado."""
    from snowflake_io import get_encomenda_df

    snowflake_error: str | None = None
    try:
        df = get_encomenda_df()
        fonte = "snowflake"
    except Exception as e:
        snowflake_error = f"{type(e).__name__}: {e}"
        if not ENCOMENDA_FILE.exists():
            raise RuntimeError(
                f"Falha ao ler do Snowflake ({e}) e arquivo local "
                f"`{ENCOMENDA_FILE.name}` ausente."
            ) from e
        df = pd.read_excel(ENCOMENDA_FILE)
        fonte = f"arquivo local ({ENCOMENDA_FILE.name})"

    df["NR_SEQ_INSINF"] = _seq_to_clean_str(df["NR_SEQ_INSINF"])
    df["CD_INFORM"]     = _seq_to_clean_str(df["CD_INFORM"])
    df["CD_INSUMO"]     = _seq_to_clean_str(df["CD_INSUMO"])
    # Snowflake entrega DATA_PREVISTA como DATE; xlsx local como string 'DD/MM/YYYY'.
    # Normaliza para datetime.date para comparações exatas no filtro por data.
    if "DATA_PREVISTA" in df.columns:
        df["DATA_PREVISTA"] = pd.to_datetime(
            df["DATA_PREVISTA"], errors="coerce", dayfirst=True
        ).dt.date
    return df, fonte, snowflake_error


def _dedupe_carga(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por (CD_INFORM, NR_SEQ_INSINF), mantendo DT_COL_PRECCOL mais recente."""
    _dp = pd.to_datetime(df.get("DT_COL_PRECCOL"), errors="coerce")
    return (
        df.assign(_data_ord=_dp)
        .sort_values("_data_ord", ascending=False, na_position="last")
        .drop_duplicates(subset=["CD_INFORM", "NR_SEQ_INSINF"], keep="first")
        .drop(columns=["_data_ord"])
        .reset_index(drop=True)
    )


def _data_prevista_corrente(hoje: date | None = None) -> date:
    """Fim do decêndio em que o dia corrente cai:
    - dias 1-10  → dia 10
    - dias 11-20 → dia 20
    - dias 21-31 → dia 30 (ou último dia do mês, se < 30)
    Exemplo: hoje=12/05 → 20/05; hoje=27/02 → 28/02 (ou 29 em ano bissexto).
    """
    hoje = hoje or date.today()
    last_day = calendar.monthrange(hoje.year, hoje.month)[1]
    if hoje.day <= 10:
        target = 10
    elif hoje.day <= 20:
        target = 20
    else:
        target = min(30, last_day)
    return hoje.replace(day=target)


def _run_critica(df_match: pd.DataFrame) -> tuple[pd.DataFrame, str | None]:
    """Roda a crítica analítica (Snowflake regras + cálculos pandas).
    Retorna (df enriquecido com validacao_*, manutencao, motivo_manutencao,
    erro_or_None). Em falha de Snowflake, marca tudo como manutenção com
    motivo "SEM REGRAS DISPONIVEIS" e devolve a mensagem de erro para a UI."""
    from snowflake_io import get_critica_rules_df

    insumos = sorted({str(x) for x in df_match["NR_SEQ_INSINF"].dropna()})
    try:
        rules = get_critica_rules_df(insumos)
    except Exception as e:
        df_fail = df_match.copy()
        df_fail["ultimo_preco_db"] = pd.NA
        df_fail["variacao_atual_pct"] = pd.NA
        df_fail["vl_lim_inf_var"] = pd.NA
        df_fail["vl_lim_sup_var"] = pd.NA
        df_fail["faltas_consecutivas_aceitas"] = pd.NA
        df_fail["total_faltas_com_atual"] = pd.NA
        df_fail["validacao_variacao"] = "SEM PRECO ANTERIOR"
        df_fail["validacao_faltas"] = "FALTAS DENTRO DO LIMITE"
        df_fail["validacao_incidencia"] = "FRETE OK"
        df_fail["manutencao"] = 1
        df_fail["motivo_manutencao"] = "SEM REGRAS DISPONIVEIS (FALHA SNOWFLAKE)"
        return df_fail, f"{type(e).__name__}: {e}"

    return run_validation(df_match, rules), None


@st.cache_data(ttl=300, show_spinner=False)
def _compute_critica_geral(data_prev_iso: str, hoje_iso: str) -> dict:
    """Pipeline da crítica para todos os insumos da encomenda do dec corrente.

    Classifica cada (CD_INFORM, NR_SEQ_INSINF) com DATA_PREVISTA == data_prev em
    cinco buckets:
      - aprovados: par cadastrado em tbl_ecommerce_registered_ins_inform_prod,
        com coleta em tbl_ecommerce_collect_prod e passou crítica.
      - reprovados: par cadastrado, com coleta + falhou crítica (preço fora
        do range, faltas consecutivas excedidas, frete não declarado).
      - falta: par cadastrado, sem coleta e hoje >= DATA_PREVISTA.
      - pendente: par cadastrado, sem coleta e hoje < DATA_PREVISTA.
      - nao_cadastrados: par não cadastrado, mas o CD_INFORM tem ao menos
        um cadastro em tbl_ecommerce_registered_ins_inform_prod (informante
        ativo em BP cujo insumo específico ainda não foi vinculado).

    Linhas cujo CD_INFORM não consta em tbl_ecommerce_registered_ins_inform_prod
    são contabilizadas em `n_informante_fora_bp` e ficam fora dos cinco buckets.
    """
    df_enc, _, _ = _load_encomenda_cached()
    data_prev = date.fromisoformat(data_prev_iso)
    hoje = date.fromisoformat(hoje_iso)

    empty_result = {
        "aprovados":       pd.DataFrame(),
        "reprovados":      pd.DataFrame(),
        "falta":           pd.DataFrame(),
        "pendente":        pd.DataFrame(),
        "nao_cadastrados": pd.DataFrame(),
        "data_prev":       data_prev,
        "hoje":            hoje,
        "n_total":               0,
        "n_informante_fora_bp":  0,
        "critica_error":   None,
    }

    df_match = df_enc[df_enc["DATA_PREVISTA"] == data_prev].copy()
    if df_match.empty:
        return empty_result

    df_match = _dedupe_carga(df_match)
    df_match["CD_INFORM"]     = df_match["CD_INFORM"].astype(str).str.strip()
    df_match["NR_SEQ_INSINF"] = df_match["NR_SEQ_INSINF"].astype(str).str.strip()
    n_total = len(df_match)

    pairs = list({
        (cd, ns)
        for cd, ns in zip(df_match["CD_INFORM"], df_match["NR_SEQ_INSINF"])
    })
    try:
        df_prices = get_prices_by_insumo_informado(pairs)
    except Exception:
        df_prices = pd.DataFrame()

    bridge_pairs: set[tuple[str, str]] = set()
    if not df_prices.empty:
        df_prices = df_prices.copy()
        df_prices["cod_informante"]   = df_prices["cod_informante"].astype(str).str.strip()
        df_prices["insumo_informado"] = df_prices["insumo_informado"].astype(str).str.strip()
        if "url" not in df_prices.columns:
            df_prices["url"] = pd.NA
        bridge_pairs = set(zip(df_prices["cod_informante"], df_prices["insumo_informado"]))

        df_prices_collected = (
            df_prices.dropna(subset=["preco"])
            .sort_values("preco", ascending=False)
            .drop_duplicates(subset=["cod_informante", "insumo_informado"], keep="first")
            .rename(columns={
                "cod_informante":   "CD_INFORM",
                "insumo_informado": "NR_SEQ_INSINF",
                "preco":            "_ATHENA_PRECO",
                "data_coleta":      "_ATHENA_DATA",
                "url":              "_ATHENA_URL",
            })
        )
        df_match = df_match.merge(
            df_prices_collected[["CD_INFORM", "NR_SEQ_INSINF",
                                 "_ATHENA_PRECO", "_ATHENA_DATA", "_ATHENA_URL"]],
            on=["CD_INFORM", "NR_SEQ_INSINF"],
            how="left",
        )
    else:
        df_match["_ATHENA_PRECO"] = pd.NA
        df_match["_ATHENA_DATA"]  = pd.NaT
        df_match["_ATHENA_URL"]   = pd.NA

    df_match["VL_PRECCOL"]     = pd.to_numeric(df_match["_ATHENA_PRECO"], errors="coerce")
    df_match["DT_COL_PRECCOL"] = df_match["_ATHENA_DATA"]
    df_match["URL_COLETADA"]   = df_match["_ATHENA_URL"]
    df_match = df_match.drop(columns=["_ATHENA_PRECO", "_ATHENA_DATA", "_ATHENA_URL"])

    # Cadastrado em BP = par (CD_INFORM, NR_SEQ_INSINF) consta na bridge.
    df_match["_em_bp"] = [
        (cd, ns) in bridge_pairs
        for cd, ns in zip(df_match["CD_INFORM"], df_match["NR_SEQ_INSINF"])
    ]

    # Informantes com qualquer cadastro em BP (mesmo que para insumos fora
    # da encomenda) — necessário para separar "Não cadastrados" de
    # "Informante totalmente fora de BP".
    cods_encomenda = sorted({cd for cd, _ in pairs})
    try:
        bridge_informantes = get_informantes_in_bp(cods_encomenda)
    except Exception:
        bridge_informantes = {cd for cd, _ in bridge_pairs}
    bridge_informantes |= {cd for cd, _ in bridge_pairs}
    df_match["_inf_em_bp"] = df_match["CD_INFORM"].isin(bridge_informantes)

    df_em_bp     = df_match[df_match["_em_bp"]].copy()
    df_nao_cad   = df_match[(~df_match["_em_bp"]) & df_match["_inf_em_bp"]].copy().reset_index(drop=True)
    df_fora_bp   = df_match[(~df_match["_em_bp"]) & (~df_match["_inf_em_bp"])]
    n_fora_bp    = int(len(df_fora_bp))

    coletado_mask = df_em_bp["VL_PRECCOL"].notna()
    df_coletado     = df_em_bp[coletado_mask].copy().reset_index(drop=True)
    df_nao_coletado = df_em_bp[~coletado_mask].copy().reset_index(drop=True)

    critica_error = None
    if not df_coletado.empty:
        df_crit, critica_error = _run_critica(df_coletado)
        df_aprov = df_crit[df_crit["manutencao"] == 0].copy().reset_index(drop=True)
        df_repro = df_crit[df_crit["manutencao"] == 1].copy().reset_index(drop=True)
    else:
        df_aprov = pd.DataFrame()
        df_repro = pd.DataFrame()

    if not df_nao_coletado.empty:
        falta_mask = df_nao_coletado["DATA_PREVISTA"].apply(
            lambda d: isinstance(d, date) and d <= hoje
        )
        df_falta    = df_nao_coletado[falta_mask].copy().reset_index(drop=True)
        df_pendente = df_nao_coletado[~falta_mask].copy().reset_index(drop=True)
    else:
        df_falta    = pd.DataFrame()
        df_pendente = pd.DataFrame()

    for df in (df_aprov, df_repro, df_falta, df_pendente, df_nao_cad):
        for c in ("_em_bp", "_inf_em_bp"):
            if c in df.columns:
                df.drop(columns=[c], inplace=True)

    return {
        "aprovados":       df_aprov,
        "reprovados":      df_repro,
        "falta":           df_falta,
        "pendente":        df_pendente,
        "nao_cadastrados": df_nao_cad,
        "data_prev":       data_prev,
        "hoje":            hoje,
        "n_total":         int(n_total),
        "n_informante_fora_bp": n_fora_bp,
        "critica_error":   critica_error,
    }


_CRITICA_INFO_COLS = [
    "CD_INFORM", "NM_INFORM", "NR_SEQ_INSINF", "NM_INSUMO",
    "CD_INSUMO", "CD_TPPRECO", "CD_PERIOD", "DATA_PREVISTA",
]


def _render_critica_aprovados(df: pd.DataFrame, file_base: str) -> None:
    if df.empty:
        st.info("Nenhum insumo aprovado.")
        return

    view_cols = [c for c in (
        _CRITICA_INFO_COLS +
        ["VL_PRECCOL", "DT_COL_PRECCOL", "ultimo_preco_db",
         "variacao_atual_pct", "URL_COLETADA"]
    ) if c in df.columns]

    st.dataframe(
        df[view_cols],
        column_config={
            "DATA_PREVISTA":      st.column_config.DateColumn("Data Prevista", format="DD/MM/YYYY"),
            "DT_COL_PRECCOL":     st.column_config.DateColumn("Data Coleta", format="DD/MM/YYYY"),
            "VL_PRECCOL":         st.column_config.NumberColumn("Preço", format="R$ %.2f"),
            "ultimo_preco_db":    st.column_config.NumberColumn("Último Preço BP", format="R$ %.2f"),
            "variacao_atual_pct": st.column_config.NumberColumn("Variação", format="%.2f%%"),
            "URL_COLETADA":       st.column_config.LinkColumn("URL Coletada"),
        },
        use_container_width=True,
        hide_index=True,
    )

    try:
        xls_bytes = export_carga_real(df)
    except Exception as e:
        st.error(f"Falha ao gerar arquivo aprovado: {e}")
        return

    st.download_button(
        label=f"Baixar Carga Aprovada ({len(df)})",
        data=xls_bytes,
        file_name=f"carga_BP_aprovada_{file_base}.xls",
        mime="application/vnd.ms-excel",
        type="primary",
        key="critica_dl_aprov",
        use_container_width=True,
    )


def _render_critica_reprovados(df: pd.DataFrame, file_base: str) -> None:
    if df.empty:
        st.info("Nenhum insumo reprovado.")
        return

    st.info(
        "Edite **Justificativa Livre** e **URL do insumo** antes de baixar a "
        "carga. As demais colunas são apenas para consulta."
    )

    def _col(name: str, default=""):
        if name in df.columns:
            return df[name]
        return pd.Series([default] * len(df))

    n = len(df)
    url_coletada  = _col("URL_COLETADA", "").fillna("").astype(str)
    url_encomenda = _col("URL_DO_INSUMO", "").fillna("").astype(str)
    url_final = url_coletada.where(url_coletada.str.len() > 0, url_encomenda)

    view = pd.DataFrame({
        "JOB":                       _col("JOB", "").astype(str),
        "Insumo Informado":          _col("NR_SEQ_INSINF", "").astype(str),
        "Sinônimo Insumo Informado": _col("DS_SINO_NOME_INS_INSINF", "").astype(str),
        "Código do Informante":      _col("CD_INFORM", "").astype(str),
        "Tipo de Preço":             _col("CD_TPPRECO", "").astype(str),
        "Estado":                    _col("ESTADO", "").astype(str),
        "Periodicidade":             _col("CD_PERIOD", "").astype(str),
        "Data do Preço":             pd.to_datetime(_col("DT_COL_PRECCOL", None), errors="coerce").dt.date,
        "Data Prevista":             pd.to_datetime(_col("DATA_PREVISTA", None), errors="coerce").dt.date,
        "Valor do Preço":            pd.to_numeric(_col("VL_PRECCOL", None), errors="coerce"),
        "valor_anterior":            pd.to_numeric(_col("ultimo_preco_db", None), errors="coerce"),
        "Valor do Frete":            pd.to_numeric(_col("VALOR_FRETE", None), errors="coerce"),
        "Frete anterior":            pd.to_numeric(_col("vl_frete_anterior", None), errors="coerce"),
        "Motivo":                    _col("motivo_manutencao", "").astype(str),
        "Justificativa Livre":       _col("DS_OBS_PRECCOL", "").fillna("").astype(str),
        "URL do insumo":             url_final,
    })

    editable_cols = {"Justificativa Livre", "URL do insumo"}
    edited = st.data_editor(
        view,
        column_config={
            "Data do Preço":       st.column_config.DateColumn(format="DD/MM/YYYY", width="small"),
            "Data Prevista":       st.column_config.DateColumn(format="DD/MM/YYYY", width="small"),
            "Valor do Preço":      st.column_config.NumberColumn(format="R$ %.2f", width="small"),
            "valor_anterior":      st.column_config.NumberColumn(format="R$ %.2f", width="small"),
            "Valor do Frete":      st.column_config.NumberColumn(format="R$ %.2f", width="small"),
            "Frete anterior":      st.column_config.NumberColumn(format="R$ %.2f", width="small"),
            "Justificativa Livre": st.column_config.TextColumn(width="large"),
            "URL do insumo":       st.column_config.TextColumn(width="large"),
        },
        disabled=[c for c in view.columns if c not in editable_cols],
        hide_index=True,
        use_container_width=True,
        key=f"critica_repro_editor_{file_base}",
        height=36 + n * 35,
    )

    df_final = df.copy()
    df_final["DS_OBS_PRECCOL"] = edited["Justificativa Livre"].astype(str).values
    df_final["URL_DO_INSUMO"]  = edited["URL do insumo"].astype(str).values

    try:
        xls_bytes = export_carga_reprovada(df_final)
    except Exception as e:
        st.error(f"Falha ao gerar arquivo reprovado: {e}")
        return

    st.download_button(
        label=f"Baixar Carga Reprovada ({len(df_final)})",
        data=xls_bytes,
        file_name=f"carga_BP_reprovada_{file_base}.xls",
        mime="application/vnd.ms-excel",
        type="primary",
        key="critica_dl_repro",
        use_container_width=True,
    )


def _render_critica_informativa(df: pd.DataFrame) -> None:
    view_cols = [c for c in (["JOB"] + _CRITICA_INFO_COLS + ["URL_DO_INSUMO"])
                 if c in df.columns]
    st.dataframe(
        df[view_cols],
        column_config={
            "JOB":           st.column_config.TextColumn("Job", width="small"),
            "DATA_PREVISTA": st.column_config.DateColumn("Data Prevista", format="DD/MM/YYYY"),
            "URL_DO_INSUMO": st.column_config.LinkColumn("URL Encomenda"),
        },
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"{len(df)} insumo(s)")


_BUCKET_KEYS = ("aprovados", "reprovados", "falta", "pendente", "nao_cadastrados")


def _apply_critica_filters(
    df: pd.DataFrame, jobs: list, infs: list, seqs: list,
) -> pd.DataFrame:
    if df.empty or not (jobs or infs or seqs):
        return df
    mask = pd.Series(True, index=df.index)
    if jobs and "JOB" in df.columns:
        mask &= df["JOB"].astype(str).isin([str(x) for x in jobs])
    if infs and "CD_INFORM" in df.columns:
        mask &= df["CD_INFORM"].astype(str).isin([str(x) for x in infs])
    if seqs and "NR_SEQ_INSINF" in df.columns:
        mask &= df["NR_SEQ_INSINF"].astype(str).isin([str(x) for x in seqs])
    return df[mask].reset_index(drop=True)


def _collect_filter_options(result: dict) -> dict:
    jobs, infs, seqs = set(), set(), set()
    for k in _BUCKET_KEYS:
        df = result.get(k)
        if df is None or df.empty:
            continue
        if "JOB" in df.columns:
            jobs.update(df["JOB"].dropna().astype(str))
        if "CD_INFORM" in df.columns:
            infs.update(df["CD_INFORM"].dropna().astype(str))
        if "NR_SEQ_INSINF" in df.columns:
            seqs.update(df["NR_SEQ_INSINF"].dropna().astype(str))
    return {
        "JOB":           sorted(j for j in jobs if j and j.lower() != "nan"),
        "CD_INFORM":     sorted(i for i in infs if i and i.lower() != "nan"),
        "NR_SEQ_INSINF": sorted(seqs, key=lambda x: (len(x), x)),
    }


def _carga_page() -> None:
    st.markdown("#### Crítica de Coleta — Decêndio Corrente")

    try:
        with st.spinner("Carregando encomenda..."):
            df_enc, fonte_enc, sf_err = _load_encomenda_cached()
    except Exception as e:
        st.error(f"Falha ao ler a encomenda: {e}")
        return
    if sf_err:
        st.warning(f"Snowflake indisponível — usando fallback local. Erro: `{sf_err}`")

    data_prev = _data_prevista_corrente()
    hoje = date.today()
    dias_restantes = (data_prev - hoje).days
    if dias_restantes > 0:
        prazo_txt = f"faltam {dias_restantes} dia(s) até o prazo"
    elif dias_restantes == 0:
        prazo_txt = "hoje é o último dia"
    else:
        prazo_txt = f"prazo expirado há {-dias_restantes} dia(s)"

    st.caption(
        f"Fonte da encomenda: `{fonte_enc}` — {len(df_enc):,} linha(s) · "
        f"Data prevista: **{data_prev:%d/%m/%Y}** ({prazo_txt})."
    )

    try:
        with st.spinner("Rodando crítica para todos os informantes do decêndio..."):
            result = _compute_critica_geral(data_prev.isoformat(), hoje.isoformat())
    except Exception as e:
        st.error(f"Falha ao rodar crítica: {e}")
        return

    if result["n_total"] == 0:
        st.info(f"Nenhum insumo na encomenda com DATA_PREVISTA = {data_prev:%d/%m/%Y}.")
        return

    opts = _collect_filter_options(result)
    with st.expander("Filtros", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            f_job = st.multiselect("JOB", options=opts["JOB"], key="critica_f_job")
        with c2:
            f_inf = st.multiselect("Cód. Informante", options=opts["CD_INFORM"], key="critica_f_inf")
        with c3:
            f_seq = st.multiselect(
                "NR_SEQ_INSINF", options=opts["NR_SEQ_INSINF"], key="critica_f_seq",
                help="Selecione um ou mais NR_SEQ_INSINF para restringir as listas.",
            )

    filtered = {
        k: _apply_critica_filters(result[k], f_job, f_inf, f_seq)
        for k in _BUCKET_KEYS
    }
    filt_active = bool(f_job or f_inf or f_seq)

    n_aprov   = len(filtered["aprovados"])
    n_repro   = len(filtered["reprovados"])
    n_falta   = len(filtered["falta"])
    n_pend    = len(filtered["pendente"])
    n_naocad  = len(filtered["nao_cadastrados"])
    n_visivel = n_aprov + n_repro + n_falta + n_pend + n_naocad

    total_label = "Visíveis" if filt_active else "Total Decêndio"
    st.markdown(
        _metric_cards([
            (total_label,        n_visivel if filt_active else result["n_total"]),
            ("Aprovados",        n_aprov),
            ("Reprovados",       n_repro),
            ("Falta",            n_falta),
            ("Pendentes",        n_pend),
            ("Não cadastrados",  n_naocad),
        ]),
        unsafe_allow_html=True,
    )

    n_fora_bp = result.get("n_informante_fora_bp", 0)
    if n_fora_bp:
        st.caption(
            f"{n_fora_bp} insumo(s) ignorado(s): CD_INFORM sem qualquer cadastro em "
            "`tbl_ecommerce_registered_ins_inform_prod`."
        )

    if result["critica_error"]:
        st.warning(
            "Snowflake não respondeu — itens coletados foram marcados como "
            f"reprovados. Erro: `{result['critica_error']}`"
        )

    file_base = f"todos_{data_prev:%d_%m_%Y}"
    tab_aprov, tab_repro, tab_falta, tab_pend, tab_naocad = st.tabs([
        f"Aprovados ({n_aprov})",
        f"Reprovados ({n_repro})",
        f"Falta ({n_falta})",
        f"Pendentes ({n_pend})",
        f"Não cadastrados ({n_naocad})",
    ])

    with tab_aprov:
        _render_critica_aprovados(filtered["aprovados"], file_base)
    with tab_repro:
        _render_critica_reprovados(filtered["reprovados"], file_base)
    with tab_falta:
        if filtered["falta"].empty:
            st.success(
                "Nenhum insumo em falta — todos os com prazo expirado foram coletados."
            )
        else:
            _render_critica_informativa(filtered["falta"])
    with tab_pend:
        if filtered["pendente"].empty:
            st.info(
                "Nenhum insumo pendente — todos foram coletados ou já estão em falta."
            )
        else:
            _render_critica_informativa(filtered["pendente"])
    with tab_naocad:
        if filtered["nao_cadastrados"].empty:
            st.success(
                "Nenhum insumo não cadastrado — todos os pares (informante, insumo) "
                "deste decêndio estão em `tbl_ecommerce_registered_ins_inform_prod`."
            )
        else:
            st.caption(
                "Insumos informados na encomenda cujo par "
                "(CD_INFORM, NR_SEQ_INSINF) não está em "
                "`tbl_ecommerce_registered_ins_inform_prod`, mas o CD_INFORM tem "
                "outros cadastros."
            )
            _render_critica_informativa(filtered["nao_cadastrados"])


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _get_filter_options_cached() -> dict:
    return get_filter_options()


@st.cache_data(ttl=300)
def _get_monitoramento_cached() -> pd.DataFrame:
    return get_monitoramento_data()


@st.cache_data(ttl=300, show_spinner=False)
def _get_informantes_coletaram_cached(date_ini_iso: str, date_fim_iso: str) -> set[str]:
    return get_informantes_coletaram(
        date.fromisoformat(date_ini_iso),
        date.fromisoformat(date_fim_iso),
    )


@st.cache_data(ttl=300, show_spinner=False)
def _find_coletados_by_urls_cached(cod_informante: str, urls: tuple[str, ...]) -> pd.DataFrame:
    return find_coletados_by_urls(cod_informante, list(urls))


@st.cache_data(ttl=300, show_spinner=False)
def _get_existing_cadastros_cached(cod_informante: str) -> set[tuple[str, str]]:
    return get_existing_cadastros(cod_informante)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

ROLE_TABS: dict[str, list[str]] = {
    "ADMIN":     ["Consulta", "Monitoramento", "Carga"],
    "DEV":       ["Consulta", "Monitoramento"],
    "VALIDADOR": ["Carga"],
    "ANALISTA":  ["Consulta"],
}

TAB_RENDERERS = {
    "Consulta":      _data_page,
    "Monitoramento": _monitoramento_page,
    "Carga":         _carga_page,
}


def main() -> None:
    _init_state()

    if not st.session_state.user:
        _login_page()
        return

    _render_sidebar_authenticated()
    _render_banner()

    role = st.session_state.user.get("role", "ANALISTA")
    allowed = ROLE_TABS.get(role, ["Consulta"])

    if not allowed:
        st.warning("Seu perfil não tem acesso a nenhuma aba.")
        return

    tabs = st.tabs(allowed)
    for tab, name in zip(tabs, allowed):
        with tab:
            TAB_RENDERERS[name]()


if __name__ == "__main__":
    main()
