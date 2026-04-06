"""
FGV Identifica — Consulta de Coleta de Produtos
Aplicação Streamlit para consulta, filtro e exportação de dados de scraping.
"""

import hashlib
import math
from datetime import date

import pandas as pd
import streamlit as st

from database import (
    get_all_filtered_ids,
    get_data_by_ids,
    get_filter_options,
    get_page_data,
    get_total_count,
)
from export import export_to_excel

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="FGV Identifica — Coleta",
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
    /* Cabeçalho principal */
    .main-header {
        background: linear-gradient(90deg, #1B4F72 0%, #2980B9 100%);
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    .main-header h1 { color: #fff; margin: 0; font-size: 1.6rem; }
    .main-header p  { color: #D6EAF8; margin: 0; font-size: .85rem; }

    /* Cards de métricas */
    div[data-testid="metric-container"] {
        background: #F0F3F4;
        border-left: 4px solid #2980B9;
        border-radius: 6px;
        padding: .5rem 1rem;
    }

    /* Botão primário */
    div[data-testid="stButton"] > button[kind="primary"] {
        background: #1B4F72;
        color: #fff;
        border: none;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background: #2980B9;
    }

    /* Reduz padding lateral da tabela */
    div[data-testid="stDataEditor"] { border-radius: 6px; }

    /* Sidebar */
    section[data-testid="stSidebar"] { background: #EAF2FB; }
    </style>
    """,
    unsafe_allow_html=True,
)

PAGE_SIZE = 20

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def _init_state() -> None:
    defaults = {
        "selected_ids": set(),
        "page": 1,
        "table_version": 0,
        "filter_suffix": 0,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


# ---------------------------------------------------------------------------
# Sidebar — filtros
# ---------------------------------------------------------------------------

def _render_sidebar(options: dict) -> dict:
    with st.sidebar:
        st.markdown("## 🔍 Filtros")

        sfx = st.session_state.filter_suffix  # sufixo para reset de widgets

        # Botão limpar filtros
        if st.button("🗑️ Limpar Filtros", use_container_width=True):
            st.session_state.filter_suffix += 1
            st.session_state.page = 1
            st.session_state.table_version += 1
            st.rerun()

        st.divider()

        cod_inf = st.selectbox(
            "Cód. Informante",
            options=[""] + options["cod_informante"],
            key=f"f_cod_{sfx}",
        )
        nome_inf = st.selectbox(
            "Nome Informante",
            options=[""] + options["nome_informante"],
            key=f"f_nome_{sfx}",
        )
        marca = st.selectbox(
            "Marca",
            options=[""] + options["marca"],
            key=f"f_marca_{sfx}",
        )

        st.markdown("**Período de Coleta**")
        data_min = date.fromisoformat(options["data_min"])
        data_max = date.fromisoformat(options["data_max"])
        periodo = st.date_input(
            "Período",
            value=(data_min, data_max),
            min_value=data_min,
            max_value=data_max,
            key=f"f_periodo_{sfx}",
            label_visibility="collapsed",
        )

        st.divider()
        ean_sku = st.text_input(
            "Busca por EAN / SKU",
            placeholder="Digite o código exato...",
            key=f"f_ean_{sfx}",
        )
        busca_texto = st.text_input(
            "Busca por Descrição",
            placeholder="Ex: sabão pó nestlé...",
            key=f"f_texto_{sfx}",
        )

    # Monta dict de filtros
    filters: dict = {}
    if cod_inf:
        filters["cod_informante"] = cod_inf
    if nome_inf:
        filters["nome_informante"] = nome_inf
    if marca:
        filters["marca"] = marca
    if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
        filters["data_inicio"] = periodo[0]
        filters["data_fim"] = periodo[1]
    if ean_sku.strip():
        filters["ean_sku"] = ean_sku.strip()
    if busca_texto.strip():
        filters["busca_texto"] = busca_texto.strip()

    return filters


# ---------------------------------------------------------------------------
# Cabeçalho e métricas
# ---------------------------------------------------------------------------

def _render_header(total: int) -> None:
    st.markdown(
        """
        <div class="main-header">
            <h1>🛍️ FGV Identifica — Consulta de Coleta</h1>
            <p>Consulta, filtragem e exportação de dados de scraping de produtos</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Resultados encontrados", f"{total:,}".replace(",", "."))
    c2.metric("Produtos selecionados", len(st.session_state.selected_ids))
    n_pages = max(1, math.ceil(total / PAGE_SIZE))
    c3.metric("Página", f"{st.session_state.page} / {n_pages}")


# ---------------------------------------------------------------------------
# Controles de seleção
# ---------------------------------------------------------------------------

def _render_selection_controls(filters: dict) -> None:
    c1, c2, c3 = st.columns([2, 2, 8])

    with c1:
        if st.button("☑️ Selecionar Todos", use_container_width=True):
            with st.spinner("Selecionando..."):
                ids = get_all_filtered_ids(filters)
            st.session_state.selected_ids.update(ids)
            st.session_state.table_version += 1
            st.rerun()

    with c2:
        if st.button("☐ Desmarcar Todos", use_container_width=True):
            st.session_state.selected_ids.clear()
            st.session_state.table_version += 1
            st.rerun()


# ---------------------------------------------------------------------------
# Tabela interativa
# ---------------------------------------------------------------------------

COLUMN_CONFIG = {
    "Sel": st.column_config.CheckboxColumn("✓", width="small"),
    "data_coleta": st.column_config.DateColumn(
        "Data Coleta", format="DD/MM/YYYY", width="small"
    ),
    "cod_informante": st.column_config.TextColumn("Cód. Informante", width="small"),
    "nome_informante": st.column_config.TextColumn("Nome Informante", width="medium"),
    "ean": st.column_config.TextColumn("EAN", width="medium"),
    "sku": st.column_config.TextColumn("SKU", width="medium"),
    "url": st.column_config.LinkColumn("URL", width="medium"),
    "descricao": st.column_config.TextColumn("Descrição", width="large"),
    "marca": st.column_config.TextColumn("Marca", width="small"),
    "preco": st.column_config.NumberColumn("Preço", format="R$ %.2f", width="small"),
    "preco_promocional": st.column_config.NumberColumn(
        "Preço Promo", format="R$ %.2f", width="small"
    ),
    "id_produto": st.column_config.TextColumn("ID Produto", width="small"),
}

DISABLED_COLS = [
    "data_coleta", "cod_informante", "nome_informante",
    "ean", "sku", "url", "descricao", "marca",
    "preco", "preco_promocional", "id_produto",
]


def _table_key(filters: dict, page: int, version: int) -> str:
    h = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()[:8]
    return f"tbl_p{page}_{h}_v{version}"


def _render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Nenhum produto encontrado com os filtros aplicados.")
        return

    display = df.copy()
    display.insert(
        0, "Sel",
        display["id_produto"].isin(st.session_state.selected_ids)
    )

    key = _table_key(
        {},  # filtros já baked no df; versão/página controlam o reset
        st.session_state.page,
        st.session_state.table_version,
    )

    edited = st.data_editor(
        display,
        column_config=COLUMN_CONFIG,
        disabled=DISABLED_COLS,
        hide_index=True,
        use_container_width=True,
        key=key,
        height=min(36 + len(display) * 35, 36 + PAGE_SIZE * 35),
    )

    # Sincroniza seleção com session_state
    page_ids = set(df["id_produto"])
    sel_on_page = set(edited.loc[edited["Sel"], "id_produto"])
    desel_on_page = page_ids - sel_on_page

    st.session_state.selected_ids = (
        (st.session_state.selected_ids - desel_on_page) | sel_on_page
    )


# ---------------------------------------------------------------------------
# Paginação
# ---------------------------------------------------------------------------

def _render_pagination(total: int) -> None:
    n_pages = max(1, math.ceil(total / PAGE_SIZE))
    if n_pages == 1:
        return

    st.divider()
    cols = st.columns([1, 4, 1])

    with cols[0]:
        if st.button("◀ Anterior", disabled=st.session_state.page <= 1):
            st.session_state.page -= 1
            st.rerun()

    with cols[1]:
        # Slider de página
        new_page = st.slider(
            "Página",
            min_value=1,
            max_value=n_pages,
            value=st.session_state.page,
            key="pagination_slider",
            label_visibility="collapsed",
        )
        if new_page != st.session_state.page:
            st.session_state.page = new_page
            st.rerun()

    with cols[2]:
        if st.button("Próxima ▶", disabled=st.session_state.page >= n_pages):
            st.session_state.page += 1
            st.rerun()


# ---------------------------------------------------------------------------
# Exportação
# ---------------------------------------------------------------------------

def _render_export() -> None:
    n_sel = len(st.session_state.selected_ids)
    if n_sel == 0:
        st.info("Selecione produtos para habilitar a exportação.")
        return

    st.divider()
    c1, c2 = st.columns([3, 9])
    with c1:
        with st.spinner("Preparando exportação..."):
            export_df = get_data_by_ids(list(st.session_state.selected_ids))
            xlsx_bytes = export_to_excel(export_df)

        st.download_button(
            label=f"📥 Exportar {n_sel} produto(s) (.xlsx)",
            data=xlsx_bytes,
            file_name="coleta_produtos_selecionados.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary",
        )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _init_state()

    # Verifica se o banco existe
    from pathlib import Path
    db_path = Path(__file__).parent / "data" / "database.db"
    if not db_path.exists():
        st.error(
            "Banco de dados não encontrado. "
            "Execute `python generate_data.py` para criar os dados de teste."
        )
        st.stop()

    # Carrega opções de filtro (cacheado)
    options = _get_filter_options_cached()

    # Sidebar
    filters = _render_sidebar(options)

    # Quando filtros mudam, volta para pág. 1
    filters_hash = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()
    if "last_filters_hash" not in st.session_state:
        st.session_state.last_filters_hash = filters_hash
    elif st.session_state.last_filters_hash != filters_hash:
        st.session_state.page = 1
        st.session_state.last_filters_hash = filters_hash

    # Carrega dados
    with st.spinner("Carregando dados..."):
        total = get_total_count(filters)
        page_df = get_page_data(filters, st.session_state.page, PAGE_SIZE)

    # Cabeçalho
    _render_header(total)

    # Controles de seleção
    _render_selection_controls(filters)

    st.markdown("---")

    # Tabela
    _render_table(page_df)

    # Paginação
    _render_pagination(total)

    # Exportação
    _render_export()


@st.cache_data(ttl=300)
def _get_filter_options_cached() -> dict:
    return get_filter_options()


if __name__ == "__main__":
    main()
