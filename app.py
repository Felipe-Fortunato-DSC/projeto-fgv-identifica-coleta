"""
FGV Identifica — Consulta de Coleta de Produtos
Aplicação Streamlit para consulta, filtro e exportação de dados de scraping.
"""

import hashlib
import math
import os
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

from auth import USER_NAME, verify_login
from database import (
    get_carga_data,
    get_data_by_ids,
    get_filter_options,
    get_monitoramento_data,
    get_page_data_with_count,
    save_cadastrado_bp,
)
from export import export_to_excel

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
    .main-header {
        background: linear-gradient(90deg, #1B4F72 0%, #2980B9 100%);
        padding: 0.45rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    .main-header h1 { color: #fff; margin: 0; font-size: 1.2rem; }
    .main-header p  { color: #D6EAF8; margin: 0; font-size: .78rem; }

    div[data-testid="metric-container"] {
        background: #F0F3F4;
        border-left: 4px solid #2980B9;
        border-radius: 6px;
        padding: .5rem 1rem;
    }

    div[data-testid="stButton"] > button[kind="primary"] {
        background: #1B4F72;
        color: #fff;
        border: none;
    }
    div[data-testid="stButton"] > button[kind="primary"]:hover {
        background: #2980B9;
    }

    div[data-testid="stDataEditor"] { border-radius: 6px; }

    section[data-testid="stSidebar"] { background: #000000; }
    section[data-testid="stSidebar"] * { color: #ffffff !important; }
    section[data-testid="stSidebar"] hr { border-color: #444 !important; }
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
        "selected_ids": set(),
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
    _, col, _ = st.columns([1, 2, 1])
    with col:
        logo_path = Path(__file__).parent / "fgv_ibre.png"
        if logo_path.exists():
            _, logo_col, _ = st.columns([1, 2, 1])
            with logo_col:
                st.image(str(logo_path), use_container_width=True)
        st.markdown(
            """
            <div style="text-align:center; margin: 0.75rem 0 1.25rem 0;">
                <p style="color:#ffffff; font-size:1rem; font-weight:600; margin:0;">
                    Sistema de Consulta de Coleta de Preços
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.form("login_form"):
            st.subheader("Acesso ao Sistema")
            nome = st.text_input("Usuário", value=USER_NAME)
            senha = st.text_input("Senha", type="password", placeholder="Senha")
            entrar = st.form_submit_button("Entrar", use_container_width=True, type="primary")

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

def _render_sidebar_authenticated() -> None:
    user = st.session_state.user

    with st.sidebar:
        logo_path = Path(__file__).parent / "fgv_ibre.png"
        if logo_path.exists():
            _, logo_col, _ = st.columns([0.3, 3, 0.3])
            with logo_col:
                st.image(str(logo_path), use_container_width=True)
        st.markdown(f"### Bem-vindo, {user['nome']}!")
        st.divider()

        if st.button("🚪 Sair", use_container_width=True):
            for key in ("user", "selected_ids", "page", "table_version", "filter_suffix"):
                st.session_state.pop(key, None)
            st.rerun()

        st.markdown(
            """
            <div style="position:fixed; bottom:1rem; font-size:0.72rem; color:#aaa;">
                Desenvolvido por <strong>Felipe Fortunato</strong><br>FGV IBRE
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Filtros — área principal
# ---------------------------------------------------------------------------

def _render_filters(options: dict) -> dict:
    sfx = st.session_state.filter_suffix

    st.session_state["_filter_options_ref"] = options

    def _reset_state():
        st.session_state.selected_ids = set()
        st.session_state.pop("last_table_edited", None)
        st.session_state.pop("last_table_original", None)
        st.session_state.filter_cadastrados_bp = False
        st.session_state["toggle_cadastrados_bp"] = False
        st.session_state.page = 1
        st.session_state.page_cache_key = None

    def _on_cod_change():
        opts = st.session_state.get("_filter_options_ref", {})
        _sfx = st.session_state.filter_suffix
        cod = st.session_state.get(f"f_cod_{_sfx}", "")
        nome = opts.get("cod_to_nome", {}).get(cod, "")
        st.session_state[f"f_nome_{_sfx}"] = nome
        st.session_state[f"f_cod_insumo_{_sfx}"] = ""
        st.session_state[f"f_insumo_inf_{_sfx}"] = ""
        if cod:
            latest = opts.get("ultima_coleta_by_informante", {}).get(cod)
            if latest:
                st.session_state[f"f_data_{_sfx}"] = date.fromisoformat(latest)
        _reset_state()

    def _on_nome_change():
        opts = st.session_state.get("_filter_options_ref", {})
        _sfx = st.session_state.filter_suffix
        nome = st.session_state.get(f"f_nome_{_sfx}", "")
        cod = opts.get("nome_to_cod", {}).get(nome, "")
        st.session_state[f"f_cod_{_sfx}"] = cod
        st.session_state[f"f_cod_insumo_{_sfx}"] = ""
        st.session_state[f"f_insumo_inf_{_sfx}"] = ""
        if cod:
            latest = opts.get("ultima_coleta_by_informante", {}).get(cod)
            if latest:
                st.session_state[f"f_data_{_sfx}"] = date.fromisoformat(latest)
        _reset_state()

    cod_inf = nome_inf = marca = ean_sku = busca_texto = tipo_preco = uf = ""
    cod_insumo_f = insumo_informado_f = ""
    data_coleta = None

    with st.expander("🔍 Filtros", expanded=False):
        th1, th2 = st.columns([9, 1])
        with th2:
            if st.button("Limpar Filtro", key=f"limpar_{sfx}", use_container_width=True):
                st.session_state.filter_suffix += 1
                st.session_state.page = 1
                st.session_state.table_version += 1
                _reset_state()
                st.rerun()

        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])

        with col1:
            cod_inf = st.selectbox(
                "Cód. Informante",
                options=[""] + options["cod_informante"],
                index=0,
                key=f"f_cod_{sfx}",
                on_change=_on_cod_change,
            )
            ean_sku = st.text_input(
                "EAN / SKU",
                placeholder="Digite o código exato...",
                key=f"f_ean_{sfx}",
            )
            _cod_insumo_opts = (
                options.get("cod_insumo_by_informante", {}).get(cod_inf, options.get("cod_insumo", []))
                if cod_inf else options.get("cod_insumo", [])
            )
            cod_insumo_f = st.selectbox(
                "Cód. Insumo",
                options=[""] + _cod_insumo_opts,
                key=f"f_cod_insumo_{sfx}",
            )

        with col2:
            nome_inf = st.selectbox(
                "Nome Informante",
                options=[""] + options["nome_informante"],
                key=f"f_nome_{sfx}",
                on_change=_on_nome_change,
            )
            busca_texto = st.text_input(
                "Descrição",
                placeholder="Ex: sabão pó nestlé...",
                key=f"f_texto_{sfx}",
            )
            _insumo_inf_opts = (
                options.get("insumo_informado_by_informante", {}).get(cod_inf, options.get("insumo_informado", []))
                if cod_inf else options.get("insumo_informado", [])
            )
            insumo_informado_f = st.selectbox(
                "Insumo Informado",
                options=[""] + _insumo_inf_opts,
                key=f"f_insumo_inf_{sfx}",
            )

        with col3:
            marca = st.selectbox(
                "Marca",
                options=[""] + options["marca"],
                key=f"f_marca_{sfx}",
            )
            tipo_preco = st.selectbox(
                "Tipo de Preço",
                options=[""] + options["tipo_preco"],
                key=f"f_tipo_{sfx}",
            )

        with col4:
            uf = st.selectbox(
                "UF",
                options=[""] + options["uf"],
                key=f"f_uf_{sfx}",
            )
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
    if nome_inf:
        filters["nome_informante"] = nome_inf
    if marca:
        filters["marca"] = marca
    if tipo_preco:
        filters["tipo_preco"] = tipo_preco
    if uf:
        filters["uf"] = uf
    if ean_sku.strip():
        filters["ean_sku"] = ean_sku.strip()
    if busca_texto.strip():
        filters["busca_texto"] = busca_texto.strip()
    if cod_insumo_f:
        filters["cod_insumo"] = cod_insumo_f
    if insumo_informado_f:
        filters["insumo_informado"] = insumo_informado_f
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
            <p>Consulta, filtragem e exportação de dados de scraping de produtos</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_metrics(placeholder, total: int, n_insumos: int) -> None:
    def _val(n):
        color = "#ffffff" if n > 0 else "#888888"
        return f"<span style='color:{color};'>{str(n).replace(',', '.')}</span>"

    with placeholder:
        st.markdown(
            f"""
            <div style="display:flex; gap:2.5rem; margin:0.4rem 0 0.6rem 0; flex-wrap:wrap;">
                <span style="font-size:1.05rem; color:#ffffff;">
                    <strong>Produtos Coletados:</strong> {_val(total)}
                </span>
                <span style="font-size:1.05rem; color:#ffffff;">
                    <strong>Insumos Cadastrados:</strong> {_val(n_insumos)}
                </span>
            </div>
            """,
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
    "Sel": st.column_config.CheckboxColumn("✓", width="small", pinned=True),
    "data_coleta": st.column_config.DateColumn(
        "Data Coleta", format="DD/MM/YYYY", width="small"
    ),
    "plataforma": st.column_config.TextColumn("Plataforma", width="small"),
    "cod_informante": st.column_config.TextColumn("Cód. Informante", width="small"),
    "nome_informante": st.column_config.TextColumn("Nome Informante", width="medium"),
    "periodicidade": st.column_config.TextColumn("Periodicidade", width="small"),
    "tipo_preco": st.column_config.TextColumn("Tipo Preço", width="small"),
    "cod_insumo": st.column_config.TextColumn("Cód. Insumo", width="small"),
    "ean": st.column_config.TextColumn("EAN", width="medium"),
    "sku": st.column_config.TextColumn("SKU", width="medium"),
    "insumo_informado": st.column_config.TextColumn("Insumo Informado", width="medium"),
    "url": st.column_config.LinkColumn("URL", width="medium"),
    "descricao": st.column_config.TextColumn("Descrição", width="large"),
    "marca": st.column_config.TextColumn("Marca", width="small"),
    "uf": st.column_config.TextColumn("UF", width="small"),
    "moeda": st.column_config.TextColumn("Moeda", width="small"),
    "preco": st.column_config.NumberColumn("Preço", format="R$ %.2f", width="small"),
    "preco_promocional": st.column_config.NumberColumn(
        "Preço Promo", format="R$ %.2f", width="small"
    ),
    "id_produto": st.column_config.TextColumn("ID Produto", width="small"),
    "id_coleta": st.column_config.TextColumn("ID Coleta", width="small"),
    "id_imagem": st.column_config.TextColumn("ID Imagem", width="small"),
}

DISABLED_COLS = [
    "data_coleta", "plataforma", "cod_informante", "nome_informante",
    "periodicidade", "tipo_preco", "ean", "sku",
    "url", "descricao", "marca", "uf", "moeda",
    "preco", "preco_promocional", "id_produto", "id_coleta", "id_imagem",
]


def _table_key(page: int, version: int) -> str:
    return f"tbl_p{page}_v{version}"


def _render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Nenhum produto encontrado com os filtros aplicados.")
        return

    display = df.copy()
    display.insert(0, "Sel", display["id_produto"].isin(st.session_state.selected_ids))

    edited = st.data_editor(
        display,
        column_config=COLUMN_CONFIG,
        disabled=DISABLED_COLS,
        hide_index=True,
        use_container_width=True,
        key=_table_key(st.session_state.page, st.session_state.table_version),
        height=36 + len(display) * 35,
    )

    # Armazena estado atual para uso no botão de salvar
    st.session_state["last_table_edited"] = edited
    st.session_state["last_table_original"] = df

    # Trata alterações de seleção
    page_ids = set(df["id_produto"])
    sel_on_page = set(edited.loc[edited["Sel"], "id_produto"])
    desel_on_page = page_ids - sel_on_page
    new_selected = (st.session_state.selected_ids - desel_on_page) | sel_on_page

    if new_selected != st.session_state.selected_ids:
        st.session_state.selected_ids = new_selected
        st.session_state.table_version += 1
        st.rerun()


# ---------------------------------------------------------------------------
# Paginação
# ---------------------------------------------------------------------------

def _render_pagination(total: int) -> None:
    n_pages = max(1, math.ceil(total / PAGE_SIZE))
    if n_pages == 1:
        return

    col_prev, col_info, col_next = st.columns([1, 2, 1])

    with col_prev:
        if st.button("◀ Anterior", disabled=st.session_state.page <= 1, use_container_width=True):
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
        if st.button("Próxima ▶", disabled=st.session_state.page >= n_pages, use_container_width=True):
            st.session_state.page += 1
            st.rerun()


# ---------------------------------------------------------------------------
# Exportação
# ---------------------------------------------------------------------------

def _render_export() -> None:
    n_sel = len(st.session_state.selected_ids)
    if n_sel == 0:
        return

    c1, _ = st.columns([3, 9])
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
                _get_carga_cached.clear()
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
    _render_export()
    _render_cadastrados_bp_toggle()
    _render_table(page_df)
    _render_save_button()
    _render_pagination(total)


# ---------------------------------------------------------------------------
# Aba Monitoramento
# ---------------------------------------------------------------------------

def _monitoramento_page() -> None:
    st.markdown("#### Monitoramento de Informantes")
    metrics_placeholder = st.empty()

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

    sfx = st.session_state.get("mon_filter_suffix", 0)

    with st.expander("🔍 Filtros", expanded=False):
        th1, th2 = st.columns([9, 1])
        with th2:
            if st.button("Limpar Filtro", key=f"mon_limpar_{sfx}", use_container_width=True):
                st.session_state.mon_filter_suffix = sfx + 1
                st.rerun()

        col1, col2 = st.columns(2)
        with col1:
            cod_options = sorted(df["cod_informante"].dropna().unique().tolist())
            cod_inf = st.selectbox(
                "Cód. Informante",
                options=[""] + cod_options,
                key=f"mon_f_cod_{sfx}",
            )
        with col2:
            frete_options = sorted(df["frete"].dropna().unique().tolist())
            frete = st.selectbox(
                "Frete",
                options=[""] + frete_options,
                key=f"mon_f_frete_{sfx}",
            )

    filtered = df
    if cod_inf:
        filtered = filtered[filtered["cod_informante"] == cod_inf]
    if frete:
        filtered = filtered[filtered["frete"] == frete]
    if st.session_state.get("mon_only_ativos", False):
        filtered = filtered[filtered["status"] == "Ativo"]

    total = len(df)
    ativos = int((df["status"] == "Ativo").sum())
    inativos = int((df["status"] == "Inativo").sum())

    def _val(n: int) -> str:
        color = "#ffffff" if n > 0 else "#888888"
        return f"<span style='color:{color};'>{str(n).replace(',', '.')}</span>"

    with metrics_placeholder:
        st.markdown(
            f"""
            <div style="display:flex; gap:2.5rem; margin:0.4rem 0 0.6rem 0; flex-wrap:wrap;">
                <span style="font-size:1.05rem; color:#ffffff;">
                    <strong>Qtd. de Informantes:</strong> {_val(total)}
                </span>
                <span style="font-size:1.05rem; color:#ffffff;">
                    <strong>Informantes Ativos:</strong> {_val(ativos)}
                </span>
                <span style="font-size:1.05rem; color:#ffffff;">
                    <strong>Informantes Inativos:</strong> {_val(inativos)}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.toggle(
        "Informantes Ativos",
        key="mon_only_ativos",
        help="Filtrar apenas informantes com status Ativo",
    )

    col_cfg = {
        "cod_informante": st.column_config.TextColumn("Cód. Informante"),
        "dominio":        st.column_config.TextColumn("Domínio"),
        "frete":          st.column_config.TextColumn("Frete"),
        "status":         st.column_config.TextColumn("Status"),
        "tipo_preco":     st.column_config.TextColumn("Tipo de Preço"),
        "ultima_coleta":  st.column_config.TextColumn("Última Coleta"),
        "execucao":       st.column_config.TextColumn("Execução"),
    }
    st.dataframe(filtered, use_container_width=True, column_config=col_cfg, hide_index=True)
    st.caption(f"{len(filtered)} informante(s) listado(s)")


# ---------------------------------------------------------------------------
# Aba Carga
# ---------------------------------------------------------------------------

def _carga_page() -> None:
    st.markdown("#### Insumos Cadastrados BP")
    try:
        with st.spinner("Carregando insumos cadastrados..."):
            df = _get_carga_cached()
    except Exception as e:
        st.error(f"Erro ao consultar Athena: {e}")
        return

    if df.empty:
        st.info("Nenhum insumo cadastrado com código e descrição preenchidos.")
        return

    col_cfg = {
        "cod_informante":  st.column_config.TextColumn("Cód. Informante"),
        "cod_insumo":      st.column_config.TextColumn("Cód. Insumo"),
        "insumo_informado": st.column_config.TextColumn("Insumo Informado"),
        "id_produto_site": st.column_config.TextColumn("ID Produto Site"),
    }
    st.dataframe(df, use_container_width=True, column_config=col_cfg, hide_index=True)
    st.caption(f"{len(df)} insumo(s) cadastrado(s)")


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _get_filter_options_cached() -> dict:
    return get_filter_options()


@st.cache_data(ttl=300)
def _get_monitoramento_cached() -> pd.DataFrame:
    return get_monitoramento_data()


@st.cache_data(ttl=300)
def _get_carga_cached() -> pd.DataFrame:
    return get_carga_data()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _init_state()

    if not st.session_state.user:
        _login_page()
        return

    _render_sidebar_authenticated()
    _render_banner()

    tab1, tab2, tab3 = st.tabs(["Consulta de Preços", "Monitoramento", "Carga"])
    with tab1:
        _data_page()
    with tab2:
        _monitoramento_page()
    with tab3:
        _carga_page()


if __name__ == "__main__":
    main()
