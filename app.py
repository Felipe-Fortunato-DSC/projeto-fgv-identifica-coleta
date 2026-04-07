"""
FGV Identifica — Consulta de Coleta de Produtos
Aplicação Streamlit para consulta, filtro e exportação de dados de scraping.
"""

import hashlib
import math
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from auth import (
    change_password,
    create_user,
    get_all_users,
    init_auth,
    remove_user,
    verify_login,
)
from database import (
    DB_PATH,
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
    page_title="FGV - Sistema de Indentificador de Coleta",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inicializa tabela de usuários e garante o Master
init_auth()

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

PAGE_SIZE = 10


_TIPO_LABEL = {
    "master": "Master",
    "administrador": "Administrador",
    "tecnico": "Técnico",
}

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def _init_state() -> None:
    defaults = {
        "user": None,
        "login_view": "login",   # "login" | "change_password"
        "current_view": "data",  # "data"  | "management"
        "selected_ids": set(),
        "page": 1,
        "table_version": 0,
        "filter_suffix": 0,
        "filters_open": False,
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

        if st.session_state.login_view == "login":
            _render_login_form()
        else:
            _render_change_password_form()


def _render_login_form() -> None:
    nomes = [u["nome"] for u in get_all_users()]
    with st.form("login_form"):
        st.subheader("Acesso ao Sistema")
        nome = st.selectbox("Usuário", options=nomes)
        senha = st.text_input("Senha", type="password", placeholder="Senha")
        entrar = st.form_submit_button("Entrar", use_container_width=True, type="primary")

        if entrar:
            if not senha:
                st.error("Preencha a senha.")
            else:
                user = verify_login(nome, senha)
                if user:
                    st.session_state.user = user
                    st.session_state.login_view = "login"
                    st.rerun()
                else:
                    st.error("Usuário ou senha incorretos.")

    st.divider()
    if st.button("🔑 Alterar Senha", use_container_width=True):
        st.session_state.login_view = "change_password"
        st.rerun()


def _render_change_password_form() -> None:
    nomes = [u["nome"] for u in get_all_users()]
    with st.form("change_password_form"):
        st.subheader("Alterar Senha")
        nome = st.selectbox("Usuário", options=nomes)
        senha_atual = st.text_input("Senha atual", type="password")
        nova_senha = st.text_input("Nova senha", type="password")
        confirmar = st.text_input("Confirmar nova senha", type="password")

        c1, c2 = st.columns(2)
        with c1:
            submitted = st.form_submit_button(
                "Alterar Senha", use_container_width=True, type="primary"
            )
        with c2:
            voltar = st.form_submit_button("← Voltar", use_container_width=True)

        if voltar:
            st.session_state.login_view = "login"
            st.rerun()

        if submitted:
            if not all([nome, senha_atual, nova_senha, confirmar]):
                st.error("Preencha todos os campos.")
            elif nova_senha != confirmar:
                st.error("A nova senha e a confirmação não coincidem.")
            else:
                ok, msg = change_password(nome, senha_atual, nova_senha)
                if ok:
                    st.success(msg + " Faça login com a nova senha.")
                    st.session_state.login_view = "login"
                else:
                    st.error(msg)


# ---------------------------------------------------------------------------
# Sidebar (após login)
# ---------------------------------------------------------------------------

def _render_sidebar_authenticated() -> None:
    user = st.session_state.user
    tipo_label = _TIPO_LABEL.get(user["tipo"], user["tipo"].capitalize())

    with st.sidebar:
        logo_path = Path(__file__).parent / "fgv_ibre.png"
        if logo_path.exists():
            _, logo_col, _ = st.columns([0.3, 3, 0.3])
            with logo_col:
                st.image(str(logo_path), use_container_width=True)
        st.markdown(f"### Bem-vindo, {user['nome']}!")
        st.caption(f"Perfil: {tipo_label}")
        st.divider()

        if st.button("🚪 Sair", use_container_width=True):
            for key in ("user", "selected_ids", "page", "table_version", "filter_suffix"):
                del st.session_state[key]
            st.session_state.current_view = "data"
            st.session_state.login_view = "login"
            st.rerun()

        if user["tipo"] in ("master", "administrador"):
            st.divider()
            if st.session_state.current_view == "data":
                if st.button("⚙️ Gerenciamento", use_container_width=True, key="btn_management"):
                    st.session_state.current_view = "management"
                    st.rerun()
            else:
                if st.button("🔙 Voltar à Consulta", use_container_width=True, key="btn_back"):
                    st.session_state.current_view = "data"
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

    cod_inf = nome_inf = marca = ean_sku = busca_texto = tipo_preco = uf = ""
    data_coleta = None

    with st.expander("🔍 Filtros", expanded=False):
        # Botão limpar no topo direito
        th1, th2 = st.columns([9, 1])
        with th2:
            if st.button("Limpar Filtro", key=f"limpar_{sfx}", use_container_width=True):
                st.session_state.filter_suffix += 1
                st.session_state.page = 1
                st.session_state.table_version += 1
                st.rerun()

        col1, col2, col3, col4 = st.columns([2, 2, 2, 2])

        with col1:
            cod_inf = st.selectbox(
                "Cód. Informante",
                options=[""] + options["cod_informante"],
                key=f"f_cod_{sfx}",
            )
            ean_sku = st.text_input(
                "EAN / SKU",
                placeholder="Digite o código exato...",
                key=f"f_ean_{sfx}",
            )

        with col2:
            nome_inf = st.selectbox(
                "Nome Informante",
                options=[""] + options["nome_informante"],
                key=f"f_nome_{sfx}",
            )
            busca_texto = st.text_input(
                "Descrição",
                placeholder="Ex: sabão pó nestlé...",
                key=f"f_texto_{sfx}",
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
            data_coleta = st.date_input(
                "Dados Coletados a Partir de",
                value=None,
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
    if data_coleta:
        filters["data_apos"] = data_coleta

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


def _render_metrics(placeholder, total: int) -> None:
    n_sel = len(st.session_state.selected_ids)

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
                    <strong>Produtos Selecionados:</strong> {_val(n_sel)}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Controles de seleção
# ---------------------------------------------------------------------------

def _render_selection_controls(filters: dict, total: int) -> None:
    all_ids = set(get_all_filtered_ids(filters)) if total > 0 else set()
    all_selected = bool(all_ids) and all_ids.issubset(st.session_state.selected_ids)

    checked = st.checkbox(
        "Selecionar todos os produtos filtrados",
        value=all_selected,
        key=f"chk_all_{st.session_state.table_version}",
    )

    if checked and not all_selected:
        st.session_state.selected_ids.update(all_ids)
        st.session_state.table_version += 1
        st.rerun()
    elif not checked and all_selected:
        st.session_state.selected_ids -= all_ids
        st.session_state.table_version += 1
        st.rerun()


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
    "insumo_informado": st.column_config.NumberColumn("Insumo Informado", width="small"),
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
    "periodicidade", "tipo_preco", "cod_insumo", "ean", "sku",
    "insumo_informado", "url", "descricao", "marca", "uf", "moeda",
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
        height=min(36 + len(display) * 35, 36 + PAGE_SIZE * 35),
    )

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
# Página de Dados
# ---------------------------------------------------------------------------

def _data_page() -> None:
    # Verifica se a tabela de coleta existe
    with sqlite3.connect(str(DB_PATH)) as conn:
        has_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='coleta_produtos'"
        ).fetchone()

    if not has_table:
        st.error(
            "Tabela de coleta não encontrada. "
            "Execute `python generate_data.py` para criar os dados."
        )
        return

    _render_banner()

    # Placeholder para métricas — renderizado acima dos filtros
    metrics_placeholder = st.empty()

    options = _get_filter_options_cached()
    filters = _render_filters(options)

    filters_hash = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()
    if st.session_state.get("last_filters_hash") != filters_hash:
        st.session_state.page = 1
        st.session_state.last_filters_hash = filters_hash

    with st.spinner("Carregando dados..."):
        total = get_total_count(filters)
        page_df = get_page_data(filters, st.session_state.page, PAGE_SIZE)

    _render_metrics(metrics_placeholder, total)
    _render_export()
    _render_selection_controls(filters, total)
    _render_table(page_df)
    _render_pagination(total)


# ---------------------------------------------------------------------------
# Página de Gerenciamento de Usuários
# ---------------------------------------------------------------------------

def _management_page() -> None:
    st.markdown(
        """
        <div class="main-header">
            <h1>⚙️ Gerenciamento</h1>
            <p>Adicione, visualize e remova usuários do sistema</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Lista de usuários ---
    st.subheader("Usuários Cadastrados")
    users = get_all_users()

    h1, h2, h3, h4, h5 = st.columns([3, 3, 2, 2, 1])
    h1.markdown("**Nome**")
    h2.markdown("**Email**")
    h3.markdown("**Tipo**")
    h4.markdown("**Área**")
    h5.markdown("**Ação**")
    st.divider()

    for u in users:
        is_master = u["tipo"] == "master"
        c1, c2, c3, c4, c5 = st.columns([3, 3, 2, 2, 1])

        if is_master:
            nome_html = f"<span style='color:#999;font-style:italic'>{u['nome']}</span>"
            tipo_html = f"<span style='color:#999;font-style:italic'>{_TIPO_LABEL.get(u['tipo'])}</span>"
            c1.markdown(nome_html, unsafe_allow_html=True)
            c2.markdown("<span style='color:#bbb'>—</span>", unsafe_allow_html=True)
            c3.markdown(tipo_html, unsafe_allow_html=True)
            c4.markdown("<span style='color:#bbb'>—</span>", unsafe_allow_html=True)
            c5.markdown("<span style='color:#ccc' title='Não pode ser removido'>🔒</span>", unsafe_allow_html=True)
        else:
            c1.write(u["nome"])
            c2.write(u["email"] or "—")
            c3.write(_TIPO_LABEL.get(u["tipo"], u["tipo"]))
            c4.write(u.get("area") or "—")
            if c5.button("🗑️", key=f"del_{u['id']}", help=f"Remover {u['nome']}"):
                ok, msg = remove_user(u["id"])
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    st.divider()

    # --- Formulário de adição ---
    st.subheader("Adicionar Usuário")
    with st.form("add_user_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            novo_nome = st.text_input("Nome do usuário *")
            novo_email = st.text_input("Email do usuário *")
            nova_area = st.text_input("Área")
        with col2:
            nova_senha = st.text_input("Senha *", type="password")
            novo_tipo = st.selectbox(
                "Tipo de usuário *",
                options=["tecnico", "administrador"],
                format_func=lambda x: "Técnico" if x == "tecnico" else "Administrador",
            )

        st.caption("* Campos obrigatórios. Email é obrigatório para Administrador e Técnico.")

        if st.form_submit_button("➕ Adicionar Usuário", type="primary"):
            ok, msg = create_user(novo_nome, novo_email, nova_senha, novo_tipo, nova_area)
            if ok:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def _get_filter_options_cached() -> dict:
    return get_filter_options()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    _init_state()

    if not st.session_state.user:
        _login_page()
        return

    _render_sidebar_authenticated()

    if st.session_state.current_view == "management":
        _management_page()
    else:
        _data_page()


if __name__ == "__main__":
    main()
