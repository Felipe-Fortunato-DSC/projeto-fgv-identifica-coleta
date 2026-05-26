"""
Crítica analítica da Carga BP — adaptação de ecr_validation/queries/analytical_query.sql
para pandas puro (a base de dados típica do informante é pequena: dezenas/centenas de itens).

Para cada (CD_INFORM, NR_SEQ_INSINF) da carga enriquecida com preço do
scraping, cruza com regras do Snowflake (último preço BP, limites de
variação, faltas, frete) e classifica como aprovado (manutencao=0) ou
reprovado (manutencao=1) para revisão manual.
"""

from __future__ import annotations

import pandas as pd

# Saída esperada por _enrich_with_critica em todas as situações.
CRITICA_COLS = [
    "ultimo_preco_db",
    "variacao_atual_pct",
    "vl_lim_inf_var",
    "vl_lim_sup_var",
    "faltas_consecutivas_aceitas",
    "total_faltas_com_atual",
    "validacao_variacao",
    "validacao_faltas",
    "validacao_incidencia",
    "manutencao",
    "motivo_manutencao",
]


def _status_preco(row) -> str:
    ultimo = row["ultimo_preco_db"]
    if pd.isna(ultimo) or ultimo == 0:
        return "SEM PRECO ANTERIOR"
    preco = row["VL_PRECCOL"]
    if pd.isna(preco):
        return "FALTA COLETADA"
    var_pct = row["variacao_atual_pct"]
    lim_inf = row["vl_lim_inf_var"]
    lim_sup = row["vl_lim_sup_var"]
    # Sem limites configurados ⇒ não há como julgar variação. Mantém "DENTRO".
    if pd.isna(var_pct) or pd.isna(lim_inf) or pd.isna(lim_sup):
        return "DENTRO DO LIMITE"
    return "DENTRO DO LIMITE" if lim_inf <= var_pct <= lim_sup else "FORA DO LIMITE"


def _status_faltas(row) -> str:
    total = row["total_faltas_com_atual"]
    limite = row["faltas_consecutivas_aceitas"]
    if pd.isna(total) or pd.isna(limite):
        return "FALTAS DENTRO DO LIMITE"
    return "EXCEDEU LIMITE DE FALTAS" if total > limite else "FALTAS DENTRO DO LIMITE"


def _status_incidencia(row) -> str:
    inciden = str(row.get("cd_inciden") or "").upper().strip()
    not_decl = str(row.get("IN_INCID_NAO_DECLARADA") or "").upper().strip()
    if inciden == "FR" and not_decl in {"S", "1", "TRUE"}:
        return "FRETE NAO DECLARADO"
    return "FRETE OK"


def _motivo(row) -> str:
    parts: list[str] = []
    if row["validacao_faltas"] == "EXCEDEU LIMITE DE FALTAS":
        parts.append("FALTAS ACIMA DO LIMITE ACEITO")
    if row["validacao_variacao"] == "FORA DO LIMITE":
        parts.append("PRECO FORA DO LIMITE VARIACAO")
    if row["validacao_variacao"] == "SEM PRECO ANTERIOR":
        parts.append("SEM HISTORICO DE PRECO ANTERIOR")
    if row["validacao_variacao"] == "FALTA COLETADA":
        parts.append("INSUMO NAO COLETADO NO CICLO")
    if row["validacao_incidencia"] == "FRETE NAO DECLARADO":
        parts.append("FRETE NAO DECLARADO")
    return " | ".join(parts)


def _empty_critica(df: pd.DataFrame) -> pd.DataFrame:
    """Devolve df com as colunas de crítica vazias/padrão para evitar KeyError no caller."""
    out = df.copy()
    defaults = {
        "ultimo_preco_db": pd.NA, "variacao_atual_pct": pd.NA,
        "vl_lim_inf_var": pd.NA, "vl_lim_sup_var": pd.NA,
        "faltas_consecutivas_aceitas": pd.NA, "total_faltas_com_atual": pd.NA,
        "validacao_variacao": "SEM PRECO ANTERIOR",
        "validacao_faltas":   "FALTAS DENTRO DO LIMITE",
        "validacao_incidencia": "FRETE OK",
        "manutencao": 1,
        "motivo_manutencao": "SEM HISTORICO DE PRECO ANTERIOR",
    }
    for k, v in defaults.items():
        out[k] = v
    return out


def run_validation(df_carga: pd.DataFrame, df_rules: pd.DataFrame) -> pd.DataFrame:
    """Aplica os CASEs do analytical_query.sql sobre df_carga (carga já
    enriquecida com VL_PRECCOL do Athena) cruzando com df_rules (regras do
    Snowflake). Retorna df_carga com as colunas de CRITICA_COLS adicionadas.

    df_carga precisa conter: CD_INFORM, NR_SEQ_INSINF, VL_PRECCOL (float, NaN ok).
    df_rules vem do Snowflake — colunas em UPPER são normalizadas aqui.
    """
    if df_carga.empty:
        return _empty_critica(df_carga)

    if df_rules is None or df_rules.empty:
        return _empty_critica(df_carga)

    rules = df_rules.copy()
    # Snowflake devolve nomes em UPPER. Mantém IN_INCID_NAO_DECLARADA como está
    # (o engine espera essa grafia, igual ao SQL original).
    rules.columns = [
        c if c == "IN_INCID_NAO_DECLARADA" else c.lower()
        for c in rules.columns
    ]

    rules["nr_seq_insinf"] = (
        pd.to_numeric(rules["nr_seq_insinf"], errors="coerce")
        .astype("Int64").astype(str)
    )
    keep = [
        "nr_seq_insinf", "ultimo_preco_db",
        "vl_lim_inf_var", "vl_lim_sup_var",
        "quantidade_de_faltas", "faltas_consecutivas_aceitas",
        "cd_inciden", "vl_frete_anterior", "IN_INCID_NAO_DECLARADA",
    ]
    rules = rules[[c for c in keep if c in rules.columns]].drop_duplicates(
        subset=["nr_seq_insinf"], keep="first"
    )

    df = df_carga.merge(
        rules, how="left",
        left_on="NR_SEQ_INSINF", right_on="nr_seq_insinf",
    ).drop(columns=["nr_seq_insinf"], errors="ignore")

    # Numéricos
    for col in ("VL_PRECCOL", "ultimo_preco_db", "vl_lim_inf_var",
                "vl_lim_sup_var", "quantidade_de_faltas",
                "faltas_consecutivas_aceitas"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    ultimo = df["ultimo_preco_db"]
    df["variacao_atual_pct"] = ((df["VL_PRECCOL"] - ultimo) / ultimo.replace(0, pd.NA)) * 100

    df["total_faltas_com_atual"] = (
        df["quantidade_de_faltas"].fillna(0)
        + df["VL_PRECCOL"].isna().astype(int)
    )

    df["validacao_variacao"]   = df.apply(_status_preco, axis=1)
    df["validacao_faltas"]     = df.apply(_status_faltas, axis=1)
    df["validacao_incidencia"] = df.apply(_status_incidencia, axis=1)

    fail = (
        df["validacao_variacao"].isin(
            ["FORA DO LIMITE", "SEM PRECO ANTERIOR", "FALTA COLETADA"]
        )
        | df["validacao_faltas"].eq("EXCEDEU LIMITE DE FALTAS")
        | df["validacao_incidencia"].eq("FRETE NAO DECLARADO")
    )
    df["manutencao"] = fail.astype(int)
    df["motivo_manutencao"] = df.apply(_motivo, axis=1)

    return df
