import os

import pandas as pd
import snowflake.connector

def _cfg(key: str) -> str:
    return os.environ.get(key)


def _connect():
    return snowflake.connector.connect(
        account=_cfg("ACCOUNT"),
        user=_cfg("USER"),
        password=_cfg("PASSWORD"),
        role=_cfg("ROLE"),
        warehouse=_cfg("WAREHOUSE"),
        database=_cfg("DATABASE"),
        schema=_cfg("SCHEMA"),
    )


def get_encomenda_df() -> pd.DataFrame:
    """Lê a tabela de encomenda do Snowflake e devolve como DataFrame.
    Levanta snowflake.connector.errors.* em falha de conexão/query — o caller
    decide se faz fallback (ex.: para um xlsx local)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            fqn = f'{_cfg("DATABASE")}.{_cfg("SCHEMA")}.{_cfg("TABLE")}'
            cur.execute(f"SELECT * FROM {fqn}")
            return cur.fetch_pandas_all()
        finally:
            cur.close()
    finally:
        conn.close()


# Regras para a crítica analítica (faltas, limites de variação, frete).
# Adaptado de ecr_validation/queries/query_critica_snow.sql.
_CRITICA_RULES_SQL = """
WITH Encomendas_Limpas AS (
    SELECT DISTINCT
        nr_seq_insinf, cd_period, nm_insumo, cd_inform, nm_inform,
        cd_tppreco, job, vl_preccol
    FROM BASES_SPDO.DB_COWEB.TBL_ENCOMENDA_DEC
    WHERE CD_COLETOR = 'COWEB'
      AND (cd_tppreco IS NULL OR cd_tppreco <> 'AE')
),
Mapeamento_Periodicidade AS (
    SELECT cd_period, tipo_periodicidade, faltas_consecutivas_aceitas
    FROM (VALUES
        ('D02TP', 'Semanal', 3), ('D03TP', 'Semanal', 3), ('D04TP', 'Semanal', 3),
        ('D05TP', 'Semanal', 3), ('D06TP', 'Semanal', 3), ('E25',   'Semanal', 3),
        ('E35',   'Semanal', 3), ('E246',  'Semanal', 3), ('QUINZ', 'Quinzenal', 3),
        ('M0',    'Decendial', 3), ('M1',    'Decendial', 3), ('M2',    'Decendial', 3),
        ('M3',    'Decendial', 3), ('M4',    'Decendial', 3), ('M5',    'Decendial', 3),
        ('M6',    'Decendial', 3), ('M7',    'Decendial', 3), ('M8',    'Decendial', 3),
        ('M12',   'Decendial', 3), ('M15',   'Decendial', 3), ('M16',   'Decendial', 3),
        ('M17',   'Decendial', 3), ('M020',  'Decendial', 3), ('DEC',   'Decendial', 3),
        ('M21',   'Decendial', 3), ('M24',   'Decendial', 3), ('M030',  'Decendial', 3),
        ('M10',   'Mensal', 3), ('M20',   'Mensal', 3), ('M30',   'Mensal', 3),
        ('M02',   'Mensal', 3), ('M06',   'Mensal', 3), ('M07',   'Mensal', 3),
        ('M08',   'Mensal', 3), ('B0220', 'Bimestral', 2), ('B0230', 'Bimestral', 2),
        ('B0110', 'Bimestral', 2), ('B0120', 'Bimestral', 2), ('T0120', 'Trimestral', 1),
        ('T0130', 'Trimestral', 1), ('T0210', 'Trimestral', 1), ('T0220', 'Trimestral', 1),
        ('S0430', 'Semestral', 0), ('COL10', 'Escolar', 1), ('COL20', 'Escolar', 1),
        ('COL30', 'Escolar', 1)
    ) AS t(cd_period, tipo_periodicidade, faltas_consecutivas_aceitas)
),
Ultimas_3_Previsoes AS (
    SELECT
        periodicidade,
        data_prevista,
        ROW_NUMBER() OVER (PARTITION BY periodicidade ORDER BY data_prevista DESC) AS rn
    FROM BASES_SPDO.DB_GESTAO_BANCO_PRECO.TBL_BP_CALENDARIO_PREVISAO
    WHERE data_prevista <= CURRENT_DATE()
),
Current_Previsao AS (
    SELECT periodicidade, data_prevista as current_data_prevista
    FROM Ultimas_3_Previsoes WHERE rn = 1
),
Esqueleto_3_Coletas AS (
    SELECT e.nr_seq_insinf, e.job, u.data_prevista
    FROM Encomendas_Limpas e
    INNER JOIN Ultimas_3_Previsoes u ON e.cd_period = u.periodicidade
    WHERE u.rn <= 3
),
Check_Faltas AS (
    SELECT
        esc.nr_seq_insinf, esc.job,
        CASE WHEN f.nr_seq_insinf IS NOT NULL THEN 1 ELSE 0 END AS teve_falta
    FROM Esqueleto_3_Coletas esc
    LEFT JOIN BASES_IBRE.DB_BP.PRECO_COLETADO_FALTA f
        ON  esc.nr_seq_insinf  = f.nr_seq_insinf
        AND esc.data_prevista  = f.dt_prv_preccol
        AND f.cd_coletor       = 'COWEB'
),
Faltas_Nas_Ultimas_3 AS (
    SELECT nr_seq_insinf, job, SUM(teve_falta) as quantidade_de_faltas
    FROM Check_Faltas GROUP BY nr_seq_insinf, job
),
Coleta_incidencias AS (
    SELECT nr_seq_insinf, dt_col_preccol, cd_inciden, tx_aliq_incprec,
           vl_incprec, IN_INCID_NAO_DECLARADA
    FROM BASES_IBRE.DB_BP.INCIDENCIA_PRECO
    WHERE cd_inciden = 'FR'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY nr_seq_insinf ORDER BY dt_col_preccol DESC) = 1
),
Limites_Variacao AS (
    SELECT nr_seq_insinf, vl_lim_inf_var, vl_lim_sup_var
    FROM BASES_IBRE.DB_BP.PRECO_COLETADO
    WHERE cd_coletor = 'COWEB'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY nr_seq_insinf ORDER BY dt_col_preccol DESC) = 1
)
SELECT
    e.nr_seq_insinf, e.cd_period, e.nm_insumo, e.job,
    cp.current_data_prevista,
    COALESCE(f.quantidade_de_faltas, 0) AS quantidade_de_faltas,
    mp.tipo_periodicidade,
    COALESCE(mp.faltas_consecutivas_aceitas, 0) AS faltas_consecutivas_aceitas,
    e.vl_preccol AS ultimo_preco_db,
    l.vl_lim_inf_var, l.vl_lim_sup_var,
    i.cd_inciden, i.vl_incprec AS vl_frete_anterior, i.IN_INCID_NAO_DECLARADA
FROM Encomendas_Limpas e
LEFT JOIN Current_Previsao cp ON e.cd_period = cp.periodicidade
LEFT JOIN Faltas_Nas_Ultimas_3 f ON e.nr_seq_insinf = f.nr_seq_insinf AND e.job = f.job
LEFT JOIN Mapeamento_Periodicidade mp ON e.cd_period = mp.cd_period
LEFT JOIN Limites_Variacao l ON e.nr_seq_insinf = l.nr_seq_insinf
LEFT JOIN Coleta_incidencias i ON e.nr_seq_insinf = i.nr_seq_insinf
"""


def get_critica_rules_df(insumos_list: list[str]) -> pd.DataFrame:
    """Para a lista de NR_SEQ_INSINF, devolve as regras necessárias para a
    crítica analítica (último preço, limites de variação, faltas, frete).

    Faz fetch em chunks de 5k IDs para evitar predicado IN gigante.
    Colunas vêm em UPPER do Snowflake — caller deve normalizar.
    """
    if not insumos_list:
        return pd.DataFrame()

    conn = _connect()
    try:
        cur = conn.cursor()
        try:
            frames: list[pd.DataFrame] = []
            chunk = 5000
            for i in range(0, len(insumos_list), chunk):
                lote = insumos_list[i : i + chunk]
                lote_str = ",".join(f"'{x}'" for x in lote)
                sql = (
                    f"SELECT * FROM ({_CRITICA_RULES_SQL}) AS sub "
                    f"WHERE TO_VARCHAR(NR_SEQ_INSINF) IN ({lote_str})"
                )
                cur.execute(sql)
                frames.append(cur.fetch_pandas_all())
            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        finally:
            cur.close()
    finally:
        conn.close()
