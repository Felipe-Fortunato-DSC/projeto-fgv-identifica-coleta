"""
Exportação para Excel (.xlsx) com formatação profissional:
  - Aba "produtos"
  - Cabeçalho em negrito com fundo colorido
  - Ajuste automático de largura de colunas
  - Formatação de preço e data
"""

import io
from datetime import date

import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

# Mapeamento coluna -> rótulo amigável (mesmo da exibição)
COLUMN_LABELS = {
    "data_coleta": "Data Coleta",
    "plataforma": "Plataforma",
    "cod_informante": "Cód. Informante",
    "nome_informante": "Nome Informante",
    "periodicidade": "Periodicidade",
    "tipo_preco": "Tipo Preço",
    "cod_insumo": "Cód. Insumo",
    "ean": "EAN",
    "sku": "SKU",
    "insumo_informado": "Insumo Informado",
    "url": "URL",
    "descricao": "Descrição",
    "marca": "Marca",
    "uf": "UF",
    "moeda": "Moeda",
    "preco": "Preço (R$)",
    "preco_promocional": "Preço Promo (R$)",
    "id_produto": "ID Produto",
    "id_coleta": "ID Coleta",
    "id_imagem": "ID Imagem",
}

HEADER_BG = "1B4F72"   # azul escuro
HEADER_FG = "FFFFFF"   # branco


def _auto_width(ws) -> None:
    """Ajusta a largura de cada coluna ao conteúdo mais longo."""
    for col_cells in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            try:
                cell_len = len(str(cell.value)) if cell.value is not None else 0
                max_len = max(max_len, cell_len)
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_len + 4, 60)


def export_to_excel(df: pd.DataFrame) -> bytes:
    """
    Recebe um DataFrame com os produtos selecionados e retorna
    o conteúdo do arquivo .xlsx como bytes.
    """
    wb = Workbook()
    ws = wb.active
    ws.title = "produtos"

    header_font = Font(bold=True, color=HEADER_FG)
    header_fill = PatternFill("solid", fgColor=HEADER_BG)
    center = Alignment(horizontal="center", vertical="center")

    # --- Cabeçalho ---
    headers = [COLUMN_LABELS.get(c, c) for c in df.columns]
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center

    ws.row_dimensions[1].height = 20

    # --- Dados ---
    price_cols = {"preco", "preco_promocional"}
    date_cols = {"data_coleta"}

    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        for col_idx, (col_name, value) in enumerate(
            zip(df.columns, row), start=1
        ):
            cell = ws.cell(row=row_idx, column=col_idx)

            if col_name in price_cols and value is not None:
                try:
                    cell.value = float(value)
                    cell.number_format = 'R$ #,##0.00'
                except (ValueError, TypeError):
                    cell.value = value
            elif col_name in date_cols and isinstance(value, date):
                cell.value = value
                cell.number_format = "DD/MM/YYYY"
            else:
                cell.value = value

            cell.alignment = Alignment(vertical="center")

    _auto_width(ws)

    # Freeze primeira linha
    ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()
