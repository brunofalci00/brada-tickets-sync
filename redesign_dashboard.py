"""
Dashboard Corridas Vai Bem — multi-etapa.
Gera a aba de dashboard para cada etapa configurada em sync.py.
Rodar localmente com credenciais em C:/Users/bruno/.brada-secrets/sheets-sa.json para rebuildar abas.

Layout (alturas dinamicas em funcao do n de modalidades):
  r1-r2  titulo / subtitulo
  r4-r5  KPIs (Total, Feminino, Masculino, % Nubank)
  r7     section header "INSCRITOS POR PROVA E KIT"
  r8     header da tabela (Vai Bem | Vai Bem PCD | Premium | Premium PCD | Basico | Total)
  r9..   uma linha por modalidade (3 pra BSB/BH, 4 pra Salvador c/ Caminhada)
  r{T}   Total geral da tabela (T = 9 + n)
  r{T+1} espaco
  r{T+2} section header "INSCRICOES POR TIPO DE CUPOM"
  ... +7 categorias + Total + footer
"""
import argparse
import os
import re
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# =============================================
# PALETA BRADA (laranja #f96500)
# =============================================
LARANJA_500 = {"red": 0.976, "green": 0.396, "blue": 0.0}
LARANJA_400 = {"red": 1.0, "green": 0.529, "blue": 0.239}
LARANJA_200 = {"red": 1.0, "green": 0.820, "blue": 0.678}
LARANJA_100 = {"red": 1.0, "green": 0.910, "blue": 0.839}
LARANJA_50 = {"red": 1.0, "green": 0.961, "blue": 0.922}
ESCURO = {"red": 0.2, "green": 0.12, "blue": 0.0}
BRANCO = {"red": 1, "green": 1, "blue": 1}
CINZA_TEXTO = {"red": 0.333, "green": 0.333, "blue": 0.333}
CINZA_LEVE = {"red": 0.98, "green": 0.98, "blue": 0.98}
CINZA_FOOTER = {"red": 0.733, "green": 0.733, "blue": 0.733}

BORDA_HEADER = {"bottom": {"style": "SOLID_MEDIUM", "color": LARANJA_200}}
BORDA_TOTAL = {"top": {"style": "SOLID_MEDIUM", "color": LARANJA_500}}


def _build_legacy_dashboard(sh, label, raw_tab, dash_tab, has_nubank=True, modalidades=None):
    """
    Reconstroi a aba `dash_tab` apontando formulas para `raw_tab`.

    label: ex. "Brasilia", "Belo Horizonte", "Salvador"
    raw_tab: ex. "raw_inscritos_brasilia"
    modalidades: lista de tuplas (label_exibido, filtro_modalidade), uma por linha da tabela.
        - label_exibido eh o que aparece na col B (ex: "5km", "Corrida 3km").
        - filtro_modalidade eh o valor exato do campo Modalidade na raw (ex: "Corrida 5km",
          "Caminhada 3km") usado no COUNTIFS. Salvador eh o unico evento com Caminhada
          (a modalidade NAO segue o padrao "Corrida {N}km" -> assinatura precisa do filtro
          explicito; nao da pra derivar do label).
        Default: [("5km","Corrida 5km"),("10km","Corrida 10km"),("15km","Corrida 15km")]
          (padrao historico BSB; OK pra reusar em eventos futuros sem caminhada nem
          modalidade renomeada).
    """
    if modalidades is None:
        modalidades = [("5km", "Corrida 5km"),
                       ("10km", "Corrida 10km"),
                       ("15km", "Corrida 15km")]
    n = len(modalidades)
    # Layout dinamico: posicoes calculadas a partir de n
    R_DIST_START = 9
    R_DIST_END = R_DIST_START + n - 1
    R_TOTAL = R_DIST_END + 1
    R_BLANK_1 = R_TOTAL + 1
    R_CUP_SEC = R_BLANK_1 + 1
    R_CUP_HDR = R_CUP_SEC + 1
    R_CUP_DATA_START = R_CUP_HDR + 1
    R_CUP_DATA_END = R_CUP_DATA_START + 6  # 7 categorias
    R_CUP_TOTAL = R_CUP_DATA_END + 1
    R_BLANK_2 = R_CUP_TOTAL + 1
    R_FOOTER = R_BLANK_2 + 1
    R_MAX = R_FOOTER + 2  # margem de seguranca p/ fundo branco

    # Cria aba se nao existir
    try:
        dash = sh.worksheet(dash_tab)
    except gspread.exceptions.WorksheetNotFound:
        dash = sh.add_worksheet(title=dash_tab, rows=max(50, R_MAX + 10), cols=12)
        print(f"  Aba '{dash_tab}' criada.")

    sid = dash.id

    # Helpers (fecham sobre `sid`)
    def fmt(r1, c1, r2, c2, **kw):
        cell_fmt = {}
        text_fmt = {"fontFamily": "Calibri"}
        if "bg" in kw: cell_fmt["backgroundColor"] = kw["bg"]
        if "fg" in kw: text_fmt["foregroundColor"] = kw["fg"]
        if "bold" in kw: text_fmt["bold"] = kw["bold"]
        if "italic" in kw: text_fmt["italic"] = kw["italic"]
        if "size" in kw: text_fmt["fontSize"] = kw["size"]
        cell_fmt["textFormat"] = text_fmt
        if "halign" in kw: cell_fmt["horizontalAlignment"] = kw["halign"]
        if "valign" in kw: cell_fmt["verticalAlignment"] = kw["valign"]
        if "borders" in kw: cell_fmt["borders"] = kw["borders"]
        if "numberFormat" in kw: cell_fmt["numberFormat"] = kw["numberFormat"]
        return {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": r1-1, "endRowIndex": r2, "startColumnIndex": c1-1, "endColumnIndex": c2},
            "cell": {"userEnteredFormat": cell_fmt},
            "fields": "userEnteredFormat"
        }}

    def merge(r1, c1, r2, c2):
        return {"mergeCells": {
            "range": {"sheetId": sid, "startRowIndex": r1-1, "endRowIndex": r2, "startColumnIndex": c1-1, "endColumnIndex": c2},
            "mergeType": "MERGE_ALL"
        }}

    def col_w(start, end, px):
        return {"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": start-1, "endIndex": end},
            "properties": {"pixelSize": px}, "fields": "pixelSize"
        }}

    def row_h(row, px):
        return {"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": row-1, "endIndex": row},
            "properties": {"pixelSize": px}, "fields": "pixelSize"
        }}

    # =============================================
    # LIMPAR / SETUP
    # =============================================
    dash.clear()
    sh.batch_update({"requests": [
        {"unmergeCells": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": max(40, R_MAX), "startColumnIndex": 0, "endColumnIndex": 11}}},
        {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": max(40, R_MAX), "startColumnIndex": 0, "endColumnIndex": 11},
            "cell": {"userEnteredFormat": {}},
            "fields": "userEnteredFormat"
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": sid, "gridProperties": {"hideGridlines": True}},
            "fields": "gridProperties.hideGridlines"
        }},
        {"updateSheetProperties": {
            "properties": {"sheetId": sid, "tabColor": {"red": 0.976, "green": 0.396, "blue": 0.0}},
            "fields": "tabColor"
        }},
    ]})

    # =============================================
    # FORMULAS (locale pt_BR = ;)
    # Categorias de kit usam wildcards case-insensitive para
    # tolerar variacao de nomenclatura entre etapas (ex: "Kit Vai Bem"
    # vs "KIT VAIBEM"). Wildcards *VAI*BEM* e *PREMIUM* cobrem ambos.
    # =============================================
    r = raw_tab  # alias curto

    cells = [
        # Row 1: Titulo
        {"range": "B1", "values": [[f"CORRIDA VAI BEM — {label.upper()}"]]},
        # Row 2: Subtitulo (C2 = timestamp atualizado pelo sync.py)
        {"range": "B2", "values": [["Atualizado automaticamente a cada 1 hora"]]},

        # Row 4-5: KPIs
        {"range": "B4", "values": [["TOTAL INSCRITOS"]]},
        {"range": "B5", "values": [[f'=COUNTA({r}!A:A)-1']]},

        {"range": "D4", "values": [["FEMININO"]]},
        {"range": "D5", "values": [[f'=COUNTIF({r}!D:D;"F")&"  ("&TEXTO(IFERROR(COUNTIF({r}!D:D;"F")/(COUNTA({r}!A:A)-1);0);"0%")&")"']]},

        {"range": "F4", "values": [["MASCULINO"]]},
        {"range": "F5", "values": [[f'=COUNTIF({r}!D:D;"M")&"  ("&TEXTO(IFERROR(COUNTIF({r}!D:D;"M")/(COUNTA({r}!A:A)-1);0);"0%")&")"']]},
    ]

    # KPI Nubank (col H) — condicional em has_nubank
    if has_nubank:
        cells += [
            {"range": "H4", "values": [["% CUPOM NUBANK"]]},
            {"range": "H5", "values": [[f'=TEXTO(IFERROR(COUNTIFS({r}!F:F;"*NUBANK*";{r}!E:E;"Pago")/COUNTIF({r}!E:E;"Pago");0);"0%")']]},
        ]

    cells += [
        # Row 7: Section header
        {"range": "B7", "values": [["INSCRITOS POR PROVA E KIT"]]},

        # Row 8: Table header
        {"range": "B8:H8", "values": [["", "Vai Bem", "Vai Bem PCD", "Premium", "Premium PCD", "Básico", "Total"]]},

        # Rows R_DIST_START..R_DIST_END: modalidades (parametrizadas)
        *[cell for i, (label_mod, filtro_mod) in enumerate(modalidades) for cell in [
            {"range": f"B{R_DIST_START+i}", "values": [[label_mod]]},
            {"range": f"C{R_DIST_START+i}", "values": [[f'=COUNTIFS({r}!C:C;"{filtro_mod}";{r}!B:B;"*VAI*BEM*";{r}!B:B;"<>*PCD*";{r}!B:B;"<>*B?SICO*";{r}!E:E;"Pago")']]},
            {"range": f"D{R_DIST_START+i}", "values": [[f'=COUNTIFS({r}!C:C;"{filtro_mod}";{r}!B:B;"*VAI*BEM*";{r}!B:B;"*PCD*";{r}!E:E;"Pago")']]},
            {"range": f"E{R_DIST_START+i}", "values": [[f'=COUNTIFS({r}!C:C;"{filtro_mod}";{r}!B:B;"*PREMIUM*";{r}!B:B;"<>*PCD*";{r}!E:E;"Pago")']]},
            {"range": f"F{R_DIST_START+i}", "values": [[f'=COUNTIFS({r}!C:C;"{filtro_mod}";{r}!B:B;"*PREMIUM*";{r}!B:B;"*PCD*";{r}!E:E;"Pago")']]},
            {"range": f"G{R_DIST_START+i}", "values": [[f'=COUNTIFS({r}!C:C;"{filtro_mod}";{r}!B:B;"*B?SICO*")']]},
            {"range": f"H{R_DIST_START+i}", "values": [[f"=SOMA(C{R_DIST_START+i}:G{R_DIST_START+i})"]]},
        ]],

        # Row R_TOTAL: Total
        {"range": f"B{R_TOTAL}", "values": [["Total"]]},
        {"range": f"C{R_TOTAL}", "values": [[f"=SOMA(C{R_DIST_START}:C{R_DIST_END})"]]},
        {"range": f"D{R_TOTAL}", "values": [[f"=SOMA(D{R_DIST_START}:D{R_DIST_END})"]]},
        {"range": f"E{R_TOTAL}", "values": [[f"=SOMA(E{R_DIST_START}:E{R_DIST_END})"]]},
        {"range": f"F{R_TOTAL}", "values": [[f"=SOMA(F{R_DIST_START}:F{R_DIST_END})"]]},
        {"range": f"G{R_TOTAL}", "values": [[f"=SOMA(G{R_DIST_START}:G{R_DIST_END})"]]},
        {"range": f"H{R_TOTAL}", "values": [[f"=SOMA(H{R_DIST_START}:H{R_DIST_END})"]]},

        # Row R_CUP_SEC: Section header cupons
        {"range": f"B{R_CUP_SEC}", "values": [["INSCRIÇÕES POR TIPO DE CUPOM"]]},

        # Row R_CUP_HDR: Table header cupons
        {"range": f"B{R_CUP_HDR}:D{R_CUP_HDR}", "values": [["Tipo", "Qtd", "%"]]},
    ]

    # Rows R_CUP_DATA_START..R_CUP_DATA_END: 7 categorias (offsets fixos 0..6)
    cup_rows = [
        ("Nubank",
            f'=COUNTIFS({r}!F:F;"*NUBANK*";{r}!E:E;"Pago")'),
        ("Cortesia",  # exclui *INFLU* (CORTESIA - Influenciador bateria nos dois)
            f'=COUNTIFS({r}!F:F;"*CORTESIA*";{r}!F:F;"<>*INFLU*";{r}!E:E;"Pago")'),
        ("PCD",
            f'=COUNTIFS({r}!F:F;"*PCD*";{r}!E:E;"Pago")'),
        ("Página de Corrida",  # contains — titulo pode vir com espaco sobrando (ex: Salvador "Pagina de Corrida ")
            f'=COUNTIFS({r}!F:F;"*Pagina de Corrida*";{r}!E:E;"Pago")'),
        ("Influenciador",
            f'=COUNTIFS({r}!F:F;"*INFLU*";{r}!E:E;"Pago")'),
        ("Sem cupom",
            f'=COUNTIFS({r}!F:F;"";{r}!E:E;"Pago")'),
        ("Outros",  # residuo
            f'=COUNTIF({r}!E:E;"Pago")-SOMA(C{R_CUP_DATA_START}:C{R_CUP_DATA_START+5})'),
    ]
    for i, (nome, qtd_formula) in enumerate(cup_rows):
        rr = R_CUP_DATA_START + i
        cells += [
            {"range": f"B{rr}", "values": [[nome]]},
            {"range": f"C{rr}", "values": [[qtd_formula]]},
            {"range": f"D{rr}", "values": [[f'=IFERROR(C{rr}/COUNTIF({r}!E:E;"Pago");0)']]},
        ]

    cells += [
        # Row R_CUP_TOTAL: Total cupons
        {"range": f"B{R_CUP_TOTAL}", "values": [["Total"]]},
        {"range": f"C{R_CUP_TOTAL}", "values": [[f"=SOMA(C{R_CUP_DATA_START}:C{R_CUP_DATA_END})"]]},
        {"range": f"D{R_CUP_TOTAL}", "values": [[f'=IFERROR(C{R_CUP_TOTAL}/COUNTIF({r}!E:E;"Pago");0)']]},

        # Row R_FOOTER: Footer
        {"range": f"B{R_FOOTER}", "values": [[f"Fonte: API Ticketsports  •  Sync cada 1h via GitHub Actions  •  Dados brutos na aba {raw_tab}"]]},
    ]

    dash.batch_update(cells, value_input_option="USER_ENTERED")

    # =============================================
    # FORMATACAO
    # =============================================
    reqs = [
        # Column widths: A spacer, B labels, C-F data, G spacer, H total, I spacer
        col_w(1, 1, 20),
        col_w(2, 2, 100),
        col_w(3, 3, 80),
        col_w(4, 4, 95),
        col_w(5, 5, 80),
        col_w(6, 6, 100),
        col_w(7, 7, 80),
        col_w(8, 8, 70),
        col_w(9, 9, 20),

        # Row heights fixos (cabecalho + KPIs + section headers)
        row_h(1, 44), row_h(2, 22), row_h(3, 12),
        row_h(4, 18), row_h(5, 50), row_h(6, 12),
        row_h(7, 28), row_h(8, 28),
    ]
    # Modalidades (dinamico): cada linha 28px
    for rr in range(R_DIST_START, R_DIST_END + 1):
        reqs.append(row_h(rr, 28))
    reqs += [
        row_h(R_TOTAL, 32),
        row_h(R_BLANK_1, 12),
        row_h(R_CUP_SEC, 28),
        row_h(R_CUP_HDR, 28),
    ]
    for rr in range(R_CUP_DATA_START, R_CUP_DATA_END + 1):
        reqs.append(row_h(rr, 26))
    reqs += [
        row_h(R_CUP_TOTAL, 30),
        row_h(R_BLANK_2, 10),

        # Fundo branco global (cobre todas as linhas)
        fmt(1, 1, R_MAX, 9, bg=BRANCO),

        # === TITULO (row 1-2) ===
        merge(1, 2, 1, 8), merge(2, 2, 2, 8),
        fmt(1, 1, 1, 9, bg=ESCURO),
        fmt(1, 2, 1, 8, bg=ESCURO, fg=BRANCO, bold=True, size=16, halign="CENTER", valign="MIDDLE"),
        fmt(2, 1, 2, 9, bg=ESCURO),
        fmt(2, 2, 2, 8, bg=ESCURO, fg=LARANJA_200, italic=True, size=9, halign="CENTER", valign="MIDDLE"),

        # === KPIs (row 4-5) ===
        merge(4, 2, 4, 3), merge(5, 2, 5, 3),
        fmt(4, 2, 4, 3, fg=CINZA_TEXTO, bold=True, size=9, halign="CENTER"),
        fmt(5, 2, 5, 3, fg=LARANJA_500, bold=True, size=32, halign="CENTER", valign="MIDDLE"),

        merge(4, 4, 4, 5), merge(5, 4, 5, 5),
        fmt(4, 4, 4, 5, fg=CINZA_TEXTO, bold=True, size=9, halign="CENTER"),
        fmt(5, 4, 5, 5, fg=LARANJA_400, bold=True, size=18, halign="CENTER", valign="MIDDLE"),

        merge(4, 6, 4, 7), merge(5, 6, 5, 7),
        fmt(4, 6, 4, 7, fg=CINZA_TEXTO, bold=True, size=9, halign="CENTER"),
        fmt(5, 6, 5, 7, fg=LARANJA_400, bold=True, size=18, halign="CENTER", valign="MIDDLE"),

        fmt(4, 8, 4, 8, fg=CINZA_TEXTO, bold=True, size=9, halign="CENTER"),
        fmt(5, 8, 5, 8, fg=LARANJA_500, bold=True, size=32, halign="CENTER", valign="MIDDLE"),

        # === SECTION HEADER (row 7) ===
        merge(7, 2, 7, 8),
        fmt(7, 2, 7, 8, bg=LARANJA_500, fg=BRANCO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),

        # === TABLE HEADER (row 8) ===
        fmt(8, 2, 8, 8, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=10, halign="CENTER", valign="MIDDLE", borders=BORDA_HEADER),
        fmt(8, 2, 8, 2, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=10, halign="LEFT", valign="MIDDLE", borders=BORDA_HEADER),

        # === DATA ROWS modalidades (R_DIST_START..R_DIST_END) ===
        fmt(R_DIST_START, 2, R_DIST_END, 2, fg=CINZA_TEXTO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),
        fmt(R_DIST_START, 3, R_DIST_END, 7, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE"),
        fmt(R_DIST_START, 8, R_DIST_END, 8, fg=LARANJA_500, bold=True, size=11, halign="CENTER", valign="MIDDLE"),
    ]
    # Zebra nas modalidades de indice impar (i=1,3,...) — mesmo padrao do layout original
    for i in range(n):
        if i % 2 == 1:
            rr = R_DIST_START + i
            reqs += [
                fmt(rr, 2, rr, 8, bg=CINZA_LEVE),
                fmt(rr, 2, rr, 2, bg=CINZA_LEVE, fg=CINZA_TEXTO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),
                fmt(rr, 3, rr, 7, bg=CINZA_LEVE, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE"),
                fmt(rr, 8, rr, 8, bg=CINZA_LEVE, fg=LARANJA_500, bold=True, size=11, halign="CENTER", valign="MIDDLE"),
            ]

    reqs += [
        # === TOTAL ROW (R_TOTAL) ===
        fmt(R_TOTAL, 2, R_TOTAL, 8, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=11, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL),
        fmt(R_TOTAL, 2, R_TOTAL, 2, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=11, halign="LEFT", valign="MIDDLE", borders=BORDA_TOTAL),
        fmt(R_TOTAL, 8, R_TOTAL, 8, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=13, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL),

        # === SECTION HEADER CUPONS (R_CUP_SEC) ===
        merge(R_CUP_SEC, 2, R_CUP_SEC, 8),
        fmt(R_CUP_SEC, 2, R_CUP_SEC, 8, bg=LARANJA_500, fg=BRANCO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),

        # === TABLE HEADER CUPONS (R_CUP_HDR) ===
        fmt(R_CUP_HDR, 2, R_CUP_HDR, 4, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=10, halign="CENTER", valign="MIDDLE", borders=BORDA_HEADER),
        fmt(R_CUP_HDR, 2, R_CUP_HDR, 2, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=10, halign="LEFT", valign="MIDDLE", borders=BORDA_HEADER),

        # === DATA ROWS CUPONS (R_CUP_DATA_START..R_CUP_DATA_END) ===
        fmt(R_CUP_DATA_START, 2, R_CUP_DATA_END, 2, fg=CINZA_TEXTO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),
        fmt(R_CUP_DATA_START, 3, R_CUP_DATA_END, 3, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE"),
        fmt(R_CUP_DATA_START, 4, R_CUP_DATA_END, 4, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE", numberFormat={"type": "PERCENT", "pattern": "0%"}),
    ]
    # Zebra cupons em offsets 1, 3, 5 do bloco (preserva padrao 17/19/21 do layout original)
    for i in (1, 3, 5):
        rr = R_CUP_DATA_START + i
        reqs += [
            fmt(rr, 2, rr, 4, bg=CINZA_LEVE),
            fmt(rr, 2, rr, 2, bg=CINZA_LEVE, fg=CINZA_TEXTO, bold=True, size=11, halign="LEFT", valign="MIDDLE"),
            fmt(rr, 3, rr, 3, bg=CINZA_LEVE, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE"),
            fmt(rr, 4, rr, 4, bg=CINZA_LEVE, fg=CINZA_TEXTO, size=11, halign="CENTER", valign="MIDDLE", numberFormat={"type": "PERCENT", "pattern": "0%"}),
        ]

    reqs += [
        # === TOTAL ROW CUPONS (R_CUP_TOTAL) ===
        fmt(R_CUP_TOTAL, 2, R_CUP_TOTAL, 4, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=11, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL),
        fmt(R_CUP_TOTAL, 2, R_CUP_TOTAL, 2, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=11, halign="LEFT", valign="MIDDLE", borders=BORDA_TOTAL),
        fmt(R_CUP_TOTAL, 4, R_CUP_TOTAL, 4, bg=LARANJA_50, fg=LARANJA_500, bold=True, size=11, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL, numberFormat={"type": "PERCENT", "pattern": "0%"}),

        # === FOOTER (R_FOOTER) ===
        merge(R_FOOTER, 2, R_FOOTER, 8),
        fmt(R_FOOTER, 2, R_FOOTER, 8, fg=CINZA_FOOTER, italic=True, size=8, halign="CENTER"),
    ]

    sh.batch_update({"requests": reqs})
    print(f"  -> Aba '{dash_tab}' redesenhada com sucesso (n={n} modalidades, Total@r{R_TOTAL}).")


PROTECTED_TITLES = {
    "Brasília", "Belo Horizonte", "Salvador",
    "Metas SSA", "Metas BH", "Metas BSB",
    "raw_inscritos_brasilia", "raw_inscritos_bh", "raw_inscritos_ssa",
}

DASHBOARD_CONFIGS = {
    "bsb": {
        "label": "Brasília", "raw_tab": "raw_inscritos_brasilia", "dash_tab": "Brasília",
        "has_nubank": True,
        "modalidades": [("5km", "Corrida 5km"), ("10km", "Corrida 10km"), ("15km", "Corrida 15km")],
    },
    "bh": {
        "label": "Belo Horizonte", "raw_tab": "raw_inscritos_bh", "dash_tab": "Belo Horizonte",
        "has_nubank": True,
        "modalidades": [("2km", "Corrida 2km"), ("5km", "Corrida 5km"), ("10km", "Corrida 10km")],
    },
    "ssa": {
        "label": "Salvador", "raw_tab": "raw_inscritos_ssa", "dash_tab": "Salvador",
        "has_nubank": True,
        "modalidades": [
            ("Corrida 3km", "Corrida 3km"), ("Caminhada 3km", "Caminhada 3km"),
            ("Corrida 5km", "Corrida 5km"), ("Corrida 10km", "Corrida 10km"),
        ],
    },
    "pedalx": {
        "label": "Brasília", "raw_tab": "raw_inscritos_pedalx", "dash_tab": "Pedal X Road",
        "has_nubank": False, "modalidades": [("Inscreva-se", "Inscreva-se")],
        "title": "PEDAL X ROAD — BRASÍLIA",
        "breakdown_title": "INSCRITOS POR MODALIDADE E CATEGORIA",
        "kit_columns": [{"header": "Pedal X", "criteria": [("B", "Pedal X")]}],
        "coupon_buckets": [
            {"label": "Sem cupom", "criteria": [("F", ""), ("A", "<>")]},
            {"label": "Outros", "residual": True},
        ],
        "timestamp_cell": "F2", "protected_titles": PROTECTED_TITLES,
        "allowed_dash_tabs": {"Pedal X Road"},
        "expected_modalidades": {"Inscreva-se"},
        "expected_categorias": {"Pedal X"},
        "accepted_statuses": {"Pago", "Cortesia"},
    },
}


def _column_letter(index):
    value = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        value = chr(65 + remainder) + value
    return value


def _sheet_ref(title):
    return title if re.fullmatch(r"[A-Za-z0-9_]+", title) else "'" + title.replace("'", "''") + "'"


def _countifs(raw_ref, criteria):
    pieces = []
    for column, value in criteria:
        pieces += [f"{raw_ref}!{column}:{column}", '"' + str(value).replace('"', '""') + '"']
    return "=COUNTIFS(" + ";".join(pieces) + ")"


def _fmt(sid, r1, c1, r2, c2, *, bg=None, fg=None, bold=None, italic=None,
         size=None, halign=None, valign=None, borders=None, number_format=None):
    cell = {}
    text = {"fontFamily": "Calibri"}
    if bg is not None: cell["backgroundColor"] = bg
    if fg is not None: text["foregroundColor"] = fg
    if bold is not None: text["bold"] = bold
    if italic is not None: text["italic"] = italic
    if size is not None: text["fontSize"] = size
    cell["textFormat"] = text
    if halign is not None: cell["horizontalAlignment"] = halign
    if valign is not None: cell["verticalAlignment"] = valign
    if borders is not None: cell["borders"] = borders
    if number_format is not None: cell["numberFormat"] = number_format
    return {"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": r1 - 1, "endRowIndex": r2,
                  "startColumnIndex": c1 - 1, "endColumnIndex": c2},
        "cell": {"userEnteredFormat": cell}, "fields": "userEnteredFormat",
    }}


def _merge(sid, r1, c1, r2, c2):
    return {"mergeCells": {
        "range": {"sheetId": sid, "startRowIndex": r1 - 1, "endRowIndex": r2,
                  "startColumnIndex": c1 - 1, "endColumnIndex": c2},
        "mergeType": "MERGE_ALL",
    }}


def _dimension(sid, dimension, start, end, size):
    return {"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": dimension,
                  "startIndex": start - 1, "endIndex": end},
        "properties": {"pixelSize": size}, "fields": "pixelSize",
    }}


def _validate_custom(sh, raw_tab, dash_tab, modalidades, kit_columns,
                     coupon_buckets, timestamp_cell, protected_titles,
                     allowed_dash_tabs, expected_modalidades,
                     expected_categorias, accepted_statuses):
    if not modalidades or not all(len(item) == 2 for item in modalidades):
        raise ValueError("modalidades deve conter pares (label, valor exato)")
    if not kit_columns:
        raise ValueError("kit_columns não pode ser vazio")
    residuals = [i for i, item in enumerate(coupon_buckets or ()) if item.get("residual")]
    if residuals != [len(coupon_buckets) - 1]:
        raise ValueError("coupon_buckets exige um residual na última posição")
    if (dash_tab == raw_tab or dash_tab in set(protected_titles or ()) or
            dash_tab not in set(allowed_dash_tabs or ())):
        raise ValueError(f"dashboard protegido ou inválido: {dash_tab}")
    if not re.fullmatch(r"[A-Z]+[1-9][0-9]*", timestamp_cell):
        raise ValueError(f"timestamp_cell inválida: {timestamp_cell}")
    for spec in list(kit_columns) + [x for x in coupon_buckets if not x.get("residual")]:
        criteria = spec.get("criteria")
        if not criteria or any(col not in set("ABCDEFGHIJKLMN") for col, _ in criteria):
            raise ValueError(f"criteria inválido: {spec}")
    try:
        raw_ws = sh.worksheet(raw_tab)
    except gspread.exceptions.WorksheetNotFound as exc:
        raise ValueError(f"raw obrigatória não encontrada: {raw_tab}") from exc
    values = raw_ws.get_all_values()
    expected_header = [
        "N inscricao", "Categoria", "Modalidade", "Sexo", "Status do pedido", "Cupom",
    ]
    if not values or values[0][:6] != expected_header:
        raise ValueError(f"header inesperado em {raw_tab}")
    rows = [row + [""] * (6 - len(row)) for row in values[1:] if any(row)]
    observed_modalidades = {row[2] for row in rows if row[2]}
    observed_categorias = {row[1] for row in rows if row[1]}
    observed_statuses = {row[4] for row in rows if row[4]}
    checks = (
        ("modalidades", observed_modalidades, set(expected_modalidades or ())),
        ("categorias", observed_categorias, set(expected_categorias or ())),
        ("status", observed_statuses, set(accepted_statuses or ())),
    )
    for label, observed, allowed in checks:
        if not observed or not observed.issubset(allowed):
            raise ValueError(f"raw {label} fora do contrato: {sorted(observed)}")


def _build_custom_dashboard(sh, label, raw_tab, dash_tab, has_nubank, modalidades, *,
                            title, breakdown_title, kit_columns, coupon_buckets,
                            timestamp_cell, protected_titles, allowed_dash_tabs,
                            expected_modalidades, expected_categorias,
                            accepted_statuses):
    _validate_custom(sh, raw_tab, dash_tab, modalidades, kit_columns,
                     coupon_buckets, timestamp_cell, protected_titles,
                     allowed_dash_tabs, expected_modalidades,
                     expected_categorias, accepted_statuses)
    n = len(modalidades)
    total_col = 3 + len(kit_columns)
    r_start, r_end = 9, 8 + n
    r_total, r_cup_sec, r_cup_hdr = r_end + 1, r_end + 3, r_end + 4
    r_cup_start = r_end + 5
    r_cup_end = r_cup_start + len(coupon_buckets) - 1
    r_cup_total, r_footer, r_max = r_cup_end + 1, r_cup_end + 3, r_cup_end + 5
    try:
        dash = sh.worksheet(dash_tab)
    except gspread.exceptions.WorksheetNotFound:
        dash = sh.add_worksheet(title=dash_tab, rows=max(50, r_max + 10), cols=12)
        print(f"  Aba '{dash_tab}' criada.")
    sid = dash.id
    dash.clear()
    sh.batch_update({"requests": [
        {"unmergeCells": {"range": {"sheetId": sid, "startRowIndex": 0,
                                     "endRowIndex": max(40, r_max),
                                     "startColumnIndex": 0, "endColumnIndex": 11}}},
        {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 0,
                                   "endRowIndex": max(40, r_max),
                                   "startColumnIndex": 0, "endColumnIndex": 11},
                        "cell": {"userEnteredFormat": {}}, "fields": "userEnteredFormat"}},
        {"updateSheetProperties": {"properties": {"sheetId": sid,
          "gridProperties": {"hideGridlines": True}, "tabColor": LARANJA_500},
          "fields": "gridProperties.hideGridlines,tabColor"}},
    ]})

    raw = _sheet_ref(raw_tab)
    total = f"COUNTA({raw}!A:A)-1"
    cells = [
        {"range": "B1", "values": [[title]]},
        {"range": "B2", "values": [["Atualizado automaticamente a cada 1 hora"]]},
        {"range": timestamp_cell, "values": [[datetime.now().strftime("%d/%m/%Y %H:%M")]]},
        {"range": "B4", "values": [["TOTAL INSCRITOS"]]},
        {"range": "B5", "values": [[f"={total}"]]},
        {"range": "D4", "values": [["FEMININO"]]},
        {"range": "D5", "values": [[f'=COUNTIF({raw}!D:D;"F")&"  ("&TEXTO(IFERROR(COUNTIF({raw}!D:D;"F")/({total});0);"0%")&")"']]},
        {"range": "F4", "values": [["MASCULINO"]]},
        {"range": "F5", "values": [[f'=COUNTIF({raw}!D:D;"M")&"  ("&TEXTO(IFERROR(COUNTIF({raw}!D:D;"M")/({total});0);"0%")&")"']]},
        {"range": "B7", "values": [[breakdown_title]]},
        {"range": f"B8:{_column_letter(total_col)}8",
         "values": [[""] + [x["header"] for x in kit_columns] + ["Total"]]},
    ]
    if has_nubank:
        cells += [
            {"range": "H4", "values": [["% CUPOM NUBANK"]]},
            {"range": "H5", "values": [[f'=TEXTO(IFERROR(COUNTIF({raw}!F:F;"*NUBANK*")/({total});0);"0%")']]},
        ]
    for offset, (display, exact) in enumerate(modalidades):
        row = r_start + offset
        cells.append({"range": f"B{row}", "values": [[display]]})
        for kit_offset, spec in enumerate(kit_columns):
            col = 3 + kit_offset
            cells.append({"range": f"{_column_letter(col)}{row}",
                          "values": [[_countifs(raw, [("C", exact)] + spec["criteria"])]]})
        cells.append({"range": f"{_column_letter(total_col)}{row}",
                      "values": [[f"=SOMA(C{row}:{_column_letter(total_col - 1)}{row})"]]})
    cells.append({"range": f"B{r_total}", "values": [["Total"]]})
    for col in range(3, total_col + 1):
        letter = _column_letter(col)
        cells.append({"range": f"{letter}{r_total}",
                      "values": [[f"=SOMA({letter}{r_start}:{letter}{r_end})"]]})
    cells += [
        {"range": f"B{r_cup_sec}", "values": [["INSCRIÇÕES POR TIPO DE CUPOM"]]},
        {"range": f"B{r_cup_hdr}:D{r_cup_hdr}", "values": [["Tipo", "Qtd", "%"]]},
    ]
    for offset, bucket in enumerate(coupon_buckets):
        row = r_cup_start + offset
        formula = (f"={total}-SOMA(C{r_cup_start}:C{row - 1})"
                   if bucket.get("residual") else _countifs(raw, bucket["criteria"]))
        cells += [
            {"range": f"B{row}", "values": [[bucket["label"]]]},
            {"range": f"C{row}", "values": [[formula]]},
            {"range": f"D{row}", "values": [[f'=IFERROR(C{row}/({total});0)']]},
        ]
    cells += [
        {"range": f"B{r_cup_total}", "values": [["Total"]]},
        {"range": f"C{r_cup_total}", "values": [[f"=SOMA(C{r_cup_start}:C{r_cup_end})"]]},
        {"range": f"D{r_cup_total}", "values": [[f'=IFERROR(C{r_cup_total}/({total});0)']]},
        {"range": f"B{r_footer}", "values": [[
            f"Fonte: API Ticketsports  •  Sync cada 1h via GitHub Actions  •  Dados brutos na aba {raw_tab}"
        ]]},
    ]
    dash.batch_update(cells, value_input_option="USER_ENTERED")

    reqs = [
        _dimension(sid, "COLUMNS", 1, 1, 20), _dimension(sid, "COLUMNS", 2, 2, 140),
        _dimension(sid, "COLUMNS", 3, 8, 100), _dimension(sid, "COLUMNS", 9, 9, 20),
        _fmt(sid, 1, 1, r_max, 9, bg=BRANCO),
        _merge(sid, 1, 2, 1, 8), _merge(sid, 2, 2, 2, 5), _merge(sid, 2, 6, 2, 8),
        _fmt(sid, 1, 1, 2, 9, bg=ESCURO),
        _fmt(sid, 1, 2, 1, 8, bg=ESCURO, fg=BRANCO, bold=True, size=16,
             halign="CENTER", valign="MIDDLE"),
        _fmt(sid, 2, 2, 2, 5, bg=ESCURO, fg=LARANJA_200, italic=True, size=9,
             halign="CENTER", valign="MIDDLE"),
        _fmt(sid, 2, 6, 2, 8, bg=ESCURO, fg=BRANCO, size=9,
             halign="CENTER", valign="MIDDLE",
             number_format={"type": "DATE_TIME", "pattern": "dd/mm/yyyy hh:mm"}),
    ]
    for c1, c2, color, size in ((2, 3, LARANJA_500, 32), (4, 5, LARANJA_400, 18),
                                (6, 7, LARANJA_400, 18)):
        reqs += [
            _merge(sid, 4, c1, 4, c2), _merge(sid, 5, c1, 5, c2),
            _fmt(sid, 4, c1, 4, c2, fg=CINZA_TEXTO, bold=True, size=9, halign="CENTER"),
            _fmt(sid, 5, c1, 5, c2, fg=color, bold=True, size=size,
                 halign="CENTER", valign="MIDDLE"),
        ]
    reqs += [
        _merge(sid, 7, 2, 7, 8),
        _fmt(sid, 7, 2, 7, 8, bg=LARANJA_500, fg=BRANCO, bold=True, size=11,
             halign="LEFT", valign="MIDDLE"),
        _fmt(sid, 8, 2, 8, total_col, bg=LARANJA_50, fg=LARANJA_500, bold=True,
             size=10, halign="CENTER", valign="MIDDLE", borders=BORDA_HEADER),
        _fmt(sid, r_start, 2, r_end, 2, fg=CINZA_TEXTO, bold=True, size=11,
             halign="LEFT", valign="MIDDLE"),
        _fmt(sid, r_start, 3, r_end, total_col, fg=CINZA_TEXTO, size=11,
             halign="CENTER", valign="MIDDLE"),
        _fmt(sid, r_total, 2, r_total, total_col, bg=LARANJA_50, fg=LARANJA_500,
             bold=True, size=11, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL),
        _merge(sid, r_cup_sec, 2, r_cup_sec, 8),
        _fmt(sid, r_cup_sec, 2, r_cup_sec, 8, bg=LARANJA_500, fg=BRANCO, bold=True,
             size=11, halign="LEFT", valign="MIDDLE"),
        _fmt(sid, r_cup_hdr, 2, r_cup_hdr, 4, bg=LARANJA_50, fg=LARANJA_500,
             bold=True, size=10, halign="CENTER", valign="MIDDLE", borders=BORDA_HEADER),
        _fmt(sid, r_cup_start, 2, r_cup_end, 4, fg=CINZA_TEXTO, size=11,
             halign="CENTER", valign="MIDDLE"),
        _fmt(sid, r_cup_start, 4, r_cup_end, 4, fg=CINZA_TEXTO, size=11,
             halign="CENTER", number_format={"type": "PERCENT", "pattern": "0%"}),
        _fmt(sid, r_cup_total, 2, r_cup_total, 4, bg=LARANJA_50, fg=LARANJA_500,
             bold=True, size=11, halign="CENTER", valign="MIDDLE", borders=BORDA_TOTAL),
        _merge(sid, r_footer, 2, r_footer, 8),
        _fmt(sid, r_footer, 2, r_footer, 8, fg=CINZA_FOOTER, italic=True, size=8,
             halign="CENTER"),
    ]
    sh.batch_update({"requests": reqs})
    print(f"  -> Aba '{dash_tab}' criada/atualizada com sucesso.")


def build_dashboard(sh, label, raw_tab, dash_tab, has_nubank=True, modalidades=None, *,
                    title=None, breakdown_title=None, kit_columns=None,
                    coupon_buckets=None, timestamp_cell="C2", protected_titles=None,
                    allowed_dash_tabs=None, expected_modalidades=None,
                    expected_categorias=None, accepted_statuses=None):
    custom = any(x is not None for x in
                 (title, breakdown_title, kit_columns, coupon_buckets)) or timestamp_cell != "C2"
    if not custom:
        return _build_legacy_dashboard(sh, label, raw_tab, dash_tab,
                                       has_nubank=has_nubank, modalidades=modalidades)
    return _build_custom_dashboard(
        sh, label, raw_tab, dash_tab, has_nubank, modalidades,
        title=title, breakdown_title=breakdown_title, kit_columns=kit_columns,
        coupon_buckets=coupon_buckets, timestamp_cell=timestamp_cell,
        protected_titles=protected_titles, allowed_dash_tabs=allowed_dash_tabs,
        expected_modalidades=expected_modalidades,
        expected_categorias=expected_categorias, accepted_statuses=accepted_statuses,
    )


def _run_config(sh, key):
    config = dict(DASHBOARD_CONFIGS[key])
    label, raw_tab, dash_tab = config.pop("label"), config.pop("raw_tab"), config.pop("dash_tab")
    build_dashboard(sh, label, raw_tab, dash_tab, **config)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Builder dos dashboards TicketSports")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--event", choices=sorted(DASHBOARD_CONFIGS))
    group.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        parser.error("SPREADSHEET_ID é obrigatório")
    service_account_file = os.environ.get(
        "GOOGLE_SERVICE_ACCOUNT_FILE",
        r"C:\Users\bruno\.brada-secrets\sheets-sa.json",
    )
    creds = Credentials.from_service_account_file(
        service_account_file,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"],
    )
    sh = gspread.authorize(creds).open_by_key(spreadsheet_id)
    for key in (list(DASHBOARD_CONFIGS) if args.all else [args.event]):
        _run_config(sh, key)


if __name__ == "__main__":
    main()
