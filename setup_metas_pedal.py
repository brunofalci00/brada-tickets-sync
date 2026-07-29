"""
Build das abas de META de eventos sem .xlsx de origem, no Dashboard_Inscricoes_VaiBem.

Cria "Metas Pedal Road", "Metas Pedal Manaus", "Metas Pedal Canastra" e "Metas Circuito
Santos" com o mesmo layout, formatacao, formatacao condicional e grafico das abas de meta
das corridas (Metas SSA/BH/BSB), mas montadas a partir de config declarativa.

Apesar do nome, o script nao e mais so de pedal: o Circuito Santos usa o mesmo builder.

POR QUE UM SCRIPT NOVO (e nao estender setup_metas_native.py): aquele e um TRANSCRITOR do
.xlsx da Tamyris e recria as 3 abas de corrida por delete+recreate. Nao tem fonte para pedal
e mexer nele arrisca producao. Aqui o layout e gerado; os helpers puros sao reaproveitados.

DIFERENCAS DELIBERADAS EM RELACAO AS CORRIDAS:
  - Coluna D (Acumulado) fica VISIVEL (nas corridas o build esconde). Pedido do Bruno.
  - Bloco "Meta por tier" tem UMA linha de tier: estes eventos tem preco unico (pedais R$139,
    Santos R$70, sempre + R$10 de taxa). Essa linha vai SEM meta de proposito — quem carrega a
    meta e a linha "Total Pago". Com meta nas duas, o bloco somaria o dobro e o painel
    repetiria o mesmo numero. Sem meta, ela vale como cross-check de que G continua igual a E.
  - Bloco "Metas Gratuitas": Meta em branco nos pedais (nao ha inscricao gratuita neles).
    Em Santos a meta e 1000, com duas linhas de RATEIO abaixo (600 garantidos pelo
    patrocinador Asia Shipping + 400 a conquistar). Ver `rateio_gratuitas` na CONFIGS.

SEGURANCA: guard dinamico. Le as abas existentes e aborta se qualquer request mirar um
sheetId que nao seja uma das abas-alvo recem-criadas — aba nova criada depois ja nasce
protegida, sem precisar manter lista fixa.
Aba existente NAO e sobrescrita sem --force: a coluna C (Meta) e editavel pela Tamyris e
pelo Gui, e recriar por engano apagaria o ajuste.

REGUA SEMANAL: de (1a venda -> ultimo dia de venda), 7 dias por semana, ultima podendo ser curta.
`GET /Event/{id}` nao expoe a janela de vendas (nao ha dataInicioVendas/dataFimVendas), mas o
prazo E publico: o endpoint do site `www.ticketsports.com.br/api/events/list?term=<nome>`
devolve `signUpDeadLine` em JSON limpo (achado 29/07 — antes disso so se lia renderizando a
SPA). O prazo real fica em `fim` na CONFIGS abaixo; nao derivar da data do evento (medido em
28/07: as 3 lojas de pedal fecham ~6 dias antes, nao na vespera).

LOCALE (provado por probe 22/06): a planilha parseia formula em pt_BR -> separador de
argumento e ';' (virgula da #ERROR). Vale para SUMIF/IF/OR/AND. Formulas sem separador
(=C2, =D2+C3) sao imunes.

Uso:  python setup_metas_pedal.py --all --dry-run
      python setup_metas_pedal.py --event pedalx_road
      python setup_metas_pedal.py --event pedalx_road --force   # recria aba existente
"""
# NOTA SOBRE O CIRCUITO SANTOS (87817), valida enquanto o acesso nao chegar:
# o evento e do CNPJ da EGP BRASIL, nao da Brada, e `GET /Order/List` e escopado por
# organizador — devolve 204 vazio. Logo o cron NAO preenche essa aba (a guarda de
# write_metas_native pula etapa ausente de participants_por_cidade), e a coluna E fica
# vazia. Consequencia visivel: o grafico nasce com UMA serie so (o Sheets omite serie de
# fonte inteiramente vazia — DOCUMENTACAO_TECNICA §16.5). Diferente dos pedais, isso NAO
# se auto-cura no primeiro run: so volta a duas series quando o acesso for concedido.
import argparse
import os
from datetime import date, timedelta

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Helpers puros reaproveitados do build das corridas (o modulo nao tem efeito colateral em
# import: tudo que executa esta sob `if __name__`).
from setup_metas_native import chart_request, col_idx, hexcol

SA_FILE = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE") or r"C:\Users\bruno\.brada-secrets\sheets-sa.json"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID") or "1KfTWNTDoWUok-yn_gGlZaJk_lhPmq9RXOh793mZomFA"

META_TOTAL = 500          # pagas por etapa de pedal (Tamyris, call de 28/07). Sobrescrevivel
                          # por evento com `meta_total` na CONFIGS (Santos usa 1000).
LABEL_TIER = "Inscrição R$139 (tier único)"   # default dos pedais; ver `label_tier`
N_COLS = 20               # A..T (o painel lateral vai ate T)

LARANJA = hexcol("C55A11")
ZEBRA = hexcol("FBE5D6")
DARK = hexcol("333333")
WHITE = hexcol("FFFFFF")
BORDA = hexcol("D9D9D9")
VERDE = hexcol("C6EFCE")
VERMELHO = hexcol("EA9999")

NF_INT = {"type": "NUMBER", "pattern": "#,##0"}
NF_PCT = {"type": "PERCENT", "pattern": "0%"}
NF_DATE = {"type": "DATE", "pattern": "dd/mm/yyyy"}

# Larguras em px, iguais as da aba Metas SSA ao vivo. M e a calha entre a tabela e o painel.
WIDTHS = {"A": 164, "B": 117, "C": 131, "D": 89, "E": 89, "F": 75, "G": 96, "H": 96,
          "I": 93, "J": 68, "K": 96, "M": 26, "N": 103, "O": 82, "P": 68, "Q": 68,
          "R": 68, "S": 68, "T": 68}

HEADERS = ["Semana", "Período", "Meta", "Acumulado", "Realizado", "Gap",
           "Real. Básico", "Real. Premium", "Real. Combo", "Real. PCD", "Real. Gratuito"]

# `inicio`  = data da 1a venda real, apurada em GET /Order/List.
# `fim`     = ULTIMO DIA DE VENDA. Nao e derivado da data do evento: e o "Inscrições até" que a
#             pagina publica da loja exibe, lido em 28/07. `GET /Event/{id}` nao traz esse campo,
#             mas a pagina renderizada traz — e as tres fecham ~6 dias antes do evento, nao na
#             vespera. Se o prazo mudar no painel, atualizar aqui.
# `evento`  = so referencia; nao entra no calculo da regua.
# Opcionais: `meta_total` (default META_TOTAL), `label_tier` (default LABEL_TIER),
#            `meta_gratuitas` (default vazio) e `rateio_gratuitas` (default nenhum).
CONFIGS = {
    "pedalx_road": {
        "event_id": 87735, "tab": "Metas Pedal Road", "sigla": "Pedal Road",
        "inicio": date(2026, 7, 21), "fim": date(2026, 8, 23), "evento": date(2026, 8, 30),
    },
    "pedalx_manaus": {
        "event_id": 87732, "tab": "Metas Pedal Manaus", "sigla": "Pedal Manaus",
        "inicio": date(2026, 7, 27), "fim": date(2026, 9, 13), "evento": date(2026, 9, 19),
    },
    "pedalx_canastra": {
        "event_id": 87727, "tab": "Metas Pedal Canastra", "sigla": "Pedal Canastra",
        "inicio": date(2026, 7, 24), "fim": date(2026, 10, 12), "evento": date(2026, 10, 17),
    },
    # Circuito Santos: `inicio` e HIPOTESE, nao dado. A 1a venda real nao pode ser apurada
    # porque Order/List esta bloqueado (ver nota no topo), e a loja ja tinha 5 confirmados
    # em 29/07 — ou seja, a venda comecou ANTES. Quando o acesso chegar, conferir a 1a venda
    # e corrigir EDITANDO as linhas de semana; nao recriar com --force, que apagaria ajuste
    # manual da coluna C. `fim` = signUpDeadLine lido no JSON publico em 29/07.
    "circuito_santos": {
        "event_id": 87817, "tab": "Metas Circuito Santos", "sigla": "Circuito Santos",
        "inicio": date(2026, 7, 29), "fim": date(2026, 9, 14), "evento": date(2026, 9, 20),
        "meta_total": 1000,
        "label_tier": "Inscrição R$70 (tier único)",
        "meta_gratuitas": 1000,
        # Rateio de PLANEJAMENTO. A plataforma nao distingue cortesia do patrocinador de
        # cortesia que a Brada foi buscar: ambas caem em "KIT ATLETA - CORTESIAS". Por isso
        # essas linhas carregam so a meta; o Realizado fica na linha do total, que e a unica
        # que o cron escreve (_find_gratuitas_native usa hdr+1).
        "rateio_gratuitas": [("Asia Shipping (garantido)", 600), ("A conquistar", 400)],
    },
}


# ----------------------------- regua semanal -----------------------------

def semanas(inicio, fim, meta_total=META_TOTAL):
    """[(rotulo, 'DD/MM - DD/MM', meta), ...] de `inicio` ate `fim` (ultimo dia de venda).

    Semanas de 7 dias; a ultima pode ser curta. Os limites se sobrepoem de proposito
    ('21/07 - 28/07' seguido de '28/07 - 04/08'), como nas corridas: o corte e cumulativo
    pela data FINAL e o valor semanal sai de cum(fim) - cum(fim_anterior), entao a venda do
    dia 28 cai na semana 0 e ja esta no prev_cum quando a semana 1 e calculada.

    Meta dividida por igual, com o resto nas primeiras semanas para a soma fechar exata.
    """
    if fim <= inicio:
        raise ValueError(f"janela invalida: inicio {inicio} >= fim {fim}")
    bounds = [inicio]
    while bounds[-1] + timedelta(days=7) < fim:
        bounds.append(bounds[-1] + timedelta(days=7))
    bounds.append(fim)

    n = len(bounds) - 1
    base, resto = divmod(meta_total, n)
    metas = [base + 1] * resto + [base] * (n - resto)
    return [(f"Semana {k}",
             f"{bounds[k].strftime('%d/%m')} - {bounds[k + 1].strftime('%d/%m')}",
             metas[k])
            for k in range(n)]


def serial(d):
    """date -> serial do Sheets (dias desde 1899-12-30)."""
    return (d - date(1899, 12, 30)).days


# ----------------------------- celulas -----------------------------

def cell(value=None, formula=None, bg=None, bold=False, fg=None, nf=None,
         halign=None, valign=None, borders=True, wrap=False):
    """CellData com userEnteredValue + userEnteredFormat."""
    cd = {}
    if formula is not None:
        cd["userEnteredValue"] = {"formulaValue": formula}
    elif isinstance(value, bool):
        cd["userEnteredValue"] = {"boolValue": value}
    elif isinstance(value, (int, float)):
        cd["userEnteredValue"] = {"numberValue": value}
    elif value not in (None, ""):
        cd["userEnteredValue"] = {"stringValue": str(value)}

    fmt = {}
    if bg:
        fmt["backgroundColor"] = bg
    tf = {}
    if bold:
        tf["bold"] = True
    tf["foregroundColor"] = fg or DARK
    fmt["textFormat"] = tf
    if nf:
        fmt["numberFormat"] = nf
    if halign:
        fmt["horizontalAlignment"] = halign
    if valign:
        fmt["verticalAlignment"] = valign
    if wrap:
        fmt["wrapStrategy"] = "WRAP"
    if borders:
        fmt["borders"] = {s: {"style": "SOLID", "color": BORDA}
                          for s in ("top", "bottom", "left", "right")}
    cd["userEnteredFormat"] = fmt
    return cd


def _blank_grid(n_rows):
    return [[{} for _ in range(N_COLS)] for _ in range(n_rows)]


def _put(grid, row, col_letter, cd):
    grid[row - 1][col_idx(col_letter)] = cd


# ----------------------------- layout -----------------------------

def build_layout(cfg):
    """Monta (grid, marcos) da aba. `marcos` traz as linhas 1-based que o painel e a
    formatacao condicional precisam referenciar."""
    meta_total = cfg.get("meta_total", META_TOTAL)
    rateio = cfg.get("rateio_gratuitas") or []
    linhas = semanas(cfg["inicio"], cfg["fim"], meta_total)
    n = len(linhas)

    r_wk0, r_wkN = 2, n + 1
    r_tier_hdr = n + 3
    r_tier = n + 4
    r_total = n + 5
    r_grat_titulo = n + 7
    r_grat_hdr = n + 8
    r_grat = n + 9
    # As linhas de rateio ficam ABAIXO da do cron, que precisa continuar sendo hdr+1.
    last_row = r_grat + len(rateio)

    grid = _blank_grid(last_row)

    # --- cabecalho da tabela semanal
    for j, h in enumerate(HEADERS):
        _put(grid, 1, chr(65 + j), cell(h, bg=LARANJA, bold=True, fg=WHITE,
                                        halign="CENTER", valign="MIDDLE"))

    # --- semanas. Zebra nas linhas pares, mas NUNCA em E e F: essas duas colunas ficam sem
    # fundo de proposito porque a formatacao condicional e dona delas (conferido na Metas SSA).
    for k, (rotulo, periodo, meta) in enumerate(linhas):
        r = r_wk0 + k
        z = ZEBRA if r % 2 == 0 else None
        _put(grid, r, "A", cell(rotulo, bg=z, halign="LEFT"))
        _put(grid, r, "B", cell(periodo, bg=z, halign="LEFT"))
        _put(grid, r, "C", cell(meta, bg=z, nf=NF_INT, halign="CENTER"))
        acum = f"=C{r}" if k == 0 else f"=D{r - 1}+C{r}"
        _put(grid, r, "D", cell(formula=acum, bg=z, nf=NF_INT, halign="CENTER"))
        for letra in ("E", "F"):
            _put(grid, r, letra, cell(nf=NF_INT, halign="CENTER"))
        for letra in ("G", "H", "I", "J", "K"):
            _put(grid, r, letra, cell(bg=z, nf=NF_INT, halign="CENTER"))

    # --- bloco de tier
    for letra, texto in (("A", "Meta por tier"), ("B", "Meta"), ("C", "Realizado"), ("E", "Gap")):
        _put(grid, r_tier_hdr, letra, cell(texto, bg=ZEBRA, bold=True, halign="CENTER"))
    # Linha de tier SEM meta: ver docstring do modulo.
    _put(grid, r_tier, "A", cell(cfg.get("label_tier", LABEL_TIER), halign="LEFT"))
    _put(grid, r_tier, "B", cell(nf=NF_INT))
    _put(grid, r_tier, "C", cell(formula="=SUM(G:G)", nf=NF_INT))
    _put(grid, r_tier, "E", cell(formula=f'=IF($B{r_tier}="";"";$B{r_tier}-$C{r_tier})', nf=NF_INT))

    _put(grid, r_total, "A", cell("Total Pago", halign="LEFT"))
    _put(grid, r_total, "B", cell(meta_total, nf=NF_INT))
    _put(grid, r_total, "C", cell(formula='=SUMIF($A:$A;"Semana*";$E:$E)', nf=NF_INT))
    _put(grid, r_total, "E", cell(formula=f'=IF($B{r_total}="";"";$B{r_total}-$C{r_total})', nf=NF_INT))

    # --- bloco de gratuitas. Meta em branco (nao ha inscricao gratuita nos pedais hoje);
    # o cron acha esta secao pelo header "Inicio Monitoramento" e preenche C (Realizado) e D (Gap).
    _put(grid, r_grat_titulo, "A", cell("Metas Gratuitas", bold=True, borders=False))
    for letra, texto in (("A", "Início Monitoramento"), ("B", "Meta Gratuitas"),
                         ("C", "Realizado"), ("D", "Gap"), ("E", "Observação")):
        _put(grid, r_grat_hdr, letra, cell(texto, bg=LARANJA, bold=True, fg=WHITE,
                                           halign="CENTER", valign="MIDDLE"))
    _put(grid, r_grat, "A", cell(serial(cfg["inicio"]), nf=NF_DATE))
    _put(grid, r_grat, "B", cell(cfg.get("meta_gratuitas"), nf=NF_INT))
    _put(grid, r_grat, "C", cell(nf=NF_INT))
    _put(grid, r_grat, "D", cell(nf=NF_INT))
    _put(grid, r_grat, "E", cell("Total de cortesias" if rateio else "Monitorar cortesias"))

    # Rateio: so META. O Realizado nao se divide porque a plataforma nao marca a origem da
    # cortesia — o numero real vive na linha acima, que e a que o cron escreve.
    for k, (rotulo, meta) in enumerate(rateio):
        r = r_grat + 1 + k
        z = ZEBRA if r % 2 == 0 else None
        _put(grid, r, "A", cell(rotulo, bg=z, halign="LEFT"))
        _put(grid, r, "B", cell(meta, bg=z, nf=NF_INT, halign="CENTER"))
        _put(grid, r, "E", cell("Não separável na plataforma", bg=z))

    # --- painel lateral (N:T), linhas FIXAS como nas corridas
    _put(grid, 2, "N", cell(f'Painel {cfg["sigla"]}', bg=LARANJA, bold=True, fg=WHITE,
                            halign="CENTER", valign="MIDDLE"))
    _put(grid, 3, "N", cell("Realizado", bold=True))
    _put(grid, 3, "O", cell(formula='=SUMIF($A:$A;"Semana*";$E:$E)', bold=True,
                            nf=NF_INT, halign="CENTER"))
    _put(grid, 4, "N", cell("Meta total"))
    _put(grid, 4, "O", cell(formula=f'=IF($B${r_total}="";"";$B${r_total})',
                            nf=NF_INT, halign="CENTER"))
    _put(grid, 5, "N", cell("% atingido"))
    _put(grid, 5, "O", cell(formula='=IF(OR($O$4="";$O$4=0);"";$O$3/$O$4)',
                            nf=NF_PCT, halign="CENTER"))
    _put(grid, 6, "N", cell("Gap"))
    _put(grid, 6, "O", cell(formula='=IF($O$4="";"";$O$4-$O$3)', nf=NF_INT, halign="CENTER"))

    _put(grid, 8, "N", cell("Progresso por tier", bg=ZEBRA, bold=True))
    for i, (rotulo, ref) in enumerate((("Inscrição", r_tier), ("Total", r_total),
                                       ("Gratuitas", r_grat))):
        r = 9 + i
        _put(grid, r, "N", cell(rotulo))
        _put(grid, r, "O", cell(formula=f"=$C${ref}", nf=NF_INT, halign="CENTER"))
        _put(grid, r, "P", cell(formula=f'=IF($B${ref}="";"—";$B${ref})',
                                nf=NF_INT, halign="CENTER"))
        _put(grid, r, "Q", cell(formula=f'=IF(OR($B${ref}="";$B${ref}=0);"";$C${ref}/$B${ref})',
                                nf=NF_PCT, halign="CENTER"))

    marcos = {"n": n, "wk0": r_wk0, "wkN": r_wkN, "tier_hdr": r_tier_hdr, "tier": r_tier,
              "total": r_total, "grat": r_grat, "rateio": len(rateio), "last": last_row}
    return grid, marcos


def _cf(sheet_id, r0, r1, c0, c1, formula, color, index):
    """addConditionalFormatRule com indices 0-based e fim exclusivo."""
    return {"addConditionalFormatRule": {"index": index, "rule": {
        "ranges": [{"sheetId": sheet_id, "startRowIndex": r0, "endRowIndex": r1,
                    "startColumnIndex": c0, "endColumnIndex": c1}],
        "booleanRule": {
            "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": formula}]},
            "format": {"backgroundColor": color}}}}}


def build_requests(sheet_id, cfg):
    """Requests da CALL 2: conteudo, merges, larguras, altura, CF e grafico."""
    grid, m = build_layout(cfg)
    req = [{"updateCells": {
        "start": {"sheetId": sheet_id, "rowIndex": 0, "columnIndex": 0},
        "rows": [{"values": row} for row in grid],
        "fields": "userEnteredValue,userEnteredFormat"}}]

    # banners do painel ocupam N..T
    for r0 in (1, 7):
        req.append({"mergeCells": {"range": {
            "sheetId": sheet_id, "startRowIndex": r0, "endRowIndex": r0 + 1,
            "startColumnIndex": 13, "endColumnIndex": 20}, "mergeType": "MERGE_ALL"}})

    # larguras. A coluna D fica VISIVEL aqui (diferente das corridas).
    for letra, px in WIDTHS.items():
        i = col_idx(letra)
        req.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": px}, "fields": "pixelSize"}})
    req.append({"updateDimensionProperties": {
        "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": 30}, "fields": "pixelSize"}})

    # formatacao condicional: verde bate a meta, vermelho nao bate. Tres pares, iguais aos
    # das corridas. Formula ancorada na primeira linha de cada faixa.
    faixas = [
        (m["wk0"] - 1, m["wkN"], 4, 6, f'$C{m["wk0"]}', f'$E{m["wk0"]}'),          # semanal: E:F
        (m["tier_hdr"], m["total"], 2, 5, f'$B{m["tier"]}', f'$C{m["tier"]}'),      # tier: C:E
        (m["grat"] - 1, m["grat"], 2, 4, f'$B{m["grat"]}', f'$C{m["grat"]}'),       # gratuitas: C:D
    ]
    idx = 0
    for r0, r1, c0, c1, meta_ref, real_ref in faixas:
        for op, cor in ((">=", VERDE), ("<", VERMELHO)):
            req.append(_cf(sheet_id, r0, r1, c0, c1,
                           f"=AND(ISNUMBER({meta_ref});ISNUMBER({real_ref});{real_ref}{op}{meta_ref})",
                           cor, idx))
            idx += 1

    # grafico nativo (duravel; nao morre com edicao humana). `wkN` = ultima linha de semana:
    # o helper usa como fim exclusivo do dominio e ancora o grafico em wkN+10, que cai duas
    # linhas abaixo do fim do conteudo (o bloco sob as semanas tem 8 linhas). `extra` desloca
    # a ancora quando o rateio de gratuitas alonga esse bloco, preservando a folga.
    req.append(chart_request(sheet_id, cfg["sigla"], m["wkN"], extra=m["rateio"]))
    return req, m


# ----------------------------- io -----------------------------

def sheets_service():
    creds = Credentials.from_service_account_file(
        SA_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _collect_ids(o, acc):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "sheetId" and isinstance(v, int):
                acc.add(v)
            else:
                _collect_ids(v, acc)
    elif isinstance(o, list):
        for x in o:
            _collect_ids(x, acc)


def guard(requests, permitidos):
    """Aborta se algum request mirar sheetId fora das abas-alvo. Derivado do estado real da
    planilha, entao aba criada no futuro ja nasce protegida sem editar lista nenhuma."""
    acc = set()
    _collect_ids(requests, acc)
    invasores = acc - set(permitidos)
    if invasores:
        raise SystemExit(f"GUARD ABORT: requests mirando abas fora do alvo {sorted(invasores)}")


def reposicionar(svc, titulos):
    """Move as abas recem-criadas para logo depois da ultima aba 'Metas *' que nao seja
    delas. `addSheet` sempre acrescenta no fim, o que jogaria as abas de meta para depois
    das raws ocultas — longe das outras metas, onde o time procura."""
    d = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID,
                               fields="sheets.properties(sheetId,index,title)").execute()
    props = [s["properties"] for s in d["sheets"]]
    ancora = max((p["index"] for p in props
                  if p["title"].startswith("Metas") and p["title"] not in titulos), default=-1)
    ids = {p["title"]: p["sheetId"] for p in props}
    req = [{"updateSheetProperties": {"properties": {"sheetId": ids[t], "index": ancora + 1 + i},
                                      "fields": "index"}}
           for i, t in enumerate(titulos) if t in ids]
    if req:
        svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": req}).execute()
        print(f"  posicionadas a partir do indice {ancora + 1}")


def main(keys, dry=False, force=False):
    if dry:
        for key in keys:
            cfg = CONFIGS[key]
            _grid, m = build_layout(cfg)
            # Reusar a MESMA meta que o layout usou. Recalcular sem `meta_total` fazia o
            # dry-run reportar 500 e metas de 72 numa aba que nasce com 1000 e 143 —
            # relatorio mentiroso e justamente o que um dry-run existe para evitar.
            linhas = semanas(cfg["inicio"], cfg["fim"], cfg.get("meta_total", META_TOTAL))
            req, _ = build_requests(-1, cfg)
            print(f'[DRY] {cfg["tab"]}: {m["n"]} semanas, {len(req)} requests, '
                  f'ultima linha {m["last"]}, soma das metas {sum(x[2] for x in linhas)}')
            for rotulo, periodo, meta in linhas:
                print(f"        {rotulo:<10} {periodo:<15} meta={meta}")
            if m["rateio"]:
                print(f'        rateio de gratuitas: {cfg["rateio_gratuitas"]} '
                      f'(meta total {cfg.get("meta_gratuitas")})')
        print("[DRY] nada enviado.")
        return

    svc = sheets_service()
    meta = svc.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets.properties(sheetId,title)").execute()
    existentes = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}

    req1 = []
    for key in keys:
        titulo = CONFIGS[key]["tab"]
        if titulo in existentes:
            if not force:
                raise SystemExit(
                    f"ABORT: aba '{titulo}' ja existe. A coluna C (Meta) e editada por humano; "
                    f"recriar apagaria o ajuste. Use --force se e isso mesmo que voce quer.")
            req1.append({"deleteSheet": {"sheetId": existentes[titulo]}})
        req1.append({"addSheet": {"properties": {
            "title": titulo,
            "gridProperties": {"rowCount": 60, "columnCount": 26, "frozenRowCount": 1}}}})

    # o guard da CALL 1 so pode liberar os deletes das proprias abas-alvo
    guard(req1, {existentes[CONFIGS[k]["tab"]] for k in keys if CONFIGS[k]["tab"] in existentes})
    rep = svc.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": req1}).execute()
    ids = {p["addSheet"]["properties"]["title"]: p["addSheet"]["properties"]["sheetId"]
           for p in rep["replies"] if "addSheet" in p}
    print("abas criadas:", ids)

    req2, marcos = [], {}
    for key in keys:
        cfg = CONFIGS[key]
        r, m = build_requests(ids[cfg["tab"]], cfg)
        req2 += r
        marcos[cfg["tab"]] = m
    guard(req2, set(ids.values()))
    svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": req2}).execute()
    reposicionar(svc, [CONFIGS[k]["tab"] for k in keys])

    for titulo, m in marcos.items():
        print(f'  {titulo}: {m["n"]} semanas (linhas {m["wk0"]}-{m["wkN"]}), '
              f'Total Pago na linha {m["total"]}, conteudo ate a linha {m["last"]}')
    print(f"BUILD OK: {len(req2)} requests em {len(ids)} abas.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--event", choices=sorted(CONFIGS))
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="recria aba que ja existe (APAGA a coluna Meta preenchida por humano)")
    args = ap.parse_args()
    if not args.event and not args.all:
        raise SystemExit("informe --event <chave> ou --all")
    main(sorted(CONFIGS) if args.all else [args.event], dry=args.dry_run, force=args.force)
