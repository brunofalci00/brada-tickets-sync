"""
Sync Ticketsports -> Google Sheets + Leadlovers (multi-etapa)
Puxa inscricoes da API Ticketsports para todas as etapas configuradas,
escreve em abas raw separadas no Google Sheets, e envia novos inscritos
ao Leadlovers para disparo da regua de email.
Roda via GitHub Actions (cron horario) ou manualmente.
"""

import http.client
import io
import json
import os
import ssl
import time
from datetime import datetime, date, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG TICKETSPORTS
# ===================================================

API_BASE = "api.ticketsports.com.br"
API_VERSION = "/v1.0"
PAGE_LIMIT = 50

# Etapas Corrida Vai Bem. Adicionar nova etapa = nova entrada aqui.
EVENTS = [
    {
        "id": 86595,
        "label": "Brasília",
        "raw_tab": "raw_inscritos_brasilia",
        "dash_tab": "Brasília",
        "ll_sequence_env": "LL_SEQUENCE_BSB",
        "ll_sent_tab": "Etapa Brasilia",
    },
    {
        "id": 86781,
        "label": "Belo Horizonte",
        "raw_tab": "raw_inscritos_bh",
        "dash_tab": "Belo Horizonte",
        "ll_sequence_env": "LL_SEQUENCE_BH",
        "ll_sent_tab": "Etapa BH",
    },
    {
        "id": 87008,
        "label": "Salvador",
        "raw_tab": "raw_inscritos_ssa",
        "dash_tab": "Salvador",
        "ll_sequence_env": "LL_SEQUENCE_SSA",
        "ll_sent_tab": "Etapa Salvador",
    },
]

# Credenciais via variáveis de ambiente (GitHub Secrets) ou arquivo local
TICKET_LOGIN = os.environ.get("TICKET_LOGIN", "marketing@brada.social")
TICKET_PASSWORD = os.environ.get("TICKET_PASSWORD", "102030")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    r"C:\Users\bruno\.brada-secrets\sheets-sa.json",
)

HEADER = [
    "N inscricao", "Categoria", "Modalidade", "Sexo", "Status do pedido",
    "Cupom", "Valor", "Data Pedido", "Dispositivo", "Cidade", "Estado", "Camiseta",
    "Inscricao Grupo", "Nome Grupo",
]

# ===================================================
# CONFIG LEADLOVERS
# ===================================================

LL_API_BASE = "llapi.leadlovers.com"
LL_API_TOKEN = os.environ.get("LL_API_TOKEN", "")
LL_MACHINE_CODE = os.environ.get("LL_MACHINE_CODE", "")
# Sequence code e por etapa: cada cidade tem grupo de WhatsApp diferente,
# entao precisa de sequencia propria. Lido via os.environ[ev["ll_sequence_env"]].

# Planilha separada para logs do Leadlovers (nao misturar com dashboard).
# Cada etapa tem aba propria — nome em EVENTS[i]["ll_sent_tab"].
LL_SPREADSHEET_ID = "1aaDYxjcDhhR2lMLejpOW54QVNGQuSOXc8Gj6q5S2KdA"
LL_SENT_HEADER = ["inscricao", "email", "nome", "data_envio"]


# ===================================================
# CONFIG METAS (planilha semanal da Tamyris — meta_corrida_vai_bem)
# ===================================================

# ID do arquivo .xlsx no Drive (mesmo link que o time usa). Gravacao in-place via Drive API.
# `or` (nao o default do get): secret inexistente no Actions vira string vazia, que sombrearia
# o default; assim vazio/ausente cai no ID hardcoded.
METAS_SPREADSHEET_ID = os.environ.get("METAS_SPREADSHEET_ID") or "1t5xEHgT-g6k9wAWspjXKDMssX0rNYhJS"
# Dry-run: imprime o que escreveria, sem tocar a planilha. METAS_DRY_RUN=1 liga.
METAS_DRY_RUN = os.environ.get("METAS_DRY_RUN", "").strip().lower() not in ("", "0", "false", "no")
CAMPAIGN_YEAR = 2026

# UMA aba por cidade (tabela Pagas semanal + bloco por tier + resumo Gratuitas, tudo junto).
METAS_TABS = {
    86595: "Metas [ BSB ]",
    86781: "Metas [ BH ]",
    87008: "Metas [ SSA ]",
}

# Colunas de detalhamento por tier que a automacao controla nas abas Pagas:
# (header na planilha, chave no dict de contagem).
# `Realizado` (E) ja e o total pago, entao NAO ha coluna "Real. Total Pago" (era duplicata).
META_TIER_COLS = [
    ("Real. Básico", "Básico"),
    ("Real. Premium", "Premium"),
    ("Real. Combo", "Combo"),
    ("Real. PCD", "PCD"),
    ("Real. Gratuito", "Gratuito"),
]


# ===================================================
# RETRY
# ===================================================

def _retry(fn, label, max_tries=3, initial_wait=15):
    """Executa fn, retentando ate max_tries vezes com backoff em caso de falha."""
    wait = initial_wait
    for attempt in range(1, max_tries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_tries:
                raise
            print(f"  [{label}] tentativa {attempt}/{max_tries} falhou: {e}. Aguardando {wait}s...")
            time.sleep(wait)
            wait *= 2


# ===================================================
# API TICKETSPORTS
# ===================================================

def api_request(method, endpoint, headers=None, body=None):
    """Faz request HTTP para a API Ticketsports. Suporta GET com body."""
    def _do():
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection(API_BASE, context=ctx)
        body_str = json.dumps(body) if body else None
        all_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if headers:
            all_headers.update(headers)
        if body_str:
            all_headers["Content-Length"] = str(len(body_str.encode("utf-8")))
        conn.request(method, API_VERSION + endpoint, body=body_str, headers=all_headers)
        resp = conn.getresponse()
        data = resp.read().decode("utf-8")
        conn.close()
        if resp.status == 204 or not data.strip():
            return {}  # sem conteudo (204 ou body vazio — ex: Cortesia com 0 resultados)
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status} em {endpoint}: {data[:300]}")
        return json.loads(data)

    return _retry(_do, endpoint.split("?")[0])


def authenticate():
    """Autentica na API e retorna o Bearer token."""
    def _do():
        body = f"Login={TICKET_LOGIN}&Password={TICKET_PASSWORD}&AccessType=O"
        ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection(API_BASE, context=ctx)
        conn.request(
            "POST",
            API_VERSION + "/Access",
            body=body,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            },
        )
        resp = conn.getresponse()
        data = json.loads(resp.read().decode("utf-8"))
        conn.close()
        if not data.get("access_token"):
            raise Exception(f"Falha na autenticação: {data}")
        return data["access_token"]

    token = _retry(_do, "auth")
    print("Autenticado com sucesso.")
    return token


def fetch_all_orders(token, event_id):
    """Busca todos os pedidos pagos de um evento, paginando. Retorna list[dict]."""
    all_participants = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        endpoint = f"/Order/List?page={page}&limit={PAGE_LIMIT}"
        data = api_request(
            "GET",
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            body={"events": [event_id], "status": ["Pago", "Cortesia"]},
        )

        total_pages = data.get("totalpages", data.get("totalPages", 1))
        orders = data.get("orders", [])

        for order in orders:
            participantes = order.get("participante", [])
            if not isinstance(participantes, list):
                participantes = [participantes]

            for p in participantes:
                camiseta = ""
                produtos = p.get("produtos", [])
                if produtos and produtos[0].get("Camisetas"):
                    camiseta = produtos[0]["Camisetas"]

                cidade = p.get("cidade", "") or order.get("responsavel", {}).get("cidade", "")
                estado = p.get("estado", "") or order.get("responsavel", {}).get("estado", "")

                valor = p.get("valorUnitario", "")
                if isinstance(valor, str):
                    valor = valor.replace(",", ".")

                all_participants.append({
                    "inscricao": p.get("inscricao", ""),
                    "nome": p.get("nome", ""),
                    "email": p.get("email", ""),
                    "celular": p.get("celular", ""),
                    "categoria": p.get("categoria", ""),
                    "modalidade": p.get("modalidade", ""),
                    "sexo": p.get("sexo", ""),
                    "status": order.get("status", ""),
                    "cupom": p.get("tituloCupom", ""),
                    "valor": valor,
                    "dataPedido": order.get("dataPedido", ""),
                    "dispositivo": order.get("tipoDispositivo", ""),
                    "cidade": cidade,
                    "estado": estado,
                    "camiseta": camiseta,
                    "inscricao_grupo": "Sim" if p.get("inscricao_grupo") else "Não",
                    "nome_grupo": p.get("nome_grupo", "") or "",
                })

        print(f"  Página {page}/{total_pages} ({len(orders)} pedidos)")
        page += 1

    return all_participants


def to_sheet_row(p):
    """Converte dict de participante para lista de 14 colunas do Sheet."""
    return [
        p["inscricao"], p["categoria"], p["modalidade"], p["sexo"],
        p["status"], p["cupom"], p["valor"], p["dataPedido"],
        p["dispositivo"], p["cidade"], p["estado"], p["camiseta"],
        p["inscricao_grupo"], p["nome_grupo"],
    ]


# ===================================================
# GOOGLE SHEETS
# ===================================================

def get_credentials(scopes=None):
    """Credenciais da service account (compartilhadas entre gspread e Drive API)."""
    if scopes is None:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    if SERVICE_ACCOUNT_JSON:
        return Credentials.from_service_account_info(json.loads(SERVICE_ACCOUNT_JSON), scopes=scopes)
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        return Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    raise Exception(
        "Credenciais Google não encontradas. "
        "Defina GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE."
    )


def get_sheets_client():
    """Cria cliente gspread autenticado via service account."""
    return gspread.authorize(get_credentials())


def migrate_legacy_tab(sh):
    """Renomeia raw_inscritos -> raw_inscritos_brasilia (one-shot, idempotente)."""
    try:
        legacy = sh.worksheet("raw_inscritos")
    except gspread.exceptions.WorksheetNotFound:
        return  # Já migrado
    try:
        sh.worksheet("raw_inscritos_brasilia")
        # Destino já existe — apaga legacy pra evitar conflito
        sh.del_worksheet(legacy)
        print("Legacy raw_inscritos removido (raw_inscritos_brasilia ja existia).")
    except gspread.exceptions.WorksheetNotFound:
        legacy.update_title("raw_inscritos_brasilia")
        print("Renomeado: raw_inscritos -> raw_inscritos_brasilia")


def write_raw_tab(sh, rows, raw_tab_name):
    """Sobrescreve uma aba raw com os dados frescos."""
    try:
        ws = sh.worksheet(raw_tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=raw_tab_name, rows=max(1000, len(rows) + 10), cols=len(HEADER))

    ws.clear()
    _retry(lambda: ws.update(values=[HEADER] + rows, range_name="A1"), f"write {raw_tab_name}")
    print(f"  -> {len(rows)} linhas em {raw_tab_name}")


def update_timestamps(sh, dash_tabs):
    """Escreve timestamp da ultima sync em cada aba dashboard (celula C2)."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    for tab in dash_tabs:
        try:
            ws = sh.worksheet(tab)
            ws.update(values=[[now]], range_name="C2")
        except Exception as e:
            print(f"  Aviso: timestamp {tab}: {e}")


# ===================================================
# LEADLOVERS
# ===================================================

def get_ll_sheet(gc):
    """Abre a planilha separada de logs do Leadlovers."""
    return gc.open_by_key(LL_SPREADSHEET_ID)


def get_sent_inscricoes(ll_sh, tab_name):
    """Retorna set de inscricao IDs já enviados ao Leadlovers para a etapa.

    Garante que a aba existe e tem header. Idempotente.
    """
    try:
        ws = ll_sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = ll_sh.add_worksheet(title=tab_name, rows=5000, cols=len(LL_SENT_HEADER))

    values = ws.col_values(1)  # coluna inscricao
    if not values:
        ws.update(values=[LL_SENT_HEADER], range_name="A1")
        return set()
    return set(str(v) for v in values[1:])  # pula header


def mark_sent_inscricoes(ll_sh, tab_name, new_entries):
    """Appenda novas linhas na aba da etapa."""
    ws = ll_sh.worksheet(tab_name)
    _retry(lambda: ws.append_rows(new_entries), f"log LL {tab_name}")


def push_to_leadlovers(ll_sh, participants, event):
    """Envia novos inscritos (não enviados antes) ao Leadlovers.

    Cada etapa usa sua propria sequencia (link de WhatsApp diferente por cidade).
    """
    event_label = event["label"]

    if not LL_API_TOKEN:
        print("  [LL] LL_API_TOKEN não configurado — pulando sync Leadlovers.")
        return

    sequence_env = event["ll_sequence_env"]
    sequence_code = os.environ.get(sequence_env, "")
    if not sequence_code:
        print(f"  [LL] {sequence_env} não configurado — pulando {event_label}.")
        return

    sent_tab = event["ll_sent_tab"]
    sent = get_sent_inscricoes(ll_sh, sent_tab)
    new_participants = [p for p in participants if str(p["inscricao"]) not in sent]
    print(f"  [LL] {len(new_participants)} novos de {len(participants)} total")

    if not new_participants:
        return

    ctx = ssl.create_default_context()
    successful = []
    now = datetime.now().strftime("%d/%m/%Y %H:%M")

    for p in new_participants:
        if not p.get("email"):
            print(f"    - inscricao {p['inscricao']} sem email, pulando")
            continue

        payload = {
            "Email": p["email"],
            "Name": p["nome"],
            "MachineCode": int(LL_MACHINE_CODE),
            "EmailSequenceCode": int(sequence_code),
            "SequenceLevelCode": "1",
            "PhoneNumber": p["celular"],
            "City": p["cidade"],
            "State": p["estado"],
        }
        body_str = json.dumps(payload)
        conn = http.client.HTTPSConnection(LL_API_BASE, context=ctx)
        conn.request(
            "POST",
            f"/webapi/lead?Token={LL_API_TOKEN}",
            body=body_str,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Content-Length": str(len(body_str.encode("utf-8"))),
            },
        )
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8")
        conn.close()

        if resp.status in (200, 201):
            successful.append([str(p["inscricao"]), p["email"], p["nome"], now])
            print(f"    ✓ {p['email']}")
        else:
            print(f"    ✗ {p['email']} — HTTP {resp.status}: {resp_body[:120]}")

    if successful:
        mark_sent_inscricoes(ll_sh, sent_tab, successful)
        print(f"  [LL] {len(successful)} leads enviados com sucesso.")


# ===================================================
# METAS — preenchimento da planilha semanal da Tamyris
# ===================================================

def parse_valor(x):
    """valorUnitario pode vir como numero, string '99.00'/'99,00', None ou ''."""
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def parse_data_pedido(s):
    """'DD/MM/YYYY HH:MM' (com ano) -> date. Tolera segundos e so data. None se falhar."""
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _norm_dash(s):
    return str(s).replace("–", "-").replace("—", "-")


def parse_periodo_fim(texto, year=CAMPAIGN_YEAR):
    """'DD/MM - DD/MM' -> date da data FINAL (corte cumulativo). None se falhar."""
    if not texto:
        return None
    parts = [p.strip() for p in _norm_dash(texto).split("-")]
    if len(parts) < 2 or not parts[-1]:
        return None
    chunk = parts[-1].split("/")
    try:
        d, m = int(chunk[0]), int(chunk[1])
        y = int(chunk[2]) if len(chunk) > 2 and chunk[2] else year
        return date(y, m, d)
    except (ValueError, IndexError):
        return None


def parse_inicio(texto, year=CAMPAIGN_YEAR):
    """'DD/MM' (ou 'DD/MM/AAAA') -> date. None se falhar."""
    if not texto:
        return None
    chunk = str(texto).strip().split("/")
    try:
        d, m = int(chunk[0]), int(chunk[1])
        y = int(chunk[2]) if len(chunk) > 2 and chunk[2] else year
        return date(y, m, d)
    except (ValueError, IndexError):
        return None


def _today_brt():
    """Data de 'hoje' em horario de Brasilia (UTC-3). O runner do GitHub Actions roda em UTC."""
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3))).date()


def _semana_futura(periodo_txt, hoje):
    """True se a semana ainda nao comecou (data de INICIO do periodo 'DD/MM - DD/MM' > hoje)."""
    inicio = parse_inicio(_norm_dash(periodo_txt).split("-")[0].strip())
    return inicio is not None and inicio > hoje


def is_free(p):
    """Gratis: valor 0 OU status Cortesia (cobre perna gratis de combo e cortesias)."""
    return parse_valor(p.get("valor")) < 0.01 or (p.get("status") or "") == "Cortesia"


def is_pcd(p):
    return "PCD" in (p.get("categoria") or "").upper()


def is_combo(p):
    return "COMBO" in (p.get("categoria") or "").upper()


def base_tier(p):
    """Tier base de um inscrito PAGO: Premium se a categoria contem PREMIUM, senao Basico."""
    return "Premium" if "PREMIUM" in (p.get("categoria") or "").upper() else "Básico"


def tier_counts_cumulative(participants, end_date):
    """Contagem cumulativa (dataPedido <= end_date) por tier.

    Total Pago = Basico + Premium (valor>0, nao-cortesia). Combo e PCD sao
    recortes informativos (subconjuntos), nao somam no total.
    """
    c = {"Básico": 0, "Premium": 0, "Combo": 0, "PCD": 0,
         "Gratuito": 0, "Total Pago": 0, "_ignorados": 0}
    for p in participants:
        d = parse_data_pedido(p.get("dataPedido"))
        if d is None:
            c["_ignorados"] += 1
            continue
        if d > end_date:
            continue
        if is_free(p):
            c["Gratuito"] += 1
            continue
        c["Total Pago"] += 1
        c[base_tier(p)] += 1
        if is_combo(p):
            c["Combo"] += 1
        if is_pcd(p):
            c["PCD"] += 1
    return c


def gratuito_count_since(participants, inicio_date):
    """Conta gratuitos (valor 0 ou Cortesia) com dataPedido >= inicio_date (todos se None)."""
    n = 0
    for p in participants:
        d = parse_data_pedido(p.get("dataPedido"))
        if d is None:
            continue
        if inicio_date and d < inicio_date:
            continue
        if is_free(p):
            n += 1
    return n


def _norm_header(s):
    return " ".join(str(s).strip().split()).casefold()


def _to_int(x):
    """Le um inteiro de uma celula (numero ou texto, tolera separador de milhar)."""
    if isinstance(x, (int, float)):
        return int(round(x))
    s = "".join(ch for ch in str(x) if ch.isdigit())
    return int(s) if s else None


# --- Escrita IN-PLACE no proprio .xlsx (Drive API + openpyxl, preserva o mesmo link) ---
# A Sheets API nao escreve em .xlsx. O robo baixa o arquivo fresco, edita SO as celulas
# de Realizado/Gap/Real.* com openpyxl (preserva todo o resto) e sobe nova versao via Drive
# (mesmo fileId = mesmo link). openpyxl mantem valores, estilos e larguras (testado).

XLSX_MIME = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def _norm_sheet(s):
    """Nome de aba normalizado: remove colchetes (proibidos em .xlsx), colapsa espacos, casefold."""
    s = str(s).replace("[", " ").replace("]", " ")
    return " ".join(s.split()).casefold()


def _find_ws(wb, logical_name):
    target = _norm_sheet(logical_name)
    for ws in wb.worksheets:
        if _norm_sheet(ws.title) == target:
            return ws
    return None


def _header_map_xlsx(ws):
    return {_norm_header(ws.cell(1, c).value): c
            for c in range(1, ws.max_column + 1)
            if ws.cell(1, c).value not in (None, "")}


def _ensure_cols(ws, names):
    """Garante headers na linha 1 (anexa a direita os que faltam, idempotente). Retorna o mapa."""
    hmap = _header_map_xlsx(ws)
    nxt = ws.max_column + 1
    for nm in names:
        if _norm_header(nm) not in hmap:
            ws.cell(1, nxt).value = nm
            hmap[_norm_header(nm)] = nxt
            nxt += 1
    return hmap


def write_metas_pagas_xlsx(ws, participants, label, hoje=None):
    """Escreve Realizado (valor SEMANAL = quantos entraram naquela semana), Gap (=Meta-Realizado)
    e colunas Real.* por semana. So toca essas celulas.

    Semanas cujo inicio > hoje (BRT) ficam em branco (preenchem quando a semana chega).
    """
    hoje = hoje or _today_brt()
    hmap = _header_map_xlsx(ws)
    col_semana = hmap.get(_norm_header("Semana"))
    col_periodo = hmap.get(_norm_header("Período"))
    col_real = hmap.get(_norm_header("Realizado"))
    col_meta = hmap.get(_norm_header("Meta")) or hmap.get(_norm_header("Meta Vendas Pagas"))
    if not (col_semana and col_periodo and col_real):
        print(f"  [METAS] {label}: faltam cabecalhos Semana/Periodo/Realizado — pulando aba")
        return 0
    # garante colunas de tier + Gap a direita (idempotente)
    hmap = _ensure_cols(ws, [n for n, _ in META_TIER_COLS] + ["Gap"])
    col_gap = hmap.get(_norm_header("Gap"))
    tier_idx = {name: hmap.get(_norm_header(name)) for name, _ in META_TIER_COLS}

    n = 0
    seen = {}
    ignorados = 0
    prev_cum = {}  # acumulado da semana anterior, p/ derivar o valor SEMANAL (cum - prev_cum)
    for r in range(2, ws.max_row + 1):
        semana = ws.cell(r, col_semana).value
        if semana is None or not str(semana).strip():
            continue
        if not str(semana).strip().lower().startswith("semana"):
            continue  # ignora linhas fora da tabela semanal (ex: bloco de metas por tier)
        periodo_txt = str(ws.cell(r, col_periodo).value or "").strip()
        fim = parse_periodo_fim(periodo_txt)
        if fim is None:
            print(f"  [METAS] {label} {semana}: Periodo '{periodo_txt}' nao parseavel — pulando linha")
            continue
        if periodo_txt in seen:
            print(f"  [METAS] {label} {semana}: Periodo duplicado '{periodo_txt}' (= {seen[periodo_txt]}) — revisar datas")
        seen[periodo_txt] = semana
        if _semana_futura(periodo_txt, hoje):
            # semana ainda nao comecou: limpar (None = branco real, p/ CF e grafico ignorarem)
            ws.cell(r, col_real).value = None
            if col_gap:
                ws.cell(r, col_gap).value = None
            for _name, _key in META_TIER_COLS:
                ci = tier_idx.get(_name)
                if ci:
                    ws.cell(r, ci).value = None
            if METAS_DRY_RUN:
                print(f"  [METAS DRY] {label} {semana}: FUTURA ({periodo_txt}) -> branco")
            n += 1
            continue
        cum = tier_counts_cumulative(participants, fim)
        ignorados = max(ignorados, cum["_ignorados"])
        # valor SEMANAL = quantos entraram NAQUELA semana = cumulativo - cumulativo da anterior
        weekly = {k: cum.get(k, 0) - prev_cum.get(k, 0)
                  for k in ("Básico", "Premium", "Combo", "PCD", "Gratuito", "Total Pago")}
        prev_cum = cum
        ws.cell(r, col_real).value = weekly["Total Pago"]
        if col_gap and col_meta:
            # Gap = Meta - Realizado (ambos da semana), como FORMULA pro Google avaliar.
            from openpyxl.utils import get_column_letter
            ws.cell(r, col_gap).value = f"={get_column_letter(col_meta)}{r}-{get_column_letter(col_real)}{r}"
        for name, key in META_TIER_COLS:
            ci = tier_idx.get(name)
            if ci:
                ws.cell(r, ci).value = weekly[key]
        if METAS_DRY_RUN:
            print(f"  [METAS DRY] {label} {semana}: semana={weekly['Total Pago']} (cum={cum['Total Pago']})")
        n += 1
    if ignorados:
        print(f"  [METAS] {label}: {ignorados} inscritos com dataPedido nao parseavel (ignorados)")
    print(f"  [METAS] {label}: {n} semanas processadas")
    return n


def write_metas_gratuitas_xlsx(ws, participants, label):
    """Escreve o Realizado das gratuitas na secao 'Inicio Monitoramento' (em QUALQUER lugar da aba).

    Funciona tanto na aba consolidada (secao mais embaixo) quanto numa aba so de gratuitas (linha 1).
    """
    hdr_row = None
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            if _norm_header(ws.cell(r, c).value) == _norm_header("Início Monitoramento"):
                hdr_row = r
                break
        if hdr_row:
            break
    if hdr_row is None:
        print(f"  [METAS] {label}: secao Gratuitas nao encontrada — pulando")
        return 0
    cols = {_norm_header(ws.cell(hdr_row, c).value): c
            for c in range(1, ws.max_column + 1) if ws.cell(hdr_row, c).value not in (None, "")}
    col_inicio = cols.get(_norm_header("Início Monitoramento"))
    col_meta = cols.get(_norm_header("Meta Gratuitas")) or cols.get(_norm_header("Meta"))
    col_real = cols.get(_norm_header("Realizado"))
    col_gap = cols.get(_norm_header("Gap"))
    drow = hdr_row + 1  # linha de dados, logo abaixo do cabecalho da secao
    inicio = parse_inicio(ws.cell(drow, col_inicio).value) if col_inicio else None
    g = gratuito_count_since(participants, inicio)
    if col_real:
        ws.cell(drow, col_real).value = g
    if col_gap and col_meta and col_real:
        from openpyxl.utils import get_column_letter
        ws.cell(drow, col_gap).value = f"={get_column_letter(col_meta)}{drow}-{get_column_letter(col_real)}{drow}"
    print(f"  [METAS] {label} gratuitas: Realizado={g} (desde {inicio})")
    return 1


def ensure_gratuitas_ssa(wb):
    """Cria a aba Gratuitas SSA se nao existir (.xlsx nao aceita colchetes no nome da aba)."""
    if _find_ws(wb, "Metas Gratuitas [ SSA ]") is not None:
        return
    ws = wb.create_sheet(title="Metas Gratuitas  SSA")
    ws.append(["Início Monitoramento", "Meta Gratuitas", "Observação", "Realizado", "Gap"])
    ws.append(["", 300, "Monitorar distribuição e engajamento", "", ""])
    print("  [METAS] aba 'Metas Gratuitas  SSA' criada (Meta 300 provisoria)")


def _drive_service():
    from googleapiclient.discovery import build
    creds = get_credentials(["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _download_xlsx(drive, file_id):
    from googleapiclient.http import MediaIoBaseDownload

    def _do():
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, drive.files().get_media(fileId=file_id, supportsAllDrives=True))
        done = False
        while not done:
            _, done = dl.next_chunk()
        return buf.getvalue()

    return _retry(_do, "metas download")


def _upload_xlsx(drive, file_id, data):
    from googleapiclient.http import MediaIoBaseUpload

    def _do():
        media = MediaIoBaseUpload(io.BytesIO(data), mimetype=XLSX_MIME, resumable=False)
        return drive.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()

    return _retry(_do, "metas upload")


def sync_metas(participants_por_cidade):
    """Atualiza a planilha de metas (.xlsx in-place via Drive). Isolada: nunca derruba o sync."""
    import openpyxl
    if not METAS_SPREADSHEET_ID:
        print("  [METAS] METAS_SPREADSHEET_ID nao configurado — pulando metas.")
        return
    if METAS_DRY_RUN:
        print("  [METAS] *** DRY-RUN: nada sera enviado ao Drive ***")
    drive = _drive_service()
    try:
        data = _download_xlsx(drive, METAS_SPREADSHEET_ID)
    except Exception as e:
        print(f"  [METAS] nao consegui baixar a planilha: {e}")
        print("  [METAS] confira: Drive API ativa, arquivo compartilhado com a SA, METAS_SPREADSHEET_ID correto.")
        return
    wb = openpyxl.load_workbook(io.BytesIO(data))
    total = 0
    for event_id, tab_name in METAS_TABS.items():
        parts = participants_por_cidade.get(event_id, [])
        ws = _find_ws(wb, tab_name)
        if ws is None:
            print(f"  [METAS] aba '{tab_name}' nao encontrada — pulando")
            continue
        try:
            total += write_metas_pagas_xlsx(ws, parts, tab_name)
        except Exception as e:
            print(f"  [METAS] erro pagas '{tab_name}': {e}")
        try:
            total += write_metas_gratuitas_xlsx(ws, parts, tab_name)
        except Exception as e:
            print(f"  [METAS] erro gratuitas '{tab_name}': {e}")
    if METAS_DRY_RUN:
        print(f"  [METAS] DRY-RUN: {total} blocos calculados, planilha NAO enviada.")
        return
    out = io.BytesIO()
    wb.save(out)
    _upload_xlsx(drive, METAS_SPREADSHEET_ID, out.getvalue())
    print(f"  [METAS] planilha atualizada in-place ({total} blocos).")


# ===================================================
# MAIN
# ===================================================

def main():
    print(f"=== Sync Ticketsports -> Sheets + Leadlovers ({datetime.now()}) ===")

    token = authenticate()
    gc = get_sheets_client()

    if SPREADSHEET_ID:
        sh = _retry(lambda: gc.open_by_key(SPREADSHEET_ID), "abrir dashboard")
    else:
        sh = _retry(lambda: gc.open("Dashboard Inscrições - Vai Bem"), "abrir dashboard")

    migrate_legacy_tab(sh)
    ll_sh = _retry(lambda: get_ll_sheet(gc), "abrir planilha LL")

    total_inscritos = 0
    participants_por_cidade = {}
    for ev in EVENTS:
        print(f"\n[{ev['label']}] event_id={ev['id']}")
        participants = fetch_all_orders(token, ev["id"])
        print(f"  Total: {len(participants)} inscritos")
        participants_por_cidade[ev["id"]] = participants
        rows = [to_sheet_row(p) for p in participants]
        write_raw_tab(sh, rows, ev["raw_tab"])
        push_to_leadlovers(ll_sh, participants, ev)
        total_inscritos += len(participants)

    update_timestamps(sh, [ev["dash_tab"] for ev in EVENTS])

    # Metas: ultima etapa, isolada — nunca pode derrubar o sync de raw/Leadlovers.
    try:
        print("\n[METAS] atualizando planilha de metas...")
        sync_metas(participants_por_cidade)
    except Exception as e:
        print(f"  [METAS] etapa de metas falhou (sync principal seguiu OK): {e}")

    print(f"\n=== Concluído: {total_inscritos} inscritos em {len(EVENTS)} etapas ===")


if __name__ == "__main__":
    main()
