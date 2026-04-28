"""
Sync Ticketsports -> Google Sheets (multi-etapa)
Puxa inscricoes da API Ticketsports para todas as etapas configuradas
e escreve em abas raw separadas no Google Sheets.
Roda via GitHub Actions (cron a cada hora) ou manualmente.
"""

import http.client
import json
import os
import ssl
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# ===================================================
# CONFIG
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
    },
    {
        "id": 86781,
        "label": "Belo Horizonte",
        "raw_tab": "raw_inscritos_bh",
        "dash_tab": "Belo Horizonte",
    },
]

# Credenciais via variáveis de ambiente (GitHub Secrets) ou arquivo local
TICKET_LOGIN = os.environ.get("TICKET_LOGIN", "marketing@brada.social")
TICKET_PASSWORD = os.environ.get("TICKET_PASSWORD", "102030")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
SERVICE_ACCOUNT_FILE = os.environ.get(
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    os.path.join(os.path.dirname(__file__), "..", "service-account-key.json"),
)

HEADER = [
    "N inscricao", "Categoria", "Modalidade", "Sexo", "Status do pedido",
    "Cupom", "Valor", "Data Pedido", "Dispositivo", "Cidade", "Estado", "Camiseta",
]


# ===================================================
# API TICKETSPORTS
# ===================================================

def api_request(method, endpoint, headers=None, body=None):
    """Faz request HTTP para a API Ticketsports. Suporta GET com body."""
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

    if resp.status != 200:
        raise Exception(f"HTTP {resp.status} em {endpoint}: {data[:300]}")

    return json.loads(data)


def authenticate():
    """Autentica na API e retorna o Bearer token."""
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

    print(f"Autenticado com sucesso.")
    return data["access_token"]


def fetch_all_orders(token, event_id):
    """Busca todos os pedidos pagos de um evento, paginando."""
    all_rows = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        endpoint = f"/Order/List?page={page}&limit={PAGE_LIMIT}"
        data = api_request(
            "GET",
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            body={"events": [event_id], "status": ["Pago"]},
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

                all_rows.append([
                    p.get("inscricao", ""),
                    p.get("categoria", ""),
                    p.get("modalidade", ""),
                    p.get("sexo", ""),
                    order.get("status", ""),
                    p.get("tituloCupom", ""),
                    valor,
                    order.get("dataPedido", ""),
                    order.get("tipoDispositivo", ""),
                    cidade,
                    estado,
                    camiseta,
                ])

        print(f"  Página {page}/{total_pages} ({len(orders)} pedidos)")
        page += 1

    return all_rows


# ===================================================
# GOOGLE SHEETS
# ===================================================

def get_sheets_client():
    """Cria cliente gspread autenticado via service account."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if SERVICE_ACCOUNT_JSON:
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        raise Exception(
            "Credenciais Google não encontradas. "
            "Defina GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    return gspread.authorize(creds)


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
    ws.update(values=[HEADER] + rows, range_name="A1")
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
# MAIN
# ===================================================

def main():
    print(f"=== Sync Ticketsports -> Sheets ({datetime.now()}) ===")

    token = authenticate()
    gc = get_sheets_client()

    if SPREADSHEET_ID:
        sh = gc.open_by_key(SPREADSHEET_ID)
    else:
        sh = gc.open("Dashboard Inscrições - Vai Bem")

    migrate_legacy_tab(sh)

    total_inscritos = 0
    for ev in EVENTS:
        print(f"\n[{ev['label']}] event_id={ev['id']}")
        rows = fetch_all_orders(token, ev["id"])
        print(f"  Total: {len(rows)} inscritos")
        write_raw_tab(sh, rows, ev["raw_tab"])
        total_inscritos += len(rows)

    update_timestamps(sh, [ev["dash_tab"] for ev in EVENTS])

    print(f"\n=== Concluído: {total_inscritos} inscritos em {len(EVENTS)} etapas ===")


if __name__ == "__main__":
    main()
