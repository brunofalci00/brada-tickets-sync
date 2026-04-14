"""
Sync Ticketsports -> Google Sheets
Puxa inscrições da API Ticketsports e escreve no Google Sheets.
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
EVENT_ID = 86595
PAGE_LIMIT = 50

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


def fetch_all_orders(token):
    """Busca todos os pedidos pagos do evento, paginando."""
    all_rows = []
    page = 1
    total_pages = 1

    while page <= total_pages:
        endpoint = f"/Order/List?page={page}&limit={PAGE_LIMIT}"
        data = api_request(
            "GET",
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            body={"events": [EVENT_ID], "status": ["Pago"]},
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

        print(f"Página {page}/{total_pages} ({len(orders)} pedidos)")
        page += 1

    print(f"Total: {len(all_rows)} inscritos")
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
        # GitHub Actions: JSON vem da variável de ambiente
        info = json.loads(SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        # Local: JSON vem do arquivo
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    else:
        raise Exception(
            "Credenciais Google não encontradas. "
            "Defina GOOGLE_SERVICE_ACCOUNT_JSON ou GOOGLE_SERVICE_ACCOUNT_FILE."
        )

    return gspread.authorize(creds)


def write_to_sheets(rows):
    """Sobrescreve a aba raw_inscritos com os dados frescos."""
    gc = get_sheets_client()

    if SPREADSHEET_ID:
        sh = gc.open_by_key(SPREADSHEET_ID)
    else:
        # Tenta abrir pelo nome
        sh = gc.open("Dashboard Inscrições - Vai Bem")

    # Selecionar ou criar aba raw_inscritos
    try:
        ws = sh.worksheet("raw_inscritos")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title="raw_inscritos", rows=1000, cols=len(HEADER))

    # Limpar e reescrever
    ws.clear()
    ws.update(values=[HEADER] + rows, range_name="A1")

    # Atualizar timestamp na aba Brasília
    try:
        dash = sh.worksheet("Brasília")
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        dash.update(values=[[now]], range_name="C2")
    except Exception as e:
        print(f"Aviso: não conseguiu atualizar timestamp: {e}")

    print(f"Sheets atualizado: {len(rows)} linhas em raw_inscritos")


# ===================================================
# MAIN
# ===================================================

def main():
    print(f"=== Sync Ticketsports -> Sheets ({datetime.now()}) ===")

    # 1. Autenticar
    token = authenticate()

    # 2. Buscar dados
    rows = fetch_all_orders(token)

    if not rows:
        print("Nenhum registro encontrado. Abortando.")
        return

    # 3. Escrever no Sheets
    write_to_sheets(rows)

    print("=== Concluído ===")


if __name__ == "__main__":
    main()
