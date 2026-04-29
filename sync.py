"""
Sync Ticketsports -> Google Sheets + Leadlovers (multi-etapa)
Puxa inscricoes da API Ticketsports para todas as etapas configuradas,
escreve em abas raw separadas no Google Sheets, e envia novos inscritos
ao Leadlovers para disparo da regua de email.
Roda via GitHub Actions (cron a cada 5min) ou manualmente.
"""

import http.client
import json
import os
import ssl
from datetime import datetime

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
# CONFIG LEADLOVERS
# ===================================================

LL_API_BASE = "llapi.leadlovers.com"
LL_API_TOKEN = os.environ.get("LL_API_TOKEN", "")
LL_MACHINE_CODE = os.environ.get("LL_MACHINE_CODE", "")
LL_SEQUENCE_CODE = os.environ.get("LL_SEQUENCE_CODE", "")

# Planilha separada para logs do Leadlovers (nao misturar com dashboard)
LL_SPREADSHEET_ID = "1aaDYxjcDhhR2lMLejpOW54QVNGQuSOXc8Gj6q5S2KdA"
LL_SENT_TAB = "ll_enviados"
LL_SENT_HEADER = ["inscricao", "email", "evento", "data_envio"]


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
                })

        print(f"  Página {page}/{total_pages} ({len(orders)} pedidos)")
        page += 1

    return all_participants


def to_sheet_row(p):
    """Converte dict de participante para lista de 12 colunas do Sheet."""
    return [
        p["inscricao"], p["categoria"], p["modalidade"], p["sexo"],
        p["status"], p["cupom"], p["valor"], p["dataPedido"],
        p["dispositivo"], p["cidade"], p["estado"], p["camiseta"],
    ]


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
# LEADLOVERS
# ===================================================

def get_ll_sheet(gc):
    """Abre a planilha separada de logs do Leadlovers."""
    return gc.open_by_key(LL_SPREADSHEET_ID)


def get_sent_inscricoes(ll_sh):
    """Retorna set de inscricao IDs já enviados ao Leadlovers."""
    try:
        ws = ll_sh.worksheet(LL_SENT_TAB)
        values = ws.col_values(1)  # coluna inscricao
        return set(str(v) for v in values[1:])  # pula header
    except gspread.exceptions.WorksheetNotFound:
        ws = ll_sh.add_worksheet(title=LL_SENT_TAB, rows=5000, cols=4)
        ws.update(values=[LL_SENT_HEADER], range_name="A1")
        return set()


def mark_sent_inscricoes(ll_sh, new_entries):
    """Appenda novas linhas na aba ll_enviados."""
    ws = ll_sh.worksheet(LL_SENT_TAB)
    ws.append_rows(new_entries)


def push_to_leadlovers(ll_sh, participants, event_label):
    """Envia novos inscritos (não enviados antes) ao Leadlovers."""
    if not LL_API_TOKEN:
        print("  [LL] LL_API_TOKEN não configurado — pulando sync Leadlovers.")
        return

    sent = get_sent_inscricoes(ll_sh)
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
            "EmailSequenceCode": int(LL_SEQUENCE_CODE),
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
            successful.append([str(p["inscricao"]), p["email"], event_label, now])
            print(f"    ✓ {p['email']}")
        else:
            print(f"    ✗ {p['email']} — HTTP {resp.status}: {resp_body[:120]}")

    if successful:
        mark_sent_inscricoes(ll_sh, successful)
        print(f"  [LL] {len(successful)} leads enviados com sucesso.")


# ===================================================
# MAIN
# ===================================================

def main():
    print(f"=== Sync Ticketsports -> Sheets + Leadlovers ({datetime.now()}) ===")

    token = authenticate()
    gc = get_sheets_client()

    if SPREADSHEET_ID:
        sh = gc.open_by_key(SPREADSHEET_ID)
    else:
        sh = gc.open("Dashboard Inscrições - Vai Bem")

    migrate_legacy_tab(sh)
    ll_sh = get_ll_sheet(gc)

    total_inscritos = 0
    for ev in EVENTS:
        print(f"\n[{ev['label']}] event_id={ev['id']}")
        participants = fetch_all_orders(token, ev["id"])
        print(f"  Total: {len(participants)} inscritos")
        rows = [to_sheet_row(p) for p in participants]
        write_raw_tab(sh, rows, ev["raw_tab"])
        push_to_leadlovers(ll_sh, participants, ev["label"])
        total_inscritos += len(participants)

    update_timestamps(sh, [ev["dash_tab"] for ev in EVENTS])

    print(f"\n=== Concluído: {total_inscritos} inscritos em {len(EVENTS)} etapas ===")


if __name__ == "__main__":
    main()
