"""
Testes unitarios (sem rede) da logica de metas em sync.py.
Rodar: python test_metas.py   (de dentro de github-actions/)
"""
from datetime import date

import sync

_fail = 0


def chk(name, got, exp):
    global _fail
    ok = got == exp
    print(("OK  " if ok else "FAIL") + f" {name}: got={got!r} exp={exp!r}")
    if not ok:
        _fail += 1


def P(cat, valor, status="Pago", data="01/06/2026 10:00"):
    return {"categoria": cat, "valor": valor, "status": status, "dataPedido": data}


# --- parse_valor ---
chk("valor_str", sync.parse_valor("99.00"), 99.0)
chk("valor_virgula", sync.parse_valor("109,90"), 109.9)
chk("valor_num", sync.parse_valor(159), 159.0)
chk("valor_none", sync.parse_valor(None), 0.0)
chk("valor_vazio", sync.parse_valor(""), 0.0)
chk("valor_zero", sync.parse_valor("0,00"), 0.0)

# --- parse_data_pedido ---
chk("data_ok", sync.parse_data_pedido("13/05/2026 11:02"), date(2026, 5, 13))
chk("data_sec", sync.parse_data_pedido("13/05/2026 11:02:33"), date(2026, 5, 13))
chk("data_ruim", sync.parse_data_pedido(""), None)
chk("data_lixo", sync.parse_data_pedido("sem data"), None)

# --- parse_periodo_fim / parse_inicio ---
chk("periodo", sync.parse_periodo_fim("14/05 - 21/05"), date(2026, 5, 21))
chk("periodo_endash", sync.parse_periodo_fim("14/05 – 21/05"), date(2026, 5, 21))
chk("periodo_ruim", sync.parse_periodo_fim("xxx"), None)
chk("periodo_vazio", sync.parse_periodo_fim(""), None)
chk("inicio", sync.parse_inicio("01/06"), date(2026, 6, 1))
chk("inicio_ano", sync.parse_inicio("01/06/2026"), date(2026, 6, 1))

# --- classificacao (categorias reais das 3 cidades) ---
chk("base_basico", sync.base_tier(P("Kit Vai Bem", "99.00")), "Básico")
chk("base_oculto", sync.base_tier(P("Kit Vai Bem - Oculto", "99.00")), "Básico")
chk("base_premium", sync.base_tier(P("KIT PREMIUM", "159.00")), "Premium")
chk("base_premium_combo", sync.base_tier(P("KIT PREMIUM - COMBO DIA DAS MAES", "222.60")), "Premium")
chk("is_combo", sync.is_combo(P("Kit VAI BEM - COMBO DIA DOS NAMORADOS", "109,90")), True)
chk("is_combo_nao", sync.is_combo(P("Kit Vai Bem", "99.00")), False)
chk("is_pcd", sync.is_pcd(P("KIT PREMIUM - PCD", "79,50")), True)
chk("free_valor0", sync.is_free(P("Kit VAI BEM - COMBO DIA DOS NAMORADOS", "0,00")), True)
chk("free_cortesia", sync.is_free(P("Kit Vai Bem - Oculto", "99.00", status="Cortesia")), True)
chk("free_nao", sync.is_free(P("Kit Vai Bem", "99.00")), False)

# --- tier_counts_cumulative ---
amostra = [
    P("Kit Vai Bem", "99.00", data="10/05/2026 10:00"),                                   # basico pago
    P("KIT PREMIUM", "159.00", data="12/05/2026 10:00"),                                  # premium pago
    P("Kit VAI BEM - COMBO DIA DOS NAMORADOS", "109,90", data="12/05/2026 11:00"),        # combo pago (base basico)
    P("Kit VAI BEM - COMBO DIA DOS NAMORADOS", "0,00", data="12/05/2026 11:00"),          # combo gratis
    P("KIT PREMIUM - PCD", "79,50", data="20/05/2026 10:00"),                             # premium+pcd pago (20/05)
    P("Kit Vai Bem - Oculto", "99.00", status="Cortesia", data="09/05/2026 10:00"),       # cortesia -> gratuito
]
c = sync.tier_counts_cumulative(amostra, date(2026, 5, 14))  # exclui o de 20/05
chk("cum_total_pago", c["Total Pago"], 3)
chk("cum_basico", c["Básico"], 2)
chk("cum_premium", c["Premium"], 1)
chk("cum_combo", c["Combo"], 1)
chk("cum_pcd", c["PCD"], 0)
chk("cum_gratuito", c["Gratuito"], 2)
chk("cum_total_eq_base", c["Básico"] + c["Premium"], c["Total Pago"])

c2 = sync.tier_counts_cumulative(amostra, date(2026, 5, 31))  # inclui o de 20/05
chk("cum2_total_pago", c2["Total Pago"], 4)
chk("cum2_premium", c2["Premium"], 2)
chk("cum2_pcd", c2["PCD"], 1)

# --- gratuito_count_since ---
chk("grat_since_10mai", sync.gratuito_count_since(amostra, date(2026, 5, 10)), 1)  # cortesia 09/05 fora
chk("grat_since_none", sync.gratuito_count_since(amostra, None), 2)

# --- _to_int (Acumulado/Meta) ---
chk("toint_str", sync._to_int("1200"), 1200)
chk("toint_milhar", sync._to_int("1.200"), 1200)
chk("toint_num", sync._to_int(510), 510)
chk("toint_vazio", sync._to_int(""), None)

# --- _semana_futura (fill-as-time) ---
HOJE = date(2026, 6, 21)
chk("futura_sim", sync._semana_futura("25/06 - 02/07", HOJE), True)     # inicio 25/06 > 21/06
chk("futura_borda", sync._semana_futura("21/06 - 28/06", HOJE), False)  # comeca hoje -> mostra
chk("futura_atual", sync._semana_futura("18/06 - 25/06", HOJE), False)  # semana corrente
chk("futura_passada", sync._semana_futura("14/05 - 21/05", HOJE), False)
chk("futura_endash", sync._semana_futura("25/06 – 02/07", HOJE), True)  # en-dash
chk("futura_lixo", sync._semana_futura("xxx", HOJE), False)             # nao-parseavel -> nao esconde

# --- painel + grafico (storytelling duravel; nada de rede) ---
import io as _io         # noqa: E402
import os as _os         # noqa: E402
import sys as _sys       # noqa: E402
import openpyxl as _oxl  # noqa: E402
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import setup_metas_xlsx as _S  # noqa: E402


def _fake_tab():
    wb = _oxl.Workbook()
    ws = wb.active
    ws.title = "Metas BH"
    hdr = ["Semana", "Período", "Meta", "Acumulado", "Realizado", "Gap",
           "Real. Básico", "Real. Premium", "Real. Combo", "Real. PCD", "Real. Gratuito"]
    for c, h in enumerate(hdr, start=1):
        ws.cell(1, c).value = h
    semanas = [("Semana 0", "30/04 - 07/05"), ("Semana 1", "07/05 - 14/05"),
               ("Semana 2", "14/05 - 21/05"), ("Semana 3", "21/05 - 28/05"),
               ("Semana 4", "28/05 - 04/06"), ("Semana 5", "04/06 - 11/06"),
               ("Semana 6", "11/06 - 18/06")]
    for i, (s, p) in enumerate(semanas):
        r = 2 + i
        ws.cell(r, 1).value = s
        ws.cell(r, 2).value = p
        ws.cell(r, 3).value = 100 + i * 10   # Meta semanal
        ws.cell(r, 5).value = 50 + i * 5     # Realizado semanal
        ws.cell(r, 7).value = 40 + i * 5     # Real. Basico
        ws.cell(r, 8).value = 10             # Real. Premium
    # so o que o painel referencia do bloco de tier (11-13) e gratuitas (17)
    ws.cell(11, 2).value, ws.cell(11, 3).value = 1200, 672
    ws.cell(12, 2).value, ws.cell(12, 3).value = 200, 160
    ws.cell(13, 2).value, ws.cell(13, 3).value = 1400, 832
    ws.cell(17, 2).value, ws.cell(17, 3).value = 300, 99
    return wb, ws


# layout dinamico (posicoes pelo nº de semanas)
chk("layout_bh", sync._metas_layout(8), (10, 15, "A19"))
chk("layout_ssa_estendido", sync._metas_layout(14), (16, 21, "A25"))

_wb, _ws = _fake_tab()
_before_AK = [[_ws.cell(r, c).value for c in range(1, 12)] for r in range(2, 9)]
# fake tab tem tier@10-13 e grat@15-17 -> tier_row=10, grat_row=15
_S.build_painel(_ws, "BH", 8, {"Básico": 1200, "Premium": 200, "Total": 1400}, 10, 15)
sync.add_evolucao_chart(_ws, 8, "BH")

chk("painel_titulo", _ws.cell(2, 14).value, "Painel BH")
chk("painel_realizado_sum", _ws.cell(3, 15).value, '=SUMIF($A:$A,"Semana*",$E:$E)')
chk("painel_tier_basico_real", _ws.cell(9, 15).value, "=$C$11")
chk("painel_tier_basico_pct", str(_ws.cell(9, 17).value).startswith("=IF(OR($B$11"), True)
chk("painel_sem_sparkline", _ws.cell(3, 18).value, None)   # R3 vazio (sparkline removido)
chk("painel_AK_intacto", [[_ws.cell(r, c).value for c in range(1, 12)] for r in range(2, 9)], _before_AK)
chk("painel_chart_1", len(_ws._charts), 1)
chk("painel_chart_anchor_A20", _ws._charts[0].anchor, "A20")  # em memoria e a string; vira A20 (col0,row19) ao salvar

# sobrevive ao round-trip do openpyxl (== o que o cron faz: load -> save)
_buf = _io.BytesIO()
_wb.save(_buf)
_ws2 = _oxl.load_workbook(_io.BytesIO(_buf.getvalue())).worksheets[0]
chk("painel_rt_titulo", _ws2.cell(2, 14).value, "Painel BH")
chk("painel_rt_chart_sem_dup", len(_ws2._charts), 1)

# --- bloco de tier: Gap na coluna E (NAO na D escondida) ---
_wbt = _oxl.Workbook()
_wst = _wbt.active
_wst.cell(2, 1).value = "Semana 1"   # 1 linha 'Semana*' pro SUMIF
_wst.cell(2, 5).value = 100
_S.build_tier_block(_wst, {"Básico": 1200, "Premium": 200, "Total": 1400}, 10)
chk("tier_header_gap_em_E", _wst.cell(10, 5).value, "Gap")          # E10
chk("tier_header_D_vazio", _wst.cell(10, 4).value, None)            # D10 sem 'Gap'
chk("tier_gap_total_em_E", str(_wst.cell(13, 5).value).startswith('=IF($B13'), True)  # E13 Gap guardado
chk("tier_gap_D_vazio", _wst.cell(13, 4).value, None)              # D13 sem Gap
chk("tier_total_realizado_sumif", _wst.cell(13, 3).value, '=SUMIF($A:$A,"Semana*",$E:$E)')

# --- write_metas_native: _build_metas_writes (mapeamento/branco/gratuitas, sem rede) ---
HOJE_N = date(2026, 6, 22)
_grid = [
    ["Semana", "Período", "Meta", "Acumulado", "Realizado", "Gap",
     "Real. Básico", "Real. Premium", "Real. Combo", "Real. PCD", "Real. Gratuito"],
    ["Semana 0", "30/04 - 07/05"],   # passada, 0 inscritos ate 07/05
    ["Semana 1", "07/05 - 14/05"],   # passada, 2 pagos
    ["Semana 2", "25/06 - 02/07"],   # FUTURA (inicio 25/06 > 22/06) -> clear
    [],
    ["Meta por tier", "Meta", "Realizado"],   # NAO e linha 'Semana' -> ignora
    ["Básico (R$99)"],
    ["Metas Gratuitas"],
    ["Início Monitoramento", "Meta Gratuitas", "Realizado", "Gap", "Observação"],
    ["01/05", 300, "", "", "obs"],   # gratuitas: dados na linha 10
]
_parts = [P("Kit Vai Bem", "99.00", data="10/05/2026 10:00"),                          # basico, Semana 1
          P("KIT PREMIUM", "159.00", data="12/05/2026 10:00"),                         # premium, Semana 1
          P("Kit Vai Bem - Oculto", "99.00", status="Cortesia", data="03/05/2026 10:00")]  # gratuito desde 01/05
_u, _c, _log = sync._build_metas_writes(_grid, _parts, "TEST", HOJE_N)
_um = {d["range"]: d["values"][0] for d in _u}
_starts = [r.split(":")[0] for r in _um]
chk("nat_clear_futura", "E4:K4" in _c, True)               # Semana 2 (linha 4) futura -> clear
chk("nat_sem0_zero", _um["E2:K2"][0], 0)                   # Semana 0: 0 pagos -> escreve 0 (nao branco)
chk("nat_gap_formula", _um["E3:K3"][1], "=C3-E3")          # F = formula sem separador (imune a locale)
chk("nat_sem1_total", _um["E3:K3"][0], 2)                  # Semana 1: 2 pagos ate 14/05
chk("nat_sem1_basico", _um["E3:K3"][2], 1)                 # G: 1 basico
chk("nat_sem1_premium", _um["E3:K3"][3], 1)                # H: 1 premium
chk("nat_no_tierblock", any(s in ("E6", "E7") for s in _starts), False)  # bloco tier nao escrito
chk("nat_grat_real_C", _um["C10"][0], 1)                   # gratuitas Realizado em col C (header-relativo)
chk("nat_grat_gap_D", _um["D10"][0], "=B10-C10")           # gratuitas Gap em col D
chk("nat_never_AB", any(s[0] in "AB" for s in _starts), False)            # nunca escreve A/B (humano)
chk("nat_C_only_grat", all(s == "C10" for s in _starts if s[0] == "C"), True)  # C so na gratuitas

print()
if _fail:
    print(f"=== {_fail} FALHAS ===")
    raise SystemExit(1)
print("=== TODOS OS TESTES PASSARAM ===")
