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

print()
if _fail:
    print(f"=== {_fail} FALHAS ===")
    raise SystemExit(1)
print("=== TODOS OS TESTES PASSARAM ===")
