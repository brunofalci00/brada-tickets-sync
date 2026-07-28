import hashlib
import json
import unittest

import gspread

import redesign_dashboard as dashboard


RAW_HEADER = [
    "N inscricao", "Categoria", "Modalidade", "Sexo", "Status do pedido", "Cupom",
    "Valor", "Data Pedido", "Dispositivo", "Cidade", "Estado", "Camiseta",
    "Inscricao Grupo", "Nome Grupo",
]


class FakeWorksheet:
    def __init__(self, sheet_id=123, values=None):
        self.id = sheet_id
        self.values = values or []
        self.calls = []
        self.clear_count = 0

    def clear(self):
        self.clear_count += 1

    def batch_update(self, payload, **kwargs):
        self.calls.append(payload)

    def get_all_values(self):
        return self.values


class FakeSpreadsheet:
    def __init__(self, raw_values=None, dashboard_exists=True):
        self.raw = FakeWorksheet(10, raw_values)
        self.dash = FakeWorksheet(20)
        self.dashboard_exists = dashboard_exists
        self.calls = []
        self.added = []

    def worksheet(self, name):
        if name.startswith("raw_inscritos_"):
            if not self.raw.values:
                raise gspread.exceptions.WorksheetNotFound(name)
            return self.raw
        if self.dashboard_exists:
            return self.dash
        raise gspread.exceptions.WorksheetNotFound(name)

    def add_worksheet(self, title, rows, cols):
        self.added.append((title, rows, cols))
        self.dashboard_exists = True
        return self.dash

    def batch_update(self, payload):
        self.calls.append(payload)


class DashboardTests(unittest.TestCase):
    def test_legacy_payload_golden_snapshots(self):
        expected = {
            "bsb": "298d96f5fcb8b21a281c4711fcc8af32c88d7060989e1e98ce51a8610b7784ef",
            "bh": "0c9d9b8946f896536ae94099f6147acb502f0bed5bb22202d9338271e2dd1f33",
            "ssa": "8f4a105b2f24f3ab8d22f539666d1a42c7eb722ea5e48f3a6bb00f1ab4a15eff",
        }
        for key, digest in expected.items():
            sh = FakeSpreadsheet(raw_values=[RAW_HEADER], dashboard_exists=True)
            config = dashboard.DASHBOARD_CONFIGS[key]
            dashboard.build_dashboard(
                sh, config["label"], config["raw_tab"], config["dash_tab"],
                has_nubank=config["has_nubank"], modalidades=config["modalidades"],
            )
            payload = json.dumps(
                {"sh": sh.calls, "dash": sh.dash.calls},
                ensure_ascii=False, separators=(",", ":"),
            )
            self.assertEqual(hashlib.sha256(payload.encode()).hexdigest(), digest, key)

    def test_pedal_layout_formulas_and_residual(self):
        raw = [RAW_HEADER, ["1", "Pedal X", "Inscreva-se", "F", "Pago", ""]]
        sh = FakeSpreadsheet(raw, dashboard_exists=False)
        dashboard._run_config(sh, "pedalx")
        self.assertEqual(sh.added[0][0], "Pedal X Road")
        self.assertEqual(sh.dash.clear_count, 1)
        serialized = json.dumps(sh.dash.calls, ensure_ascii=False)
        self.assertIn("PEDAL X ROAD — BRASÍLIA", serialized)
        self.assertIn("F2", serialized)
        self.assertIn("Inscreva-se", serialized)
        self.assertIn("Pedal X", serialized)
        self.assertIn("Sem cupom", serialized)
        self.assertIn("=COUNTA(raw_inscritos_pedalx!A:A)-1-SOMA(C", serialized)
        # Nubank e patrocinador e vende nos 3 pedais: KPI + balde proprio.
        # Antes caia em "Outros" e sumia do dashboard.
        self.assertIn("% CUPOM NUBANK", serialized)
        for balde in ("Nubank", "Federação", "Assessoria", "Cortesia"):
            self.assertIn(balde, serialized)

    def test_mtb_layout_has_summary_then_detail(self):
        """MTB: bloco resumo por prova + tabela detalhada por categoria etaria."""
        for key, n_linhas, prova in (("pedalx_manaus", 38, "XCO"),
                                     ("pedalx_canastra", 24, "Sport 30 km")):
            with self.subTest(key=key):
                config = dashboard.DASHBOARD_CONFIGS[key]
                cat = sorted(config["expected_categorias"])[0]
                mod = config["tables"][1]["rows"][0][1]
                raw = [RAW_HEADER, ["1", cat, mod, "M", "Pago", ""]]
                sh = FakeSpreadsheet(raw, dashboard_exists=False)
                dashboard._run_config(sh, key)
                cells = sh.dash.calls[0]
                by_range = {c["range"]: c["values"][0][0] for c in cells}
                # Resumo comeca em B7 (secao), B8 (header), B9/B10 (2 provas), B11 (Total).
                self.assertEqual(by_range["B7"], "RESUMO POR PROVA")
                self.assertEqual(by_range["B11"], "Total")
                # Detalhada logo abaixo, com uma linha por categoria etaria.
                self.assertEqual(by_range["B13"], "INSCRITOS POR CATEGORIA E PROVA")
                self.assertEqual(by_range[f"B{15 + n_linhas}"], "Total")
                serialized = json.dumps(cells, ensure_ascii=False)
                self.assertIn(prova, serialized)
                self.assertIn("Masculino", serialized)
                # Criterio literal (typo da TicketSports preservado no COUNTIFS).
                self.assertIn(mod, serialized)

    def test_mtb_rows_cover_every_modalidade_da_api(self):
        """Toda modalidade esperada na raw tem linha na tabela detalhada.

        Se faltar, a venda daquela categoria fica invisivel no dashboard.
        """
        for key in ("pedalx_manaus", "pedalx_canastra"):
            with self.subTest(key=key):
                config = dashboard.DASHBOARD_CONFIGS[key]
                criterios = [c for _, c in config["tables"][1]["rows"]]
                self.assertEqual(set(criterios), set(config["expected_modalidades"]))
                self.assertEqual(len(criterios), len(set(criterios)), "linha duplicada")

    def test_raw_missing_fails_before_creation_or_clear(self):
        sh = FakeSpreadsheet(raw_values=None, dashboard_exists=False)
        with self.assertRaisesRegex(ValueError, "raw obrigatória"):
            dashboard._run_config(sh, "pedalx")
        self.assertEqual(sh.added, [])
        self.assertEqual(sh.dash.clear_count, 0)

    def test_schema_drift_fails_before_creation_or_clear(self):
        raw = [RAW_HEADER, ["1", "Outra", "Inscreva-se", "F", "Pago", ""]]
        sh = FakeSpreadsheet(raw, dashboard_exists=False)
        with self.assertRaisesRegex(ValueError, "categorias fora do contrato"):
            dashboard._run_config(sh, "pedalx")
        self.assertEqual(sh.added, [])
        self.assertEqual(sh.dash.clear_count, 0)

    def test_target_allowlist_and_formula_escaping(self):
        config = dict(dashboard.DASHBOARD_CONFIGS["pedalx"])
        config["dash_tab"] = "Aba não autorizada"
        sh = FakeSpreadsheet(
            [RAW_HEADER, ["1", "Pedal X", "Inscreva-se", "F", "Pago", ""]],
            dashboard_exists=False,
        )
        with self.assertRaisesRegex(ValueError, "protegido ou inválido"):
            dashboard.build_dashboard(
                sh, config.pop("label"), config.pop("raw_tab"), config.pop("dash_tab"), **config,
            )
        self.assertEqual(sh.added, [])
        self.assertEqual(dashboard._sheet_ref("raw d'evento"), "'raw d''evento'")
        self.assertIn('A""B', dashboard._countifs("raw", [("B", 'A"B')]))


if __name__ == "__main__":
    unittest.main()
