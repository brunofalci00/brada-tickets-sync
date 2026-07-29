import contextlib
import io
import os
import unittest
from unittest import mock

import gspread

import sync


class FakeCell:
    def __init__(self):
        self.value = None


class FakeWorksheet:
    def __init__(self):
        self.cells = {}

    def acell(self, address):
        return self.cells.setdefault(address, FakeCell())

    def update_acell(self, address, value):
        self.acell(address).value = value

    def update(self, values, range_name):
        self.acell(range_name).value = values[0][0]


class FakeSpreadsheet:
    def __init__(self):
        self.worksheets = {}
        self.requested = []

    def worksheet(self, name):
        self.requested.append(name)
        return self.worksheets.setdefault(name, FakeWorksheet())


class SyncPedalXTests(unittest.TestCase):
    def setUp(self):
        self.event = next(event for event in sync.EVENTS if event["key"] == "pedalx")

    def test_configuration_is_appended_and_isolated_from_metas(self):
        self.assertEqual([e["key"] for e in sync.EVENTS],
                         ["bsb", "bh", "ssa", "pedalx", "pedalx_manaus", "pedalx_canastra",
                          "santos"])
        self.assertEqual(self.event["id"], 87735)
        self.assertEqual(self.event["raw_tab"], "raw_inscritos_pedalx")
        self.assertEqual(self.event["dash_tab"], "Pedal X Road")
        self.assertEqual(self.event["timestamp_cell"], "F2")
        self.assertTrue(self.event["non_blocking"])
        self.assertNotIn(87735, sync.METAS_TABS)

    def test_pedal_events_are_isolated_and_never_email(self):
        """Os 3 pedais: metas so no backend nativo, non_blocking, e sem regua de e-mail.

        O secret de sequencia nao existe -> sync_to_leadlovers retorna cedo. Se alguem
        criar LL_SEQUENCE_PEDALX*, comeca a disparar e-mail sem querer.

        Desde 28/07 os pedais TEM aba de meta, mas so nativa: o .xlsx (METAS_TABS) e da
        Tamyris e so tem corridas, e METAS_CHART_TABS e a auto-cura do grafico daquele
        arquivo — o grafico nativo e duravel e nao precisa.
        """
        pedais = [e for e in sync.EVENTS if e["key"].startswith("pedalx")]
        self.assertEqual(len(pedais), 3)
        for event in pedais:
            with self.subTest(event=event["key"]):
                self.assertTrue(event["non_blocking"])
                self.assertEqual(event["timestamp_cell"], "F2")
                self.assertNotIn(event["id"], sync.METAS_TABS)
                self.assertNotIn(event["id"], sync.METAS_CHART_TABS)
                self.assertIn(event["id"], sync.METAS_TABS_NATIVE)
                self.assertEqual(os.environ.get(event["ll_sequence_env"], ""), "")
        ids = {e["id"] for e in pedais}
        self.assertEqual(ids, {87735, 87732, 87727})
        tabs = [e["raw_tab"] for e in pedais] + [e["dash_tab"] for e in pedais]
        self.assertEqual(len(tabs), len(set(tabs)), "abas de pedal nao podem colidir")

    def test_santos_is_registered_non_blocking_and_never_emails(self):
        """Circuito Santos (87817) e de OUTRO organizador (EGP BRASIL).

        Order/List e escopado por CNPJ do login e devolve 204 vazio, entao a etapa falha
        todo run ate o acesso chegar. `non_blocking` e o que transforma isso em log em vez
        de run quebrado — sem ele, as outras 6 etapas parariam junto.
        """
        santos = next(e for e in sync.EVENTS if e["key"] == "santos")
        self.assertEqual(santos["id"], 87817)
        self.assertTrue(santos["non_blocking"], "sem non_blocking o bloqueio derruba o run")
        self.assertEqual(santos["timestamp_cell"], "F2")
        self.assertIn(87817, sync.METAS_TABS_NATIVE)
        self.assertNotIn(87817, sync.METAS_TABS)
        self.assertNotIn(87817, sync.METAS_CHART_TABS)
        self.assertEqual(os.environ.get(santos["ll_sequence_env"], ""), "")
        # a raw e o dash nao podem colidir com nenhuma outra etapa
        outros = [e for e in sync.EVENTS if e["key"] != "santos"]
        self.assertNotIn(santos["raw_tab"], {e["raw_tab"] for e in outros})
        self.assertNotIn(santos["dash_tab"], {e["dash_tab"] for e in outros})

    def test_santos_uses_its_own_credential_and_never_falls_back(self):
        """Evento de outro organizador tem credencial propria, e a falta dela LEVANTA.

        O fallback silencioso para a conta padrao seria o pior resultado possivel: a API
        devolveria 204 vazio, indistinguivel de "loja sem venda", e um secret faltando
        ficaria escondido por semanas.
        """
        santos = next(e for e in sync.EVENTS if e["key"] == "santos")
        self.assertEqual(santos["login_env"], "TICKET_LOGIN_SANTOS")
        self.assertEqual(santos["password_env"], "TICKET_PASSWORD_SANTOS")

        # nenhuma outra etapa declara credencial propria
        outros = [e for e in sync.EVENTS if e["key"] != "santos"]
        self.assertEqual([e for e in outros if e.get("login_env")], [])

        # secret ausente -> levanta, sem tocar na rede
        with mock.patch.dict(os.environ, {"TICKET_LOGIN_SANTOS": "", "TICKET_PASSWORD_SANTOS": ""}):
            with self.assertRaisesRegex(RuntimeError, "TICKET_LOGIN_SANTOS"):
                sync.token_for_event(santos, {})

        # secret presente -> autentica com a credencial DELE, nao com a padrao
        vistos = []
        with mock.patch.object(sync, "authenticate", lambda l=None, p=None: vistos.append((l, p)) or "tok"):
            with mock.patch.dict(os.environ, {"TICKET_LOGIN_SANTOS": "u", "TICKET_PASSWORD_SANTOS": "s"}):
                cache = {}
                self.assertEqual(sync.token_for_event(santos, cache), "tok")
                sync.token_for_event(santos, cache)          # 2a chamada reusa o cache
            self.assertEqual(vistos, [("u", "s")], "autenticou mais de uma vez ou com a conta errada")
            # etapa sem login_env continua na conta padrao
            sync.token_for_event(outros[0], cache)
            self.assertEqual(vistos[-1], (None, None))

    def test_blocked_event_leaves_metas_tab_untouched(self):
        """O caminho completo do bloqueio: 204 -> raw preservada -> aba de metas intocada.

        Escrever 0 diria "nao vendeu" quando a verdade e "nao da pra ver". O elo que
        garante isso: write_raw_tab LEVANTA em lista vazia, main() pula a chave, e
        write_metas_native so processa evento presente em participants_por_cidade.
        """
        # elo 1: lista vazia levanta, entao a raw nunca e sobrescrita
        with self.assertRaisesRegex(ValueError, "zero participantes"):
            sync.write_raw_tab(FakeSpreadsheet(), [], "raw_inscritos_santos")

        # elo 2: sem a chave, write_metas_native nem pede a aba
        pedidas = []

        class _Espiao:
            def worksheet(self, nome):
                pedidas.append(nome)
                raise gspread.exceptions.WorksheetNotFound(nome)

        sync.write_metas_native(_Espiao(), {})
        self.assertEqual(pedidas, [], "etapa nao sincronizada nao pode tocar a aba")

        # controle: com a chave presente (loja aberta sem venda) a aba E processada,
        # porque ai o zero e verdade. E o par que prova que a guarda discrimina.
        sync.write_metas_native(_Espiao(), {87817: []})
        self.assertEqual(pedidas, ["Metas Circuito Santos"])

    def test_empty_api_response_fails_before_worksheet_access(self):
        sh = FakeSpreadsheet()
        with self.assertRaisesRegex(ValueError, "zero participantes"):
            sync.write_raw_tab(sh, [], "raw_inscritos_pedalx")
        self.assertEqual(sh.requested, [])

    def test_schema_drift_warns_without_raising(self):
        participants = [
            {"modalidade": "Inscreva-se", "categoria": "Pedal X"},
            {"modalidade": "Nova modalidade", "categoria": "Pedal X 2"},
        ]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            warnings = sync.check_event_schema(participants, self.event)
        self.assertEqual(len(warnings), 2)
        self.assertIn("[SCHEMA WARNING]", output.getvalue())

    def test_custom_timestamp_cell(self):
        sh = FakeSpreadsheet()
        sync.update_timestamps(sh, [self.event])
        self.assertIsNotNone(sh.worksheet("Pedal X Road").acell("F2").value)
        self.assertIsNone(sh.worksheet("Pedal X Road").acell("C2").value)

    def test_raw_only_skips_leadlovers_timestamps_and_metas(self):
        old_spreadsheet_id = sync.SPREADSHEET_ID
        sync.SPREADSHEET_ID = "test-sheet-id"
        try:
            with mock.patch.object(sync, "authenticate", return_value="token"), \
                 mock.patch.object(sync, "get_sheets_client", return_value=mock.Mock()), \
                 mock.patch.object(sync, "_open_dashboard", return_value="sheet"), \
                 mock.patch.object(sync, "sync_event", return_value=[{"id": 1}]) as event_call, \
                 mock.patch.object(sync, "migrate_legacy_tab") as migrate, \
                 mock.patch.object(sync, "get_ll_sheet") as get_ll, \
                 mock.patch.object(sync, "update_timestamps") as timestamps, \
                 mock.patch.object(sync, "sync_metas") as metas:
                sync.main(["--event", "pedalx", "--raw-only"])
            event_call.assert_called_once()
            self.assertFalse(event_call.call_args.kwargs["send_leads"])
            migrate.assert_not_called()
            get_ll.assert_not_called()
            timestamps.assert_not_called()
            metas.assert_not_called()
        finally:
            sync.SPREADSHEET_ID = old_spreadsheet_id

    def test_pedal_failure_is_non_blocking_in_full_sync(self):
        old_events = sync.EVENTS
        first = dict(old_events[0])
        pedal = dict(self.event)
        sync.EVENTS = [first, pedal]
        calls = []

        def fake_sync_event(_token, _sh, event, **_kwargs):
            calls.append(event["key"])
            if event["key"] == "pedalx":
                raise RuntimeError("simulated")
            return [{"id": 1}]

        try:
            with mock.patch.object(sync, "_parse_args", return_value=mock.Mock(raw_only=False)), \
                 mock.patch.object(sync, "authenticate", return_value="token"), \
                 mock.patch.object(sync, "get_sheets_client", return_value=mock.Mock()), \
                 mock.patch.object(sync, "_open_dashboard", return_value="sheet"), \
                 mock.patch.object(sync, "migrate_legacy_tab"), \
                 mock.patch.object(sync, "get_ll_sheet", return_value="ll"), \
                 mock.patch.object(sync, "sync_event", side_effect=fake_sync_event), \
                 mock.patch.object(sync, "update_timestamps") as timestamps, \
                 mock.patch.object(sync, "sync_metas") as metas, \
                 mock.patch.object(sync, "write_metas_native") as metas_native:
                sync.main([])
            self.assertEqual(calls, [first["key"], "pedalx"])
            timestamps.assert_called_once()
            self.assertEqual(timestamps.call_args.args[1], [first])
            # backend "both" (default): os DOIS destinos de metas sao escritos.
            metas.assert_called_once()
            metas_native.assert_called_once()
        finally:
            sync.EVENTS = old_events


if __name__ == "__main__":
    unittest.main()
