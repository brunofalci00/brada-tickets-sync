import contextlib
import io
import os
import unittest
from unittest import mock

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
        self.assertEqual([e["key"] for e in sync.EVENTS], ["bsb", "bh", "ssa", "pedalx"])
        self.assertEqual(self.event["id"], 87735)
        self.assertEqual(self.event["raw_tab"], "raw_inscritos_pedalx")
        self.assertEqual(self.event["dash_tab"], "Pedal X Road")
        self.assertEqual(self.event["timestamp_cell"], "F2")
        self.assertTrue(self.event["non_blocking"])
        self.assertNotIn(87735, sync.METAS_TABS)

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
                 mock.patch.object(sync, "sync_metas") as metas:
                sync.main([])
            self.assertEqual(calls, [first["key"], "pedalx"])
            timestamps.assert_called_once()
            self.assertEqual(timestamps.call_args.args[1], [first])
            metas.assert_called_once()
        finally:
            sync.EVENTS = old_events


if __name__ == "__main__":
    unittest.main()
