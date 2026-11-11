from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

from storage.supabase_store import SupabaseStore


class StorageTests(TestCase):
    def test_partner_lookup_uses_supported_query_filters(self):
        store = object.__new__(SupabaseStore)
        store.client = Mock()
        query = Mock()
        query.eq.return_value = query
        query.execute.return_value = SimpleNamespace(data=[{"url": "https://partner.example"}])
        store.client.table.return_value.select.return_value = query
        self.assertEqual(store.get_partner_link("fixture", "US"), "https://partner.example")
        self.assertEqual(query.eq.call_args_list, [
            (("partner_key", "fixture"),), (("country_code", "US"),), (("enabled", True),),
        ])

    def test_missing_supabase_credentials_fail_at_client_creation(self):
        with patch.dict("os.environ", {"SUPABASE_URL": "", "SUPABASE_SERVICE_ROLE_KEY": ""}), \
                self.assertRaisesRegex(RuntimeError, "Missing Supabase credentials"):
            SupabaseStore()
