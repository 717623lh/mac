import datetime as dt
import tempfile
import unittest
from pathlib import Path

from stock_screener import HistoryCache, StockQuote, screen_quotes


class ScreeningLogicTests(unittest.TestCase):
    def test_stock_matches_when_price_is_above_flat_moving_averages(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = HistoryCache(Path(temp_dir) / "history.sqlite3")
            before_date = dt.date(2026, 6, 1)
            closes = [
                ((before_date - dt.timedelta(days=20 - index)).isoformat(), 10.0)
                for index in range(20)
            ]
            cache.store_closes("000001", closes)

            results, stats = screen_quotes(
                [StockQuote(code="000001", name="平安银行", price=10.5)],
                lambda code: cache.get_last_closes(code, before_date),
            )

            cache.close()
            self.assertEqual(stats.matched, 1)
            self.assertEqual(results[0].code, "000001")

    def test_stock_is_rejected_when_price_is_not_above_previous_close(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            cache = HistoryCache(Path(temp_dir) / "history.sqlite3")
            before_date = dt.date(2026, 6, 1)
            closes = [
                ((before_date - dt.timedelta(days=20 - index)).isoformat(), 10.0)
                for index in range(20)
            ]
            cache.store_closes("000002", closes)

            results, stats = screen_quotes(
                [StockQuote(code="000002", name="万科A", price=10.0)],
                lambda code: cache.get_last_closes(code, before_date),
            )

            cache.close()
            self.assertEqual(stats.matched, 0)
            self.assertEqual(results, [])


if __name__ == "__main__":
    unittest.main()
