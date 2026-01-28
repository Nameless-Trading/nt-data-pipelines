"""Unit tests for slack_daily_summary module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pipelines.utils.slack_daily_summary import (
    categorize_trades,
    get_current_positions_with_weights,
    get_position_changes,
)


class TestCategorizeTrades:
    """Tests for trade categorization logic."""

    def test_categorize_trades_mixed(self):
        """Test categorizing mixed buy and sell trades."""
        trades = [
            {
                "side": "buy",
                "notional": 1000,
                "ticker": "AAPL",
                "filled_qty": 10,
                "filled_avg_price": 100,
            },
            {
                "side": "sell",
                "notional": 500,
                "ticker": "MSFT",
                "filled_qty": 5,
                "filled_avg_price": 100,
            },
            {
                "side": "buy",
                "notional": 2000,
                "ticker": "TSLA",
                "filled_qty": 20,
                "filled_avg_price": 100,
            },
        ]

        result = categorize_trades(trades)

        assert len(result["buys"]) == 2
        assert len(result["sells"]) == 1
        assert result["total_notional"] == 3500
        assert result["total_buys_notional"] == 3000
        assert result["total_sells_notional"] == 500
        assert len(result["largest_trades"]) == 3

    def test_categorize_trades_largest_sorted(self):
        """Test that largest trades are sorted correctly."""
        trades = [
            {
                "side": "buy",
                "notional": 100,
                "ticker": "A",
                "filled_qty": 1,
                "filled_avg_price": 100,
            },
            {
                "side": "buy",
                "notional": 3000,
                "ticker": "C",
                "filled_qty": 30,
                "filled_avg_price": 100,
            },
            {
                "side": "buy",
                "notional": 500,
                "ticker": "B",
                "filled_qty": 5,
                "filled_avg_price": 100,
            },
        ]

        result = categorize_trades(trades)

        assert result["largest_trades"][0]["ticker"] == "C"
        assert result["largest_trades"][0]["notional"] == 3000
        assert result["largest_trades"][1]["ticker"] == "B"
        assert result["largest_trades"][1]["notional"] == 500

    def test_categorize_trades_top_3_only(self):
        """Test that only top 3 trades are returned."""
        trades = [
            {
                "side": "buy",
                "notional": i * 100,
                "ticker": f"T{i}",
                "filled_qty": i,
                "filled_avg_price": 100,
            }
            for i in range(1, 6)
        ]

        result = categorize_trades(trades)

        assert len(result["largest_trades"]) == 3
        assert result["largest_trades"][0]["notional"] == 500

    def test_categorize_trades_empty(self):
        """Test with empty trade list."""
        trades = []

        result = categorize_trades(trades)

        assert len(result["buys"]) == 0
        assert len(result["sells"]) == 0
        assert result["total_notional"] == 0
        assert len(result["largest_trades"]) == 0

    def test_categorize_trades_only_buys(self):
        """Test with only buy trades."""
        trades = [
            {
                "side": "buy",
                "notional": 1000,
                "ticker": "AAPL",
                "filled_qty": 10,
                "filled_avg_price": 100,
            },
            {
                "side": "buy",
                "notional": 2000,
                "ticker": "MSFT",
                "filled_qty": 20,
                "filled_avg_price": 100,
            },
        ]

        result = categorize_trades(trades)

        assert len(result["buys"]) == 2
        assert len(result["sells"]) == 0
        assert result["total_buys_notional"] == 3000
        assert result["total_sells_notional"] == 0

    def test_categorize_trades_only_sells(self):
        """Test with only sell trades."""
        trades = [
            {
                "side": "sell",
                "notional": 1000,
                "ticker": "AAPL",
                "filled_qty": 10,
                "filled_avg_price": 100,
            },
            {
                "side": "sell",
                "notional": 2000,
                "ticker": "MSFT",
                "filled_qty": 20,
                "filled_avg_price": 100,
            },
        ]

        result = categorize_trades(trades)

        assert len(result["buys"]) == 0
        assert len(result["sells"]) == 2
        assert result["total_buys_notional"] == 0
        assert result["total_sells_notional"] == 3000


class TestGetCurrentPositionsWithWeights:
    """Tests for position weight calculation."""

    @patch("pipelines.utils.slack_daily_summary.get_alpaca_trading_client")
    def test_get_positions_with_weights(self, mock_get_client):
        """Test position weight calculation."""
        # Mock Alpaca positions
        mock_pos1 = Mock()
        mock_pos1.symbol = "AAPL"
        mock_pos1.qty = 100
        mock_pos1.market_value = 30000
        mock_pos1.cost_basis = 30000  # 100 * 300
        mock_pos1.current_price = 300
        mock_pos1.unrealized_pl = 1000
        mock_pos1.unrealized_plpc = 0.0345

        mock_pos2 = Mock()
        mock_pos2.symbol = "MSFT"
        mock_pos2.qty = 50
        mock_pos2.market_value = 20000
        mock_pos2.cost_basis = 20000  # 50 * 400
        mock_pos2.current_price = 400
        mock_pos2.unrealized_pl = 500
        mock_pos2.unrealized_plpc = 0.0263

        mock_client = Mock()
        mock_client.get_all_positions.return_value = [mock_pos1, mock_pos2]
        mock_get_client.return_value = mock_client

        account_value = 50000

        result = get_current_positions_with_weights(account_value)

        assert len(result) == 2
        assert result[0]["ticker"] == "AAPL"
        assert result[0]["value"] == 30000
        assert result[0]["weight"] == pytest.approx(0.6)
        assert result[1]["ticker"] == "MSFT"
        assert result[1]["weight"] == pytest.approx(0.4)

    @patch("pipelines.utils.slack_daily_summary.get_alpaca_trading_client")
    def test_get_positions_sorted_by_value(self, mock_get_client):
        """Test that positions are sorted by value descending."""
        mock_pos1 = Mock()
        mock_pos1.symbol = "SMALL"
        mock_pos1.qty = 10
        mock_pos1.market_value = 1000
        mock_pos1.cost_basis = 1000  # 10 * 100
        mock_pos1.current_price = 100
        mock_pos1.unrealized_pl = 0
        mock_pos1.unrealized_plpc = 0

        mock_pos2 = Mock()
        mock_pos2.symbol = "LARGE"
        mock_pos2.qty = 100
        mock_pos2.market_value = 50000
        mock_pos2.cost_basis = 50000  # 100 * 500
        mock_pos2.current_price = 500
        mock_pos2.unrealized_pl = 2000
        mock_pos2.unrealized_plpc = 0.04

        mock_client = Mock()
        mock_client.get_all_positions.return_value = [mock_pos1, mock_pos2]
        mock_get_client.return_value = mock_client

        result = get_current_positions_with_weights(51000)

        assert result[0]["ticker"] == "LARGE"
        assert result[1]["ticker"] == "SMALL"

    @patch("pipelines.utils.slack_daily_summary.get_alpaca_trading_client")
    def test_get_positions_empty(self, mock_get_client):
        """Test with no open positions."""
        mock_client = Mock()
        mock_client.get_all_positions.return_value = []
        mock_get_client.return_value = mock_client

        result = get_current_positions_with_weights(100000)

        assert len(result) == 0

    @patch("pipelines.utils.slack_daily_summary.get_alpaca_trading_client")
    def test_get_positions_zero_account_value(self, mock_get_client):
        """Test with zero account value (edge case)."""
        mock_pos = Mock()
        mock_pos.symbol = "AAPL"
        mock_pos.qty = 100
        mock_pos.market_value = 30000
        mock_pos.cost_basis = 30000  # 100 * 300
        mock_pos.current_price = 300
        mock_pos.unrealized_pl = 1000
        mock_pos.unrealized_plpc = 0.0345

        mock_client = Mock()
        mock_client.get_all_positions.return_value = [mock_pos]
        mock_get_client.return_value = mock_client

        result = get_current_positions_with_weights(0)

        assert len(result) == 1
        assert result[0]["weight"] == 0

    @patch("pipelines.utils.slack_daily_summary.get_alpaca_trading_client")
    def test_get_positions_handles_none_current_price(self, mock_get_client):
        """Test handling of None current_price."""
        mock_pos = Mock()
        mock_pos.symbol = "AAPL"
        mock_pos.qty = 100
        mock_pos.market_value = 30000
        mock_pos.cost_basis = 30000  # 100 * 300
        mock_pos.current_price = None  # None current price
        mock_pos.unrealized_pl = None
        mock_pos.unrealized_plpc = None

        mock_client = Mock()
        mock_client.get_all_positions.return_value = [mock_pos]
        mock_get_client.return_value = mock_client

        result = get_current_positions_with_weights(50000)

        assert len(result) == 1
        assert result[0]["current_price"] == 0
        assert result[0]["unrealized_pl"] == 0
        assert result[0]["unrealized_plpc"] == 0


class TestGetPositionChanges:
    """Tests for position change tracking."""

    def test_get_position_changes_initiated(self):
        """Test identifying newly initiated positions."""
        trades = [
            {
                "side": "buy",
                "notional": 1000,
                "ticker": "AAPL",
                "filled_qty": 10,
                "filled_avg_price": 100,
            },
            {
                "side": "buy",
                "notional": 2000,
                "ticker": "MSFT",
                "filled_qty": 20,
                "filled_avg_price": 100,
            },
        ]

        result = get_position_changes(trades)

        assert len(result["initiated"]) == 2
        assert len(result["exited"]) == 0
        assert result["initiated"][0]["ticker"] == "AAPL"
        assert result["initiated"][0]["qty"] == 10
        assert result["initiated"][0]["avg_price"] == 100

    def test_get_position_changes_exited(self):
        """Test identifying exited positions."""
        trades = [
            {
                "side": "sell",
                "notional": 1000,
                "ticker": "AAPL",
                "filled_qty": 10,
                "filled_avg_price": 100,
            },
            {
                "side": "sell",
                "notional": 2000,
                "ticker": "MSFT",
                "filled_qty": 20,
                "filled_avg_price": 100,
            },
        ]

        result = get_position_changes(trades)

        assert len(result["initiated"]) == 0
        assert len(result["exited"]) == 2
        assert result["exited"][0]["ticker"] == "AAPL"
        assert result["exited"][0]["qty"] == 10

    def test_get_position_changes_mixed(self):
        """Test with both buys and sells of different tickers."""
        trades = [
            {"side": "buy", "notional": 1000, "ticker": "AAPL", "filled_qty": 10, "filled_avg_price": 100},
            {"side": "sell", "notional": 500, "ticker": "MSFT", "filled_qty": 5, "filled_avg_price": 100},
        ]

        result = get_position_changes(trades)

        assert len(result["initiated"]) == 1
        assert result["initiated"][0]["ticker"] == "AAPL"
        assert len(result["exited"]) == 1
        assert result["exited"][0]["ticker"] == "MSFT"

    def test_get_position_changes_buy_and_sell_same_ticker(self):
        """Test buying and selling the same ticker (not initiated or exited)."""
        trades = [
            {"side": "buy", "notional": 1000, "ticker": "AAPL", "filled_qty": 10, "filled_avg_price": 100},
            {"side": "sell", "notional": 500, "ticker": "AAPL", "filled_qty": 5, "filled_avg_price": 100},
        ]

        result = get_position_changes(trades)

        # Neither initiated nor exited since we bought and sold the same ticker
        assert len(result["initiated"]) == 0
        assert len(result["exited"]) == 0

    def test_get_position_changes_avg_price(self):
        """Test average price calculation for multiple trades."""
        trades = [
            {"side": "buy", "notional": 1000, "ticker": "AAPL", "filled_qty": 10, "filled_avg_price": 100},
            {"side": "buy", "notional": 500, "ticker": "AAPL", "filled_qty": 5, "filled_avg_price": 100},
        ]

        result = get_position_changes(trades)

        # Both trades are buys, so it's initiated
        assert len(result["initiated"]) == 1
        assert result["initiated"][0]["qty"] == 15
        assert result["initiated"][0]["notional"] == 1500
        assert result["initiated"][0]["avg_price"] == 100

    def test_get_position_changes_empty(self):
        """Test with no trades."""
        trades = []

        result = get_position_changes(trades)

        assert len(result["initiated"]) == 0
        assert len(result["exited"]) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
