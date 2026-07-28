from unittest.mock import patch

import pytest

from kalshi_research.collector import PaperCollector


def test_loop_stops_cleanly_on_keyboard_interrupt() -> None:
    collector = object.__new__(PaperCollector)
    with patch.object(collector, "collect_once"), patch("kalshi_research.collector.time.sleep", side_effect=KeyboardInterrupt):
        collector.run_forever(2)


def test_loop_retries_transient_network_error() -> None:
    import httpx

    collector = object.__new__(PaperCollector)
    with patch.object(collector, "collect_once", side_effect=[httpx.ConnectError("offline"), KeyboardInterrupt]), \
         patch("kalshi_research.collector.time.sleep") as sleep:
        collector.run_forever(2)
    sleep.assert_called_once_with(2)
