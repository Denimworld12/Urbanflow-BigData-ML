"""Row-key design for the HBase serving layer. No HBase or Spark needed."""
from datetime import date
import pytest
from urbanflow.hbase import keys


def test_demand_key_and_prefixes():
    assert keys.demand_key("JFK Airport", "weekday", 7) == "JFK Airport#weekday#07"
    assert keys.demand_prefix("JFK Airport") == "JFK Airport#"
    assert keys.demand_prefix("JFK Airport", "weekend") == "JFK Airport#weekend#"
    assert keys.demand_key("JFK Airport", "weekend", 3).startswith(keys.demand_prefix("JFK Airport", "weekend"))


def test_zero_padding_makes_byte_order_numeric_order():
    hours = [keys.demand_key("Z", "weekday", h) for h in range(24)]
    assert sorted(hours) == hours
    miles = [keys.duration_key("Manhattan", "Queens", "weekday", 8, d) for d in range(1, 31)]
    assert sorted(miles) == miles


def test_duration_key_matches_prefixes():
    k = keys.duration_key("Manhattan", "Queens", "weekday", 8, 10)
    assert k == "Manhattan#Queens#weekday#08#10.0"
    for p in (keys.duration_prefix("Manhattan", "Queens"),
              keys.duration_prefix("Manhattan", "Queens", "weekday"),
              keys.duration_prefix("Manhattan", "Queens", "weekday", 8)):
        assert k.startswith(p)
    # "Manhattan#Queens#weekday#08#" must not also match hour 08x or borough "Queens2"
    assert not keys.duration_key("Manhattan", "Queens", "weekday", 18, 1).startswith(
        keys.duration_prefix("Manhattan", "Queens", "weekday", 8))


def test_daily_key_is_iso_date():
    assert keys.daily_key(date(2025, 3, 7)) == "2025-03-07"


def test_rejects_values_that_would_break_the_key():
    with pytest.raises(ValueError):
        keys.demand_key("A#B", "weekday", 1)
    with pytest.raises(ValueError):
        keys.demand_key("A", "weekday", 24)
    with pytest.raises(ValueError):
        keys.duration_key("A", "B", "weekday", 1, 100)
