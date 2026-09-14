from datetime import datetime, timedelta, timezone

from pulseguard_streaming.feature_state import FEATURE_NAMES, apply_event, new_state

BASE_TIME = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)  # a Tuesday


def _event(**overrides):
    base = {
        "timestamp": BASE_TIME,
        "amount": 500.0,
        "merchantId": "M001",
        "merchantCategory": "GROCERY",
        "deviceId": "D-1",
        "city": "Mumbai",
        "lat": 19.0760,
        "lon": 72.8777,
    }
    base.update(overrides)
    return base


def test_first_event_has_sensible_defaults():
    state, features = apply_event(new_state(), _event())
    assert features["secondsSincePrevTxn"] is None
    assert features["txnCount1Min"] == 1
    assert features["amountZScore"] == 0.0
    assert features["isNewMerchantCategory"] is True
    assert features["distanceFromPrevKm"] == 0.0
    assert state["n_seen"] == 1


def test_feature_vector_covers_at_least_twenty_features():
    assert len(FEATURE_NAMES) >= 20
    _state, features = apply_event(new_state(), _event())
    assert set(FEATURE_NAMES) <= set(features.keys())


def test_velocity_counts_increment_within_window():
    state = new_state()
    t = BASE_TIME
    for _ in range(4):
        state, features = apply_event(state, _event(timestamp=t))
        t += timedelta(seconds=10)
    # 5th event, all within 1 minute of each other
    state, features = apply_event(state, _event(timestamp=t))
    assert features["txnCount1Min"] == 5
    assert features["txnCount5Min"] == 5


def test_velocity_counts_exclude_events_outside_window():
    state = new_state()
    state, _ = apply_event(state, _event(timestamp=BASE_TIME))
    later = BASE_TIME + timedelta(minutes=10)
    _state, features = apply_event(state, _event(timestamp=later))
    assert features["txnCount1Min"] == 1
    assert features["txnCount15Min"] == 2


def test_repeated_similar_amounts_then_outlier_has_high_zscore():
    state = new_state()
    t = BASE_TIME
    for _ in range(30):
        state, _ = apply_event(state, _event(timestamp=t, amount=500.0 + (t.second % 5)))
        t += timedelta(minutes=20)

    _state, features = apply_event(state, _event(timestamp=t, amount=15000.0))
    assert features["amountZScore"] > 4


def test_new_device_and_new_city_are_visible_in_state_not_flagged_as_feature_directly():
    # deviceIsNew / cityIsNew style logic lives in the rule engine (Phase 3),
    # which reads distinctDevices24Hour / distinctCities24Hour from here.
    state, _ = apply_event(new_state(), _event(deviceId="D-1", city="Mumbai"))
    _state, features = apply_event(state, _event(timestamp=BASE_TIME + timedelta(minutes=5), deviceId="D-2", city="Delhi"))
    assert features["distinctDevices24Hour"] == 2
    assert features["distinctCities24Hour"] == 2


def test_geo_impossible_travel_produces_high_implied_speed():
    state, _ = apply_event(new_state(), _event(city="Mumbai", lat=19.0760, lon=72.8777, timestamp=BASE_TIME))
    later = BASE_TIME + timedelta(minutes=20)
    _state, features = apply_event(state, _event(city="Delhi", lat=28.7041, lon=77.1025, timestamp=later))
    # Mumbai-Delhi is ~1150km in 20 minutes -> a few thousand km/h, physically impossible
    assert features["distanceFromPrevKm"] > 1000
    assert features["impliedSpeedKmh"] > 900


def test_merchant_category_becomes_not_new_on_repeat_visit():
    state, first = apply_event(new_state(), _event(merchantCategory="JEWELLERY"))
    assert first["isNewMerchantCategory"] is True
    _state, second = apply_event(state, _event(merchantCategory="JEWELLERY", timestamp=BASE_TIME + timedelta(days=1)))
    assert second["isNewMerchantCategory"] is False


def test_events_older_than_seven_days_drop_out_of_windowed_counts():
    state, _ = apply_event(new_state(), _event(timestamp=BASE_TIME))
    much_later = BASE_TIME + timedelta(days=8)
    _state, features = apply_event(state, _event(timestamp=much_later))
    assert features["distinctDevices7Day"] == 1  # only the current event's device remains in window


def test_max_txns_in_60s_window_today_tracks_bursts():
    state = new_state()
    t = BASE_TIME.replace(hour=9, minute=0, second=0, microsecond=0)
    for _ in range(6):
        state, features = apply_event(state, _event(timestamp=t))
        t += timedelta(seconds=5)
    assert features["maxTxnsInAny60SecWindowToday"] == 6


def test_amount_percentile_reflects_history():
    state = new_state()
    t = BASE_TIME
    for amt in [100, 200, 300, 400, 500]:
        state, _ = apply_event(state, _event(timestamp=t, amount=float(amt)))
        t += timedelta(minutes=30)
    _state, features = apply_event(state, _event(timestamp=t, amount=1000.0))
    assert features["amountPercentile"] == 1.0  # higher than all 5 prior amounts

    _state2, features2 = apply_event(state, _event(timestamp=t, amount=0.0))
    assert features2["amountPercentile"] == 0.0  # lower than all prior amounts
