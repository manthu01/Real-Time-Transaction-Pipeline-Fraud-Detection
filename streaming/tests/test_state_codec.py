from datetime import datetime, timedelta, timezone

from pulseguard_streaming.feature_state import apply_event, new_state
from pulseguard_streaming.state_codec import deserialize_state, serialize_state

BASE_TIME = datetime(2026, 3, 10, 10, 0, 0, tzinfo=timezone.utc)


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


def test_round_trip_preserves_state_and_downstream_features():
    state, _ = apply_event(new_state(), _event())
    state, _ = apply_event(state, _event(timestamp=BASE_TIME + timedelta(minutes=5), amount=750.0, deviceId="D-2"))

    raw = serialize_state(state)
    restored = deserialize_state(raw)

    assert restored["n_seen"] == state["n_seen"]
    assert restored["visited_categories"] == state["visited_categories"]
    assert restored["known_devices"].keys() == state["known_devices"].keys()
    assert restored["last_txn_time"] == state["last_txn_time"]

    # continuing the stream on the restored state must produce identical
    # features to continuing on the original -- this is the property that
    # actually matters (a checkpoint restore must not change downstream output).
    next_event = _event(timestamp=BASE_TIME + timedelta(minutes=10), amount=600.0)
    _s1, features_from_original = apply_event(state, next_event)
    _s2, features_from_restored = apply_event(restored, next_event)
    assert features_from_original == features_from_restored
