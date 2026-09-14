from pulseguard_streaming.redis_writer import to_redis_hash


def test_none_becomes_empty_string():
    assert to_redis_hash({"secondsSincePrevTxn": None})["secondsSincePrevTxn"] == ""


def test_bool_becomes_zero_or_one():
    out = to_redis_hash({"isNewMerchantCategory": True, "isOutsideActiveHours": False})
    assert out["isNewMerchantCategory"] == "1"
    assert out["isOutsideActiveHours"] == "0"


def test_numbers_become_strings():
    out = to_redis_hash({"amountZScore": 3.14159, "txnCount1Min": 2})
    assert out["amountZScore"] == "3.14159"
    assert out["txnCount1Min"] == "2"
