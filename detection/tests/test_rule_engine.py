import pytest

from pulseguard_detection.rule_engine import (
    Rule,
    UnsafeExpressionError,
    evaluate_rules,
    load_rules,
    safe_eval,
)


def test_basic_comparison_and_boolean_logic():
    assert safe_eval("amount > 100 and amount < 200", {"amount": 150}) is True
    assert safe_eval("amount > 100 and amount < 200", {"amount": 250}) is False
    assert safe_eval("amount > 1000 or isNew", {"amount": 5, "isNew": True}) is True


def test_arithmetic_in_expression():
    assert safe_eval("amount > 10 * trailingMean", {"amount": 5000, "trailingMean": 100}) is True
    assert safe_eval("amount > 10 * trailingMean", {"amount": 500, "trailingMean": 100}) is False


def test_boolean_feature_used_directly():
    assert safe_eval("deviceIsNew and amountZScore > 3", {"deviceIsNew": True, "amountZScore": 5.0}) is True
    assert safe_eval("deviceIsNew and amountZScore > 3", {"deviceIsNew": False, "amountZScore": 5.0}) is False


@pytest.mark.parametrize(
    "dangerous_expr",
    [
        "__import__('os').system('echo pwned')",
        "().__class__.__bases__[0]",
        "open('/etc/passwd').read()",
        "[x for x in range(10)]",
        "amount if True else 0",
        "lambda: 1",
        "amount.bit_length()",
        "context['secret']",
    ],
)
def test_dangerous_expressions_are_rejected(dangerous_expr):
    with pytest.raises(UnsafeExpressionError):
        safe_eval(dangerous_expr, {"amount": 1})


def test_unknown_variable_raises():
    with pytest.raises(UnsafeExpressionError):
        safe_eval("nonexistentFeature > 5", {})


def test_rule_treats_none_feature_as_non_match_not_a_crash():
    rule = Rule(id="TEST", expr="secondsSincePrevTxn < 1", severity="LOW")
    assert rule.matches({"secondsSincePrevTxn": None}) is False


def test_load_rules_from_default_rules_json():
    rules = load_rules()
    assert len(rules) == 6
    ids = {r.id for r in rules}
    assert ids == {
        "CARD_TESTING",
        "AMOUNT_SPIKE",
        "ATO_SIGNATURE",
        "IMPOSSIBLE_TRAVEL",
        "MULE_PATTERN",
        "THRESHOLD_STRUCTURING",
    }


def test_evaluate_rules_can_return_multiple_matches():
    rules = [
        Rule(id="A", expr="amount > 100", severity="LOW"),
        Rule(id="B", expr="amount > 200", severity="LOW"),
        Rule(id="C", expr="amount > 900", severity="LOW"),
    ]
    fired = evaluate_rules(rules, {"amount": 250})
    assert {r.id for r in fired} == {"A", "B"}


def test_card_testing_rule_fires_on_burst_of_small_amounts():
    rules = load_rules()
    card_testing = next(r for r in rules if r.id == "CARD_TESTING")
    assert card_testing.matches({"maxTxnsInAny60SecWindowToday": 6, "amount": 10.0}) is True
    assert card_testing.matches({"maxTxnsInAny60SecWindowToday": 1, "amount": 10.0}) is False


def test_impossible_travel_rule():
    rules = load_rules()
    rule = next(r for r in rules if r.id == "IMPOSSIBLE_TRAVEL")
    assert rule.matches({"impliedSpeedKmh": 3000, "distanceFromPrevKm": 1150}) is True
    assert rule.matches({"impliedSpeedKmh": 60, "distanceFromPrevKm": 1150}) is False
