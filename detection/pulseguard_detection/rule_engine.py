"""A safe expression evaluator for versioned velocity rules.

Same principle as the build plan calls out: rules are versioned data (see
rules.json), evaluated by a restricted interpreter -- never `eval()`. An
`eval()`-based rule engine means anyone who can write a rule (or inject one
through a compromised admin panel, a bad deploy, or a supply-chain issue in
a rules-management UI) can run arbitrary Python on the scoring service.
This evaluator walks a parsed AST and only permits a small whitelist of
node types: boolean/comparison/arithmetic operators and variable lookups
against the feature dict. No attribute access, no subscripting, no
function calls, no imports -- so there is no code-execution surface at all,
regardless of what a rule string contains.
"""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ALLOWED_BOOL_OPS = (ast.And, ast.Or)
_ALLOWED_BIN_OPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod)
_ALLOWED_UNARY_OPS = (ast.Not, ast.USub)
_ALLOWED_COMPARE_OPS = (ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.Eq, ast.NotEq)


class UnsafeExpressionError(ValueError):
    """Raised when a rule expression contains a node type outside the
    evaluator's whitelist (attribute access, calls, subscripts, etc.)."""


def _eval_node(node: ast.AST, context: dict[str, Any]) -> Any:
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, context)

    if isinstance(node, ast.Constant):
        if isinstance(node.value, (bool, int, float)) or node.value is None:
            return node.value
        raise UnsafeExpressionError(f"unsupported constant type: {type(node.value)}")

    if isinstance(node, ast.Name):
        if node.id not in context:
            raise UnsafeExpressionError(f"unknown variable: {node.id}")
        return context[node.id]

    if isinstance(node, ast.BoolOp):
        if not isinstance(node.op, _ALLOWED_BOOL_OPS):
            raise UnsafeExpressionError(f"unsupported bool op: {node.op}")
        values = [_eval_node(v, context) for v in node.values]
        return all(values) if isinstance(node.op, ast.And) else any(values)

    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, _ALLOWED_UNARY_OPS):
            raise UnsafeExpressionError(f"unsupported unary op: {node.op}")
        operand = _eval_node(node.operand, context)
        return (not operand) if isinstance(node.op, ast.Not) else (-operand)

    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, _ALLOWED_BIN_OPS):
            raise UnsafeExpressionError(f"unsupported binary op: {node.op}")
        left, right = _eval_node(node.left, context), _eval_node(node.right, context)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right if right else float("inf")
        return left % right

    if isinstance(node, ast.Compare):
        left = _eval_node(node.left, context)
        for op, comparator in zip(node.ops, node.comparators):
            if not isinstance(op, _ALLOWED_COMPARE_OPS):
                raise UnsafeExpressionError(f"unsupported comparison: {op}")
            right = _eval_node(comparator, context)
            result = {
                ast.Lt: left < right,
                ast.LtE: left <= right,
                ast.Gt: left > right,
                ast.GtE: left >= right,
                ast.Eq: left == right,
                ast.NotEq: left != right,
            }[type(op)]
            if not result:
                return False
            left = right
        return True

    raise UnsafeExpressionError(f"unsupported expression node: {type(node).__name__}")


def safe_eval(expr: str, context: dict[str, Any]) -> bool:
    """Parses `expr` and evaluates it against `context`, raising
    UnsafeExpressionError for anything outside the whitelisted grammar."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise UnsafeExpressionError(f"invalid syntax: {e}") from e
    return bool(_eval_node(tree, context))


@dataclass(frozen=True)
class Rule:
    id: str
    expr: str
    severity: str
    version: int = 1

    def matches(self, context: dict[str, Any]) -> bool:
        try:
            return safe_eval(self.expr, context)
        except UnsafeExpressionError:
            raise
        except (TypeError, ZeroDivisionError):
            # a feature was None (e.g. secondsSincePrevTxn on a customer's
            # first-ever transaction) -- treat as "rule doesn't apply" rather
            # than crashing the scoring job on one customer's first event.
            return False


def load_rules(path: str | Path | None = None) -> list[Rule]:
    path = Path(path) if path else Path(__file__).parent / "rules.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    return [Rule(id=r["id"], expr=r["expr"], severity=r["severity"], version=r.get("version", 1)) for r in raw]


def evaluate_rules(rules: list[Rule], context: dict[str, Any]) -> list[Rule]:
    """Returns every rule that fires against this context, not just the
    first -- an alert can (and often should) carry multiple triggered
    rules for the analyst to see."""
    return [r for r in rules if r.matches(context)]
