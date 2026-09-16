from unittest.mock import MagicMock, patch

import redis

from pulseguard_streaming.redis_writer import write_partition

FEATURE_NAMES = ("amountZScore",)


def _row(customer_id="C1"):
    return {"customerId": customer_id, "txnId": "t1", "timestamp": "2026-01-01T00:00:00Z", "amountZScore": 1.5}


def test_redis_connection_error_does_not_propagate():
    """This is the failure drill from the build plan: killing Redis must
    degrade the pipeline, not crash it. A RedisError here previously
    propagated all the way out of foreachPartition, failing the Spark
    task and (via feature_job.main()'s awaitAnyTermination) taking the
    whole driver down with it."""
    with patch("redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_client.pipeline.return_value.execute.side_effect = redis.exceptions.ConnectionError("connection refused")
        mock_redis_cls.return_value = mock_client

        # must not raise
        write_partition([_row()], FEATURE_NAMES, redis_host="unreachable-host", redis_port=6379)


def test_redis_timeout_error_does_not_propagate():
    with patch("redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_client.pipeline.return_value.execute.side_effect = redis.exceptions.TimeoutError("timed out")
        mock_redis_cls.return_value = mock_client

        write_partition([_row()], FEATURE_NAMES, redis_host="slow-host", redis_port=6379)


def test_successful_write_still_calls_execute_and_close():
    with patch("redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_pipe = MagicMock()
        mock_client.pipeline.return_value = mock_pipe
        mock_redis_cls.return_value = mock_client

        write_partition([_row(), _row("C2")], FEATURE_NAMES, redis_host="localhost", redis_port=6379)

        assert mock_pipe.execute.called
        assert mock_client.close.called


def test_non_redis_exceptions_still_propagate():
    """Only Redis-specific errors are treated as an expected, degradable
    failure mode -- a bug in our own code (e.g. a KeyError from a
    malformed row) should still surface loudly, not be swallowed."""
    import pytest

    with patch("redis.Redis") as mock_redis_cls:
        mock_client = MagicMock()
        mock_client.pipeline.return_value.execute.side_effect = ValueError("not a redis problem")
        mock_redis_cls.return_value = mock_client

        with pytest.raises(ValueError):
            write_partition([_row()], FEATURE_NAMES, redis_host="localhost", redis_port=6379)
