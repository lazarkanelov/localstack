import time

import pytest

from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid


class TestSqsFifoDelayBatch:
    @markers.aws.validated
    @pytest.mark.parametrize("protocol", ["sqs", "sqs_query"])
    def test_fifo_queue_send_message_batch_zero_delay_defaults_to_queue_delay(
        self, sqs_create_queue, aws_client, protocol
    ):
        # ensure that in FIFO queues, per-message DelaySeconds=0 in a batch does not override the queue DelaySeconds
        delay_seconds = 2
        queue_url = sqs_create_queue(
            QueueName=f"queue-{short_uid()}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
                "DelaySeconds": str(delay_seconds),
            },
        )

        client = getattr(aws_client, protocol)

        client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": "1",
                    "MessageBody": "message-1",
                    "MessageGroupId": "g1",
                    "MessageDeduplicationId": f"d-{short_uid()}",
                    "DelaySeconds": 0,
                },
                {
                    "Id": "2",
                    "MessageBody": "message-2",
                    "MessageGroupId": "g1",
                    "MessageDeduplicationId": f"d-{short_uid()}",
                    "DelaySeconds": 0,
                },
            ],
        )

        # messages should not be visible immediately
        initial = client.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        assert initial.get("Messages", []) == []

        time.sleep(delay_seconds + 1)

        # after the queue delay, both messages should be visible in order
        after = client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=2)
        messages = after["Messages"]
        assert len(messages) == 2
        assert messages[0]["Body"] == "message-1"
        assert messages[1]["Body"] == "message-2"
