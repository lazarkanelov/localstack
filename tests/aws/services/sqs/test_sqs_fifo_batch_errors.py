import pytest

from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid


class TestSqsFifoBatchErrors:
    @markers.aws.validated
    @pytest.mark.parametrize("protocol", ["sqs", "sqs_query"])
    def test_fifo_batch_delay_seconds_error_code_matches_aws(
        self, sqs_create_queue, aws_client, protocol
    ):
        # For FIFO queues, per-message DelaySeconds is not valid; AWS returns Failed entries with Code 'InvalidParameterValue'
        queue_url = sqs_create_queue(
            QueueName=f"queue-{short_uid()}.fifo",
            Attributes={
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )

        client = getattr(aws_client, protocol)

        resp = client.send_message_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "Id": "1",
                    "MessageBody": "test",
                    "MessageGroupId": "g1",
                    "MessageDeduplicationId": f"d-{short_uid()}",
                    "DelaySeconds": 1,
                }
            ],
        )

        assert resp.get("Failed"), "Expected the batch request to contain Failed entries"
        assert resp["Failed"][0]["Code"] == "InvalidParameterValue"
