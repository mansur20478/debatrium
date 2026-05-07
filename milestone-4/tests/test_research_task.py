import json
import time
import hashlib
import uuid
import boto3

# CONFIG
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/245144441954/research-tasks-441954.fifo"

# sample payload
debate_id = "D-003"
round_num = 1
angle = "negative"
query = "What are the ethical implications of using AI in healthcare?"

# create SQS client
sqs = boto3.client("sqs", region_name="us-east-1")

def send_one():
    timestamp = int(time.time())


    message = {
        "debate_id": debate_id,
        "round": round_num,
        "query": query,
        "angle": angle,
    }

    # Each message gets its own group — no blocking between messages
    group_id = f"{debate_id}-{angle}-{round_num}"

    # Unique dedup ID per send — uuid4 guarantees no accidental deduplication
    dedup_id = str(uuid.uuid4())

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id,
    )

    # # FIFO requires these
    # group_id = debate_id
    # dedup_id = hashlib.md5(
    #     f"{debate_id}-{round_num}-{angle}-{timestamp}".encode()
    # ).hexdigest()

    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(message),
        MessageGroupId=group_id,
        MessageDeduplicationId=dedup_id
    )

    print("Sent message!")
    print("MessageId:", response["MessageId"])


if __name__ == "__main__":
    send_one()