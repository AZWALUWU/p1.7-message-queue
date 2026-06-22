# Message Queue Architecture: SNS + SQS Fan-Out Pattern

This project implements an asynchronous, event-driven order processing system utilizing an **AWS SNS + SQS Fan-Out Architecture**. Built and tested entirely in a local environment using **LocalStack (v3.5.0)** and **Docker**, this architecture demonstrates how to achieve loose coupling, high fault tolerance, and parallel batch processing in modern microservices.

---

## 🏗️ System Architecture

When a client publishes a new order to the central SNS Topic, the message is instantly replicated and pushed to three decoupled SQS queues. Each queue triggers an independent AWS Lambda function to execute specialized business logic concurrently.

```
       [ Client / Publisher ]
                 │ (Publish Order)
                 ▼
         ┌───────────────┐
         │   SNS Topic   │ (orders-topic)
         └───────┬───────┘
                 │
      ┌──────────┼──────────┐  (Fan-out Distribution)
      ▼          ▼          ▼
 ┌─────────┐┌─────────┐┌──────────┐
 │ SQS Q   ││ SQS Q   ││ SQS Q    │
 │Inventory││ Billing ││Notif     │
 └────┬────┘└────┬────┘└────┬─────┘
      │          │          │
      ▼          ▼          ▼
 ┌─────────┐┌─────────┐┌──────────┐
 │ Lambda  ││ Lambda  ││ Lambda   │
 │Inventory││ Billing ││Notif     │
 └─────────┘└────┬────┘└──────────┘
                 │ (On Failure)
                 ▼
            ┌─────────┐
            │ SQS DLQ │ (billing-dlq)
            └─────────┘

```

---

## 📊 Deep Dive: Message Queue vs. Direct API Call

| Characteristic | Direct API Call (Synchronous) | Message Queue (Asynchronous) |
| --- | --- | --- |
| **Coupling** | **Tightly Coupled.** Service A must directly know the network address/URL of Service B. | **Loosely Coupled.** Service A only knows the Topic/Queue endpoint. It is blind to who consumes it. |
| **Availability** | If Service B is down, the entire transaction fails immediately (**Single Point of Failure**). | If a consumer service drops, messages remain safely buffered inside the Queue until it recovers. |
| **Performance & Latency** | The client waits for all downstream services to finish processing (Accumulative Latency). | The client receives an instant acknowledgment as soon as the message lands in the queue. |
| **Traffic Spike Resilience** | Sudden traffic bursts can exhaust server resources, causing service crashes. | Queues act as a **load buffer**, allowing consumers to ingest data safely at their own pace. |

> **Key Takeaway:** Use **Message Queues** when immediate execution feedback is not structurally required by the sender (e.g., generating an invoice, updating fulfillment databases, or firing transactional emails).

---

## ⚙️ Core Concepts Covered

* **SNS Fan-out Pattern:** Allows a single published message to be duplicated and distributed to multiple subscriber queues simultaneously, enabling parallel processing without modifying publisher logic.
* **SQS Standard vs. FIFO:** Standard queues provide nearly unlimited throughput with at-least-once delivery (used here). FIFO queues guarantee exact ordering and strictly exactly-once processing, vital for critical banking ledgers.
* **Message Visibility Timeout:** The period during which SQS hides a message from other consumers while one consumer handles it. If processing fails, the timeout expires and the message reappears for retry.
* **Dead Letter Queue (DLQ):** A safety net queue where persistently failing ("poison pill") messages are isolated for isolated developer diagnosis without blocking the primary highway.

---

## 🚀 Step-by-Step Implementation Guide

### Step 1: Environment Setup & LocalStack Up

We use LocalStack to emulate AWS core services locally inside a lightweight Docker container.

#### 1. Create the `docker-compose.yml` file:

```yaml
version: "3.8"

services:
  localstack:
    container_name: localstack_main
    image: localstack/localstack:3.5.0
    ports:
      - "127.0.0.1:4566:4566"            # LocalStack Gateway Port
      - "127.0.0.1:4510-4559:4510-4559"  # Port range for internal services
    environment:
      - SERVICES=sqs,sns,lambda          # Explicitly spin up needed services
      - AWS_DEFAULT_REGION=us-east-1
    volumes:
      - "${LOCALSTACK_VOLUME_DIR:-./volume}:/var/lib/localstack"
      - "/var/run/docker.sock:/var/run/docker.sock"

```

#### 2. Start the local infrastructure:

```powershell
docker compose up -d

```

#### 3. Configure a dummy AWS CLI profile for local routing:

```powershell
aws configure set aws_access_key_id mock_key --profile localstack
aws configure set aws_secret_access_key mock_secret --profile localstack
aws configure set region us-east-1 --profile localstack

```

---

### Step 2: Provision Infrastructure (SNS & SQS)

Run these commands sequentially to construct the fan-out topology:

#### 1. Create the SNS Topic:

```powershell
aws --endpoint-url=http://localhost:4566 --profile localstack sns create-topic --name orders-topic

```

#### 2. Create SQS Queues & the Dead Letter Queue:

```powershell
aws --endpoint-url=http://localhost:4566 --profile localstack sqs create-queue --queue-name billing-dlq
aws --endpoint-url=http://localhost:4566 --profile localstack sqs create-queue --queue-name inventory-queue
aws --endpoint-url=http://localhost:4566 --profile localstack sqs create-queue --queue-name billing-queue
aws --endpoint-url=http://localhost:4566 --profile localstack sqs create-queue --queue-name notification-queue

```

#### 3. Subscribe the SQS Queues to the SNS Topic:

```powershell
aws --endpoint-url=http://localhost:4566 --profile localstack sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:orders-topic --protocol sqs --notification-endpoint arn:aws:sqs:us-east-1:000000000000:inventory-queue

aws --endpoint-url=http://localhost:4566 --profile localstack sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:orders-topic --protocol sqs --notification-endpoint arn:aws:sqs:us-east-1:000000000000:billing-queue

aws --endpoint-url=http://localhost:4566 --profile localstack sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:orders-topic --protocol sqs --notification-endpoint arn:aws:sqs:us-east-1:000000000000:notification-queue

```

---

### Step 3: Develop Lambda Business Logic

Create a directory named `src/` and save the following three worker scripts. Note that because SNS wraps payloads before forwarding them to SQS, the code parses the nested JSON payloads sequentially.

#### 1. `src/inventory.py`

```python
import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[INVENTORY SERVICE] Deducting stock for Order ID: {order_data['order_id']}")
        print(f"[INVENTORY SERVICE] SKU: {order_data['item']}, Qty: {order_data['quantity']}")
    return {'statusCode': 200, 'body': json.dumps('Inventory verified.')}

```

#### 2. `src/billing.py`

```python
import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[BILLING SERVICE] Generating invoice for Order ID: {order_data['order_id']}")
        print(f"[BILLING SERVICE] Charging: Rp {order_data['total_price']}")
    return {'statusCode': 200, 'body': json.dumps('Billing completed.')}

```

#### 3. `src/notification.py`

```python
import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[NOTIFICATION SERVICE] Dispatching email confirmation to: {order_data['customer_email']}")
        print(f"[NOTIFICATION SERVICE] Content: 'Your order {order_data['order_id']} is being processed!'")
    return {'statusCode': 200, 'body': json.dumps('Notification dispatched.')}

```

#### 4. Compress & Deploy Workers to LocalStack:

```powershell
# Compress scripts to zip files
Compress-Archive -Path src/inventory.py -DestinationPath src/inventory.zip -Force
Compress-Archive -Path src/billing.py -DestinationPath src/billing.zip -Force
Compress-Archive -Path src/notification.py -DestinationPath src/notification.zip -Force

# Create Lambda Functions
aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-function --function-name inventory-service --runtime python3.10 --role arn:aws:iam::000000000000:role/lambda-role --handler inventory.lambda_handler --zip-file fileb://src/inventory.zip

aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-function --function-name billing-service --runtime python3.10 --role arn:aws:iam::000000000000:role/lambda-role --handler billing.lambda_handler --zip-file fileb://src/billing.zip

aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-function --function-name notification-service --runtime python3.10 --role arn:aws:iam::000000000000:role/lambda-role --handler notification.lambda_handler --zip-file fileb://src/notification.zip

```

---

### Step 4: Map Event Sources & Test

#### 1. Create Event Source Mappings (Bind SQS events to trigger Lambda):

```powershell
aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-event-source-mapping --function-name inventory-service --batch-size 10 --event-source-arn arn:aws:sqs:us-east-1:000000000000:inventory-queue

aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-event-source-mapping --function-name billing-service --batch-size 10 --event-source-arn arn:aws:sqs:us-east-1:000000000000:billing-queue

aws --endpoint-url=http://localhost:4566 --profile localstack lambda create-event-source-mapping --function-name notification-service --batch-size 10 --event-source-arn arn:aws:sqs:us-east-1:000000000000:notification-queue

```

#### 2. Draft test payload (`order.json`):

```json
{
  "order_id": "ORD-998877",
  "item": "MacBook Pro M3",
  "quantity": 1,
  "total_price": 35000000,
  "customer_email": "aza@example.com"
}

```

#### 3. Execute End-to-End Simulation:

Publish the order file payload to the SNS Topic using the command below:

```powershell
aws --endpoint-url=http://localhost:4566 --profile localstack sns publish --topic-arn arn:aws:sns:us-east-1:000000000000:orders-topic --message file://order.json

```

#### 4. Validate Async Executions:

Check your Docker engine log streams to confirm that the single publish step concurrently invoked the Lambdas with HTTP status code logs (`202 Accepted` invocation response):

```powershell
docker logs localstack_main

```

---

### Step 5: Repository Management

Keep deployment files and runtime assets isolated out of Git history by adding a `.gitignore` file:

```text
# LocalStack data engine volumes
volume/
.localstack/

# Compiled lambda binary deployment layers
src/*.zip

# OS temporary metadata caches
.DS_Store
Thumbs.db

```

Initialize your repository cleanly:

```powershell
git init
git add .
git commit -m "feat: complete SQS and SNS message queue architecture with localstack"

```
