import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[BILLING SERVICE] Generating invoice for Order ID: {order_data['order_id']}")
        print(f"[BILLING SERVICE] Charging: Rp {order_data['total_price']}")
    return {'statusCode': 200, 'body': json.dumps('Billing completed.')}
