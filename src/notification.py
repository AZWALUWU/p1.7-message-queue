import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[NOTIFICATION SERVICE] Dispatching email confirmation to: {order_data['customer_email']}")
        print(f"[NOTIFICATION SERVICE] Content: 'Your order {order_data['order_id']} is being processed!'")
    return {'statusCode': 200, 'body': json.dumps('Notification dispatched.')}
