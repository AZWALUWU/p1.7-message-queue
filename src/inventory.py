import json

def lambda_handler(event, context):
    print(f"--- Processing batch of {len(event['Records'])} SQS messages ---")
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[INVENTORY SERVICE] Deducting stock for Order ID: {order_data['order_id']}")
        print(f"[INVENTORY SERVICE] SKU: {order_data['item']}, Qty: {order_data['quantity']}")
    return {'statusCode': 200, 'body': json.dumps('Inventory verified.')}
