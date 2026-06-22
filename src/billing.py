import json

def lambda_handler(event, context):
    print(f"--- Menghandle batch berisi {len(event['Records'])} pesan SQS ---")
    
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[BILLING SERVICE] Membuat tagihan untuk Order ID: {order_data['order_id']}")
        print(f"[BILLING SERVICE] Total Biaya: Rp{order_data['total_price']}")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Billing processed successfully')
    }
