import json

def lambda_handler(event, context):
    print(f"--- Menghandle batch berisi {len(event['Records'])} pesan SQS ---")
    
    for record in event['Records']:
        # 1. Parse SQS Body (isinya adalah payload dari SNS)
        sqs_body = json.loads(record['body'])
        
        # 2. Parse SNS Message (isinya adalah data order asli)
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[INVENTORY SERVICE] Memperbarui stok untuk Order ID: {order_data['order_id']}")
        print(f"[INVENTORY SERVICE] Item: {order_data['item']}, Jumlah: {order_data['quantity']}")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Inventory processed successfully')
    }
