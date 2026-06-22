import json

def lambda_handler(event, context):
    print(f"--- Menghandle batch berisi {len(event['Records'])} pesan SQS ---")
    
    for record in event['Records']:
        sqs_body = json.loads(record['body'])
        order_data = json.loads(sqs_body['Message'])
        
        print(f"[NOTIFICATION SERVICE] Mengirim email konfirmasi ke: {order_data['customer_email']}")
        print(f"[NOTIFICATION SERVICE] Pesan: 'Halo, order {order_data['order_id']} Anda sedang diproses!'")
        
    return {
        'statusCode': 200,
        'body': json.dumps('Notification sent successfully')
    }
