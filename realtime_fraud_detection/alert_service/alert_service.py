import os
import time
import json
import logging
from datetime import datetime
from elasticsearch import Elasticsearch
import requests
from flask import Flask, jsonify

# Cấu hình logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("alert_service")

# Cấu hình service
ES_HOST = os.environ.get("ES_HOST", "elasticsearch")
ES_PORT = os.environ.get("ES_PORT", "9200")
ES_INDEX = os.environ.get("ES_INDEX", "transactions_index")
ALERT_THRESHOLD = float(os.environ.get("ALERT_THRESHOLD", 30000.0))
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", 10))  # seconds
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

# Flask app cho API
app = Flask(__name__)

def connect_to_elasticsearch():
    """Tạo kết nối đến Elasticsearch."""
    retries = 0
    max_retries = 10
    
    while retries < max_retries:
        try:
            es = Elasticsearch([f'http://{ES_HOST}:{ES_PORT}'])
            if es.ping():
                logger.info("Kết nối thành công đến Elasticsearch")
                return es
            else:
                logger.warning("Không ping được Elasticsearch")
        except Exception as e:
            logger.error(f"Lỗi kết nối đến Elasticsearch: {e}")
        
        retries += 1
        logger.info(f"Thử kết nối lại sau 5 giây (lần {retries}/{max_retries})")
        time.sleep(5)
    
    logger.error("Không thể kết nối đến Elasticsearch sau nhiều lần thử")
    return None

def check_suspicious_transactions(es_client):
    """Kiểm tra các giao dịch đáng ngờ mới."""
    try:
        # Truy vấn ES tìm giao dịch đáng ngờ trong 1 phút qua
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"is_suspicious_rule": True}},
                        {"range": {"@timestamp": {"gte": "now-1m"}}}
                    ]
                }
            },
            "sort": [{"@timestamp": {"order": "desc"}}],
            "size": 20
        }
        
        result = es_client.search(index=ES_INDEX, body=query)
        suspicious_transactions = result['hits']['hits']
        
        if suspicious_transactions:
            logger.info(f"Phát hiện {len(suspicious_transactions)} giao dịch đáng ngờ!")
            for transaction in suspicious_transactions:
                t = transaction['_source']
                send_alert(t)
                
        return suspicious_transactions
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra giao dịch đáng ngờ: {e}")
        return []

def send_alert(transaction):
    """Gửi thông báo khi phát hiện giao dịch đáng ngờ."""
    try:
        # Format thông báo
        alert_msg = {
            "title": "PHÁT HIỆN GIAO DỊCH ĐÁNG NGỜ",
            "transaction_id": transaction.get('transaction_id'),
            "user_id": transaction.get('user_id'),
            "amount": transaction.get('amount'),
            "currency": transaction.get('currency'),
            "location": transaction.get('location'),
            "timestamp": transaction.get('@timestamp'),
            "reason": f"Giao dịch vượt ngưỡng {ALERT_THRESHOLD} VND"
        }
        
        logger.info(f"CẢNH BÁO: {alert_msg}")
        
        # Gửi webhook nếu được cấu hình
        if WEBHOOK_URL:
            response = requests.post(
                WEBHOOK_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(alert_msg)
            )
            logger.info(f"Gửi webhook: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Lỗi khi gửi thông báo: {e}")

@app.route('/health', methods=['GET'])
def health_check():
    """API endpoint kiểm tra trạng thái service."""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/alerts/recent', methods=['GET'])
def recent_alerts():
    """API endpoint để lấy các cảnh báo gần đây."""
    es = connect_to_elasticsearch()
    if not es:
        return jsonify({"error": "Không thể kết nối đến Elasticsearch"}), 500
        
    alerts = check_suspicious_transactions(es)
    return jsonify({
        "count": len(alerts),
        "alerts": [alert['_source'] for alert in alerts]
    })

def alert_monitor_loop():
    """Hàm chạy vòng lặp liên tục kiểm tra giao dịch đáng ngờ."""
    es = connect_to_elasticsearch()
    if not es:
        logger.error("Không thể khởi động dịch vụ cảnh báo do không kết nối được Elasticsearch")
        return
        
    logger.info("Bắt đầu giám sát giao dịch đáng ngờ...")
    
    try:
        while True:
            check_suspicious_transactions(es)
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Dừng dịch vụ cảnh báo.")
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {e}")

if __name__ == "__main__":
    # Chạy 2 thread: một cho API và một cho việc giám sát liên tục
    import threading
    
    # Thread cho API
    api_thread = threading.Thread(target=app.run, kwargs={"host": "0.0.0.0", "port": 5000})
    api_thread.daemon = True
    api_thread.start()
    
    # Thread giám sát
    alert_monitor_loop()