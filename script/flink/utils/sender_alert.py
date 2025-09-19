import requests
from pyflink.datastream.functions import MapFunction

class SendAlertToWebhook(MapFunction):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def map(self, fraud_alert):
        message = str(fraud_alert)
        send_alert_to_third_party(message, self.webhook_url)
        return fraud_alert  # vẫn trả về để có thể in ra hoặc xử lý tiếp

def send_alert_to_third_party(alert_message, webhook_url):
    """Gửi cảnh báo tới Discord hoặc Slack qua Webhook"""
    data = {
        "content": f"🚨 **FRAUD DETECTED**\n{alert_message}"
    }
    try:
        response = requests.post(webhook_url, json=data)
        if response.status_code == 204:
            print("✅ Alert sent successfully!")
        else:
            print(f"❌ Failed to send alert: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending alert: {e}")
