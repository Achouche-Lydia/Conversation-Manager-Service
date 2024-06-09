from flask import Flask, request, jsonify
from util.kafka_client import KafkaClient
import uuid
import threading
import time
import logging

app = Flask(__name__)

kafka_client = KafkaClient('localhost:9092')
producer = kafka_client.create_producer()
consumer = kafka_client.create_consumer('text-topic', 'conversation-manager-group')

responses = {}

# Configure logging settings
logging.basicConfig(level=logging.INFO)

@app.route('/api/conversation-manager', methods=['POST'])
def conversation_manager():
    logging.info("Received API request to Conversation Manager.")
    
    if 'audio' not in request.files:
        return {"message": "No audio file provided"}, 400
    
    audio_file = request.files['audio']
    audio_data = audio_file.read()

    correlation_id = str(uuid.uuid4())
    headers = [('correlation_id', correlation_id.encode('utf-8'))]
    logging.info("Sending audio data to Kafka...")
    
    try:
        producer.send('audio-topic', value=audio_data, headers=headers).get(timeout=10)
        logging.info("Audio data sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send audio data to Kafka: {e}")
        return {"message": "Failed to send audio data to Kafka"}, 500

    timeout = 300  # seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        if correlation_id in responses:
            response_text = responses.pop(correlation_id)
            return jsonify({'response': response_text}), 200
        time.sleep(0.5)

    logging.error("STT service did not respond in time.")
    return {"message": "STT service did not respond in time"}, 504

def consume_text():
    for msg in consumer:
        correlation_id = dict(msg.headers).get(b'correlation_id', b'').decode('utf-8')
        text_data = msg.value.decode('utf-8')
        responses[correlation_id] = text_data

consumer_thread = threading.Thread(target=consume_text)
consumer_thread.daemon = True
consumer_thread.start()

if __name__ == '__main__':
    app.run(port=5002, debug=True)
