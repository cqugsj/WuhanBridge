import paho.mqtt.client as mqtt
from collections import defaultdict
from datetime import datetime

# MQTT配置
MQTT_BROKER_PRIMARY = '192.10.39.3'
MQTT_PORT = 1883
MQTT_USERNAME = 'wuhan'
MQTT_PASSWORD = 'wuhan2023'
MQTT_TOPIC_HEIGHT = 'WuHan/+/height/#'
MQTT_TOPIC_AIS = 'WuHan/+/ais/device1/dynamic'

# 用于存储最新的AIS消息
latest_ais_messages = defaultdict(dict)


# 回调函数：连接成功
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(MQTT_TOPIC_HEIGHT)
    client.subscribe(MQTT_TOPIC_AIS)


# 回调函数：收到消息
def on_message(client, userdata, msg):
    print(f"Received message on topic: {msg.topic}")
    print(f"Message payload: {msg.payload.decode()}")

    if 'ais' in msg.topic:
        process_ais_message(msg)


# 处理AIS消息
def process_ais_message(msg):
    try:
        payload = msg.payload.decode().split(',')
        if len(payload) == 8:
            longitude, latitude, mmsi, course, speed, timestamp, _, receiver_ip = payload
            mmsi = int(mmsi)
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')

            # 检查是否是最新的消息
            if mmsi not in latest_ais_messages or timestamp > latest_ais_messages[mmsi].get('timestamp'):
                latest_ais_messages[mmsi] = {
                    'timestamp': timestamp,
                    'data': {
                        'longitude': float(longitude),
                        'latitude': float(latitude),
                        'course': float(course),
                        'speed': float(speed),
                        'receiver_ip': receiver_ip
                    }
                }
                print(f"Updated AIS message for MMSI {mmsi} from receiver {receiver_ip}")
            else:
                print(f"Discarded duplicate AIS message for MMSI {mmsi}")
        else:
            print("Invalid AIS message format")
    except Exception as e:
        print(f"Error processing message: {e}")


# 创建MQTT客户端
client = mqtt.Client()
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
client.on_connect = on_connect
client.on_message = on_message

# 连接到MQTT代理
client.connect(MQTT_BROKER_PRIMARY, MQTT_PORT, 60)

# 开始循环
client.loop_forever()