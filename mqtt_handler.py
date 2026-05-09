import asyncio
import json
import ssl
import paho.mqtt.client as mqtt
from datetime import datetime
from .config import *
from .node import Node
from .storage import Storage
from .logger import PayloadLogger

class MQTTHandler:
    def __init__(self, storage: Storage, logger: PayloadLogger):
        self.storage = storage
        self.logger = logger
        self.gateway_name = GATEWAY_NAME
        self.is_connected = False
        self.loop = asyncio.get_event_loop()

        self.client = mqtt.Client(client_id=self.gateway_name)
        self.client.username_pw_set(MQTT_USER, MQTT_PASS)
        self.client.tls_set(ca_certs=CA_CERT, tls_version=ssl.PROTOCOL_TLSv1_2)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("✅ MQTT Connected")
            self.is_connected = True
            self.subscribe_topics()
        else:
            print(f"❌ MQTT Failed: {rc}")

    def subscribe_topics(self):
        base = f"gateways/{self.gateway_name}/devices"
        topics = [
            (base, 0),
            (f"{base}/motor_control", 0),
            (f"{base}/config", 0),
            (f"{base}/motor/config", 0),
            (f"{base}/mode_change", 0),
        ]
        self.client.subscribe(topics)

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except:
            return

        if "motor_control" in msg.topic:
            self.loop.create_task(self.handle_motor_control(payload))
        elif "mode_change" in msg.topic:
            self.loop.create_task(self.handle_mode_change(payload))
        elif "config" in msg.topic:
            self.loop.create_task(self.handle_config(payload))
        elif "motor/config" in msg.topic:
            self.handle_register(payload)

    async def handle_motor_control(self, msg):
        print("🔧 Motor Control Command Received")
        # Add your full logic here if needed

    async def handle_mode_change(self, msg):
        print("🔄 Mode Change Command Received")

    async def handle_config(self, msg):
        print("⚙️ Config Command Received")

    def handle_register(self, msg):
        d_id = msg.get("d_id")
        ip = self.storage.mac_to_ip.get(d_id)
        if ip:
            self.storage.register_device(d_id, ip)

    def publish(self, sub_topic: str, payload: dict):
        if not self.is_connected:
            return
        try:
            payload["t_s"] = int(datetime.utcnow().timestamp() * 1000)
            topic = f"gateways/{self.gateway_name}/devices/{sub_topic}"
            self.client.publish(topic, json.dumps(payload), qos=0)
        except Exception as e:
            print(f"Publish error: {e}")

    def connect(self):
        self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        self.client.loop_start()
