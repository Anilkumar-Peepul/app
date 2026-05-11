import asyncio
import re
import json
import ssl
import paho.mqtt.client as mqtt
from datetime import datetime
from config import *
from node import Node
from storage import Storage
from logger import PayloadLogger

# Global from monitor.py (better to avoid, but keeping for now)
connected_nodes = set()

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

        # Define ACK topic
        self.motor_control_ack_topic = "motor_control_ack"

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("✅ MQTT Connected")
            self.is_connected = True
            self.subscribe_topics()
        else:
            print(f"❌ MQTT Failed: {rc}")

    def is_ipv6(self, address: str) -> bool:
        ipv6_pattern = r'^([0-9a-fA-F]{1,4}:){1,7}([0-9a-fA-F]{1,4}|:)$|^([0-9a-fA-F]{1,4}:)*::$|^::$'
        return bool(re.match(ipv6_pattern, address)) or '::' in address

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
            self.loop.create_task(self.handle_motor_control_message(payload))
        elif "mode_change" in msg.topic:
            self.loop.create_task(self.handle_mode_change(payload))
        elif "config" in msg.topic:
            self.loop.create_task(self.handle_config(payload))
        elif "motor/config" in msg.topic:
            self.handle_register(payload)

    async def handle_motor_control_message(self, message: dict):
        """Handle bulk motor control commands"""
        print("🔧 Motor Control Command Received:", message)
        
        dev_list = []
        dev_err_list = []
        tasks = []

        for device in message.get("dev", []):
            d_id = device.get("d_id")
            mtr_1 = device.get("mtr_1")
            mtr_2 = device.get("mtr_2")

            if not d_id:
                dev_err_list.append({"d_id": "N/A", "mtr_1": 8, "mtr_2": 8})
                continue

            # Determine IP Address
            if self.is_ipv6(d_id) and d_id in connected_nodes:
                ip = d_id
            else:
                ip = self.storage.mac_to_ip.get(d_id.upper())

            if not ip:
                print(f"❌ Device {d_id} not found")
                dev_err_list.append({"d_id": d_id, "mtr_1": 8, "mtr_2": 8})
                continue

            # Validate motor values (0=OFF, 1=ON)
            if mtr_1 is not None and mtr_1 not in [0, 1]:
                dev_err_list.append({"d_id": d_id, "mtr_1": 9})
                continue
            if mtr_2 is not None and mtr_2 not in [0, 1]:
                dev_err_list.append({"d_id": d_id, "mtr_2": 9})
                continue

            # Prepare payload
            mc_payload = {}
            if mtr_1 is not None:
                mc_payload["mtr_1"] = mtr_1
            if mtr_2 is not None:
                mc_payload["mtr_2"] = mtr_2

            # Create CoAP task
            node = Node(ipv6=ip, uri="motor_control", payload=json.dumps(mc_payload))
            tasks.append((d_id, mtr_1, mtr_2, node.put()))  # .put() is async

        # Execute all CoAP requests concurrently
        if tasks:
            results = await asyncio.gather(*[t[3] for t in tasks], return_exceptions=True)

            for (d_id, m1, m2, _), data in zip(tasks, results):
                ack = {"d_id": d_id}

                if isinstance(data, Exception) or data is None:
                    ack.update({"mtr_1": 10, "mtr_2": 10})
                    dev_err_list.append(ack)
                    continue

                if m1 is not None:
                    ack["mtr_1"] = data.get("mtr_1", m1) if isinstance(data, dict) else m1
                if m2 is not None:
                    ack["mtr_2"] = data.get("mtr_2", m2) if isinstance(data, dict) else m2

                dev_list.append(ack)

        # Send acknowledgments
        if dev_list:
            self.publish(self.motor_control_ack_topic, {"dev": dev_list})
        if dev_err_list:
            self.publish(self.motor_control_ack_topic, {"dev": dev_err_list})

    async def handle_mode_change(self, msg: dict):
        print("🔄 Mode Change Command Received:", msg)
        # TODO: Implement similar logic as motor control if needed

    async def handle_config(self, msg: dict):
        print("⚙️ Config Command Received:", msg)
        # TODO: Implement config logic

    def handle_register(self, msg: dict):
        d_id = msg.get("d_id")
        if d_id:
            ip = self.storage.mac_to_ip.get(d_id.upper())
            if ip:
                self.storage.register_device(d_id, ip)

    def publish(self, sub_topic: str, payload):
        """Publish message to MQTT"""
        if not self.is_connected:
            return
        try:
            if isinstance(payload, dict):
                payload = payload.copy()
                payload["t_s"] = int(datetime.utcnow().timestamp() * 1000)
                payload_str = json.dumps(payload)
            else:
                payload_str = payload

            topic = f"gateways/{self.gateway_name}/devices/{sub_topic}"
            self.client.publish(topic, payload_str, qos=0)
        except Exception as e:
            print(f"Publish error: {e}")
            self.logger.error(f"Publish error: {e}")

    def connect(self):
        self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        self.client.loop_start()

    def on_disconnect(self, client, userdata, rc):
        print(f"⚠️ MQTT Disconnected (Reason: {rc})")
        self.is_connected = False
