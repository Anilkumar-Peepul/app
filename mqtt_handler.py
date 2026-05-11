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

# Global connected_nodes (shared with monitor.py)
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
        # ACK Topics
        self.motor_control_ack_topic = "motor_control/ack"
        self.motor_mode_ack_topic = "motor_mode/ack"           # or motor_mode_control/ack
        self.device_sync_ack_topic = "sync/ack"
        self.device_config_ack_topic = "config/ack"

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
        """Handle bulk motor control commands and send ACK"""
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

            # Get IP Address
            if self.is_ipv6(d_id) and d_id in connected_nodes:
                ip = d_id
            else:
                ip = self.storage.mac_to_ip.get(d_id.upper())

            if not ip:
                print(f"❌ Device {d_id} not found in mappings")
                dev_err_list.append({"d_id": d_id, "mtr_1": 8, "mtr_2": 8})
                continue

            # Validate motor values
            if mtr_1 is not None and mtr_1 not in [0, 1]:
                dev_err_list.append({"d_id": d_id, "mtr_1": 9, "mtr_2": None})
                continue
            if mtr_2 is not None and mtr_2 not in [0, 1]:
                dev_err_list.append({"d_id": d_id, "mtr_1": None, "mtr_2": 9})
                continue

            # Prepare payload for node
            mc_payload = {}
            if mtr_1 is not None: mc_payload["mtr_1"] = mtr_1
            if mtr_2 is not None: mc_payload["mtr_2"] = mtr_2

            # Create CoAP task
            node = Node(ipv6=ip, uri="motor_control", payload=json.dumps(mc_payload))
            tasks.append((d_id, mtr_1, mtr_2, node.put()))

        # Execute all CoAP requests in parallel
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

        # === Send ACK to Cloud on correct topic ===
        ack_payload = {}
        if dev_list:
            ack_payload["dev"] = dev_list
        if dev_err_list:
            if "dev" in ack_payload:
                ack_payload["dev"].extend(dev_err_list)
            else:
                ack_payload["dev"] = dev_err_list

        if ack_payload:
            self.publish("motor_control/ack", ack_payload)
        else:
            print("No devices processed for motor control")
    async def handle_motor_mode_control_message(self, message: dict):
        """Handle Motor Mode Control (2 → OFF, 3 → ON style)"""
        print("🔄 Motor Mode Control Command Received:", message)
        
        dev_list = []
        dev_err_list = []
        tasks = []

        for device in message.get("dev", []):
            d_id = device.get("d_id")
            mtr_1 = device.get("mtr_1")
            mtr_2 = device.get("mtr_2")

            # 1. Missing d_id
            if not d_id:
                dev_err_list.append({"d_id": "N/A", "mtr_1": 8, "mtr_2": 8})
                continue

            # 2. Resolve IP
            if self.is_ipv6(d_id) and d_id in connected_nodes:
                ip = d_id
            else:
                ip = self.storage.mac_to_ip.get(d_id.upper())

            if not ip:
                dev_err_list.append({"d_id": d_id, "mtr_1": 8, "mtr_2": 8})
                continue

            # 3. Validate values (only 2 or 3 allowed)
            if mtr_1 is not None and (not isinstance(mtr_1, int) or mtr_1 not in [2, 3]):
                dev_err_list.append({"d_id": d_id, "mtr_1": 9, "mtr_2": None})
                continue
            if mtr_2 is not None and (not isinstance(mtr_2, int) or mtr_2 not in [2, 3]):
                dev_err_list.append({"d_id": d_id, "mtr_1": None, "mtr_2": 9})
                continue

            # 4. Convert to node format: 2→0, 3→1
            mc_payload = {}
            send_m1 = 0 if mtr_1 == 2 else 1 if mtr_1 == 3 else None
            send_m2 = 0 if mtr_2 == 2 else 1 if mtr_2 == 3 else None

            if send_m1 is not None:
                mc_payload["mtr_1"] = send_m1
            if send_m2 is not None:
                mc_payload["mtr_2"] = send_m2

            # Create CoAP task
            node = Node(ipv6=ip, uri="cm_change", payload=json.dumps(mc_payload))
            tasks.append((d_id, mtr_1, mtr_2, node.put()))   # Use .put()

        # Execute all tasks concurrently
        if tasks:
            results = await asyncio.gather(*[t[3] for t in tasks], return_exceptions=True)

            for (d_id, orig_m1, orig_m2, _), data in zip(tasks, results):
                ack = {"d_id": d_id}

                if isinstance(data, Exception) or data is None:
                    ack.update({"mtr_1": 10, "mtr_2": 10})
                    dev_err_list.append(ack)
                    continue

                try:
                    if isinstance(data, dict):
                        # Map back: 0→2, 1→3
                        if orig_m1 is not None:
                            val = data.get("mtr_1", 0 if orig_m1 == 2 else 1)
                            ack["mtr_1"] = 2 if val == 0 else 3 if val == 1 else val
                        if orig_m2 is not None:
                            val = data.get("mtr_2", 0 if orig_m2 == 2 else 1)
                            ack["mtr_2"] = 2 if val == 0 else 3 if val == 1 else val
                    else:
                        # Fallback
                        if orig_m1 is not None: ack["mtr_1"] = orig_m1
                        if orig_m2 is not None: ack["mtr_2"] = orig_m2

                    dev_list.append(ack)
                except Exception:
                    ack.update({"mtr_1": 10, "mtr_2": 10})
                    dev_err_list.append(ack)

        # Publish ACK(s) to correct topic
        if dev_list:
            self.publish(self.motor_mode_ack_topic, {"dev": dev_list})
        if dev_err_list:
            self.publish(self.motor_mode_ack_topic, {"dev": dev_err_list})

    # ====================== SYNC DEVICE ======================
    async def handle_sync_device(self, ip: str, d_id: str):
        """Sync motor status from device"""
        try:
            data = await Node(ip, "info/motor_status").get()   # Use .get() for reading

            if data is None:
                print(f"❌ No data received for sync {ip}")
                return

            ack_payload = {
                "d_id": d_id,
                **data  # merge device data
            }

            self.publish(self.device_sync_ack_topic, ack_payload)
            print(f"✅ Sync successful for {d_id}")

        except Exception as e:
            self.logger.error(f"Sync error for {d_id}: {e}")
            error_ack = {"d_id": d_id, "status": "failed", "error": 10}
            self.publish(self.device_sync_ack_topic, error_ack)

    # ====================== CONFIG HANDLER ======================
    async def handle_config(self, ip: str, message: str):
        """Handle device configuration update"""
        sn = ""
        d_id = "N/A"
        try:
            # Parse incoming message
            try:
                msg_dict = json.loads(message) if isinstance(message, str) else message
                sn = msg_dict.get("sn", "")
                d_id = msg_dict.get("d_id", "N/A")
            except Exception as e:
                print(f"❌ Invalid JSON in config: {e}")
                raise

            # Step 1: Start Update
            state = await asyncio.wait_for(
                Node(ip, "config", message).put(),
                timeout=COAP_PUT_TIMEOUT
            )

            if not (state and isinstance(state, dict) and state.get("config_sts") == "UPDATE_STARTED"):
                print(f"❌ Config update not started for {ip}")
                self.publish(self.device_config_ack_topic, {"sn": sn, "d_id": d_id, "r": 0})
                return

            # Step 2: Wait and get final status
            await asyncio.sleep(5)
            data = await asyncio.wait_for(
                Node(ip, "config").put(),
                timeout=COAP_PUT_TIMEOUT
            )

            error_statuses = {"PARSE_FAILED", "MAC_MISMATCH", "VERIFY_FAILED",
                              "ERASE_FAILED", "WRITE_FAILED", "UPDATE_PENDING", "PAYLOAD_TOO_LARGE"}

            config_status = data.get("config_sts", "") if isinstance(data, dict) else ""

            if config_status in error_statuses or not isinstance(data, dict):
                print(f"❌ Config failed for {ip}: {config_status}")
                ack = {"sn": sn, "d_id": d_id, "r": 0}
            else:
                print(f"✅ Config successful for {ip}")
                ack = {
                    "sn": sn,
                    "d_id": data.get("d_id", d_id),
                    "r": 1
                }

            self.publish(self.device_config_ack_topic, ack)

        except asyncio.TimeoutError:
            print(f"⏰ Timeout during config for {ip}")
            self.publish(self.device_config_ack_topic, {"sn": sn, "d_id": d_id, "r": 0})
        except Exception as e:
            self.logger.error(f"Config error for {ip}: {e}")
            self.publish(self.device_config_ack_topic, {"sn": sn, "d_id": d_id, "r": 0})

    def handle_register(self, msg: dict):
        d_id = msg.get("d_id")
        if d_id:
            ip = self.storage.mac_to_ip.get(d_id.upper())
            if ip:
                self.storage.register_device(d_id, ip)

    def publish(self, sub_topic: str, payload):
        """Publish to gateways/<GATEWAY_NAME>/devices/<sub_topic>"""
        if not self.is_connected:
            return
        try:
            if isinstance(payload, dict):
                payload = payload.copy()
                payload["t_s"] = int(datetime.utcnow().timestamp() * 1000)
                payload_str = json.dumps(payload)
            else:
                payload_str = str(payload)

            topic = f"gateways/{self.gateway_name}/devices/{sub_topic}"
            self.client.publish(topic, payload_str, qos=0)
            print(f"📤 Published ACK to: {topic}")
        except Exception as e:
            print(f"Publish error: {e}")
            self.logger.error(f"Publish error: {e}")

    def connect(self):
        self.client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        self.client.loop_start()

    def on_disconnect(self, client, userdata, rc):
        print(f"⚠️ MQTT Disconnected (Reason: {rc})")
        self.is_connected = False
