import asyncio
import signal
from monitor import WiSunMonitor
from storage import Storage
from logger import PayloadLogger
from mqtt_handler import MQTTHandler
from config import GATEWAY_NAME
from constants import LIVE_PERIOD

class WiSunGateway:
    def __init__(self):
        self.storage = Storage()
        self.logger = PayloadLogger()
        self.monitor = WiSunMonitor()
        self.mqtt = MQTTHandler(self.storage, self.logger)

        self.storage.load_ip_mac()

    async def periodic_monitor(self):
        while True:
            if self.mqtt.is_connected:
                nodes = await self.monitor.get_nodes()
                for ip in nodes:
                    data = await Node(ip, "info/motor_status").get()
                    if data:
                        self.logger.process(data)
                        self.mqtt.publish("live_data", data)

                        mac = data.get("d_id", "").upper()
                        if mac and mac not in self.storage.mac_to_ip:
                            self.storage.save_ip_mac(ip, mac)
            await asyncio.sleep(LIVE_PERIOD)

    async def start(self):
        print(f"🚀 WiSun Gateway Started → {GATEWAY_NAME}")
        self.mqtt.connect()

        await asyncio.sleep(5)  # Wait for connection
        await self.periodic_monitor()

async def main():
    print("Starting Wisun gateway")
    gateway = WiSunGateway()
    await gateway.start()

if __name__ == "__main__":
    asyncio.run(main())
