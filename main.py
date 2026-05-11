import asyncio
import signal
from monitor import WiSunMonitor
from storage import Storage
from logger import PayloadLogger
from mqtt_handler import MQTTHandler
from config import GATEWAY_NAME
from constants import LIVE_PERIOD, CONNECT_DELAY

class WiSunGateway:
    def __init__(self):
        self.storage = Storage() #initializes storage system (data directory, registered devices
        self.logger = PayloadLogger() #sets up logging to combined_log.csv.
        self.monitor = WiSunMonitor() # starts monitoring Wi-SUN network status.
        self.mqtt = MQTTHandler(self.storage, self.logger) #creates MQTT handler, passing storage and logger so it can log payloads and update device mappings.

        self.storage.load_ip_mac() #loads IP ↔ MAC mappings from data/ip_mac_mappings.csv at startup
    #Function to Periodically monitor the Nodes
    async def periodic_monitor(self):
        while True:
            try:
                #Get the Nodes details from the wsbrd Stack
                nodes = await self.monitor.get_nodes()
    
                # Launch all node fetches concurrently by creating the each task for each node
                tasks = [Node(ip, "info/motor_status").get() for ip in nodes]
                results = await asyncio.gather(*tasks, return_exceptions=True) #Run the all the Task and store the result in results
    
                # Process results
                for ip, data in zip(nodes, results): #ZIP the results with {ip, <data>}
                    if isinstance(data, Exception): #Handle the Exception arise on result
                        self.logger.error(f"Error fetching node {ip}: {data}")
                        continue
    
                    if data: #If the data in result is Valid
                        self.logger.process(data) #Process the Data
    
                        if self.mqtt.is_connected: #Check for MQTT Connected
                            await self.mqtt.publish("live_data", data) #Publish the Live Data
                            self.storage.save_offline(data) #Save in offline
                        else: #If MQTT Connection Failed
                            self.storage.save_offline(data) # Save Offline
    
                        mac = data.get("d_id", "").upper() #Make UPPER CASE of Device MAC
                        if mac and mac not in self.storage.mac_to_ip: #Check for it existance in mac to Ip mappings
                            self.storage.save_ip_mac(ip, mac) #Stor IP to MAC for New Devices, 
    
            except Exception as e:
                self.logger.error(f"Monitor loop error: {e}")

        await asyncio.sleep(LIVE_PERIOD)


    async def start(self):
        """Start the gateway: connect MQTT and begin monitoring."""
        print(f"WiSun Gateway Started → {GATEWAY_NAME}")

        try:
            # Async connect to MQTT broker
            await self.mqtt.connect()
        except Exception as e:
            self.logger.error(f"MQTT connection failed: {e}")

        # Allow time for connection setup
        await asyncio.sleep(CONNECT_DELAY)

        # Start monitoring loop
        await self.periodic_monitor()
        
    async def shutdown(self):
        """Gracefully shut down gateway services."""
        print("Shutting down WiSun Gateway...")
        try:
            await self.mqtt.disconnect()
        except Exception as e:
            self.logger.error(f"Error during MQTT disconnect: {e}")
        print("Shutdown complete.")
        
async def main():
    """Main entry point for the gateway."""
    print("Starting Wi-SUN gateway")
    gateway = WiSunGateway()

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(gateway.shutdown()))

    await gateway.start()

if __name__ == "__main__":
    asyncio.run(main())
