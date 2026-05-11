from pathlib import Path
import json
#Base Directory resolved by getting the current Directory of current file
#File: /home/peepul/app/config.py -> BASE_DIR: /home/peepul/app 
BASE_DIR = Path(__file__).resolve().parent

# Directories
LOG_DIR = BASE_DIR / "logs" # Creating a Folder -> /home/peepul/app/logs
DATA_DIR = BASE_DIR / "data" # Creating a Folder -> /home/peepul/app/data
LOG_DIR.mkdir(exist_ok=True) 
DATA_DIR.mkdir(exist_ok=True)

# Files
COMBINED_LOG_FILE = LOG_DIR / "combined_log.csv" # Define File Path -> /home/peepul/app/logs/combined_log.csv
IP_MAC_CSV = DATA_DIR / "ip_mac_mappings.csv" # Define File Path  -> /home/peepul/app/data/ip_mac_mappings.csv
REGISTERED_DEVICES_FILE = DATA_DIR / "registered_devices.txt" # Define File Path  -> /home/peepul/app/data/registered_devices.txt

# Load Configuration file about gateway from
with open(BASE_DIR / "config/gateway.json") as f:
    gateway_config = json.load(f)

with open(BASE_DIR / "config/config.json") as f:
    broker_config = json.load(f)

#Get the details to which server need to connect
# ================== FIXES / IMPROVEMENTS ==================
ENV = gateway_config.get("environment") or broker_config.get("environment", "staging")

# Fallback safety
if ENV not in broker_config:
    print(f"⚠️ Environment '{ENV}' not found in config.json, falling back to staging")
    ENV = "staging"

#Configure broker environment staging / live
mqtt_cfg = broker_config[ENV]

#Get the gateway name == CLIENT ID
GATEWAY_NAME = gateway_config["gateway_name"]
MQTT_BROKER = mqtt_cfg["broker"]  #Check to which broker (staging / Live) need to connect
MQTT_PORT = mqtt_cfg["port"]      #Port = 8883 ( SSL / TLS )
MQTT_USER = mqtt_cfg["username"]  #Authentication user name
MQTT_PASS = mqtt_cfg["password"]  #Authentication password
CA_CERT = mqtt_cfg["ca_cert"]     #Certificate the path to connect
