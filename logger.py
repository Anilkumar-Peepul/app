import csv
import logging
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler
from .config import LOG_DIR, COMBINED_LOG_FILE

class PayloadLogger:
    def __init__(self):
        self.setup_logger()
        self.combined_file = COMBINED_LOG_FILE

        self.th = {  # thresholds
            "pfa": 400, "lva": 410, "lvf": 405, "hva": 450, "hvf": 455,
            "vif": 20, "via": 15, "m_f_dr": 0.2, "m_f_ol": 0.3,
            "m_a_dr": 0.3, "m_a_ol": 0.4
        }

    def setup_logger(self):
        self.logger = logging.getLogger("wisun_gateway")
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(LOG_DIR / "gateway.log", maxBytes=5*1024*1024, backupCount=5)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def max_diff(self, arr): return max(arr) - min(arr) if arr else 0
    def avg(self, arr): return sum(arr) / len(arr) if arr else 0

    def detect_anomalies(self, payload: dict):
        anomalies = {}
        ll_v = payload.get("ll_v", [0,0,0])

        anomalies.update({
            "Low_V_Fault": any(v < self.th["lvf"] for v in ll_v),
            "High_V_Fault": any(v > self.th["hvf"] for v in ll_v),
            "Voltage_Imbalance": self.max_diff(ll_v) > self.th["vif"]
        })
        return anomalies

    def process(self, payload: dict):
        try:
            row = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "d_id": payload.get("d_id", ""),
                "p_v": payload.get("p_v", 0),
                "pwr": payload.get("pwr", 0),
            }
            anomalies = self.detect_anomalies(payload)
            row.update(anomalies)

            # Save to CSV
            file_exists = self.combined_file.exists()
            with open(self.combined_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)

            self.logger.info(f"Processed data from {row['d_id']} | Anomalies: {anomalies}")
        except Exception as e:
            self.logger.error(f"Logger error: {e}")
