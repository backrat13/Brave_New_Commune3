import redis
import json
import time
import logging
import sys  # <--- THIS WAS LIKELY MISSING
from datetime import datetime

# ============================================================
# BNC3 - CRW DAEMON (DAY-004 CONSTITUTIONAL WORKER)
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [CRW-DAEMON] %(message)s',
    handlers=[
        logging.FileHandler("/home/splinter/Brave_New_Commune3/bnc3-infra/crw.log"),
        logging.StreamHandler(sys.stdout) # Fixed from StreamWriter to StreamHandler
    ]
)

class CRWDaemon:
    def __init__(self):
        # We use try/except here so the daemon tells us if Redis is down
        try:
            self.r = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
            self.r.ping() 
        except redis.ConnectionError:
            logging.error("CRITICAL: Could not connect to Redis. Is it running?")
            sys.exit(1)

        self.queue_name = "conflict_queue"
        self.quarantine_prefix = "quarantine:"

    def run(self):
        logging.info("BNC3 Conflict Resolution Worker initialized. Awaiting disputes...")
        
        while True:
            try:
                # brpop returns a tuple: (list_name, data)
                result = self.r.brpop(self.queue_name, timeout=0)
                if result:
                    _, message = result
                    conflict_data = json.loads(message)
                    self.process_conflict(conflict_data)
            except Exception as e:
                logging.error(f"Worker Loop Error: {e}")
                time.sleep(1) # Prevent rapid-fire crashing

    def process_conflict(self, data):
        cid = data['conflict_id']
        key = data['target_key']
        agent = data['origin_agent']
        
        logging.info(f"!!! CONFLICT DETECTED [{cid}] !!!")
        
        self.r.hset(f"{self.quarantine_prefix}{cid}", mapping={
            "status": "OPEN",
            "timestamp": datetime.now().isoformat(),
            "origin_agent": agent,
            "target_key": key,
            "S_A": json.dumps(data['S_A']),
            "S_B": json.dumps(data['S_B']),
            "evidence": data['evidence']
        })
        
        logging.info(f"Conflict {cid} Quarantined. Awaiting Splinter's Decree.")

if __name__ == "__main__":
    daemon = CRWDaemon()
    daemon.run()
