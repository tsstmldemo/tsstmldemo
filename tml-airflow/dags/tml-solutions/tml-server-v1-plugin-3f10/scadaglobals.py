import threading
import requests
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, Timeout, HTTPError

active_sim_threads = []
active_read_threads = []

# Initialize the STOP SIGNALS (Events)
stop_sim = threading.Event()
stop_read = threading.Event()

read_job = None
read_thread = None
mqtt_job = None
mqtt_thread = None
mqtt_client = None
sim_thread = None
sim_job = None
mainmode = ""
#mainmodes = []
topologyrunning = []
topologystop = []
scadatopologyrunning = []
scadatopologystop = []
scadahostport = []

class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=30):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = 0
        self.state = "CLOSED" # CLOSED (Working), OPEN (Failed), HALF-OPEN (Testing)

    def can_execute(self):
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if (time.time() - self.last_failure_time) > self.recovery_timeout:
                self.state = "HALF-OPEN"
                return True
        return self.state == "HALF-OPEN"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            print(f"🚨 CIRCUIT OPENED: Stopping requests for {self.recovery_timeout}s")

    def record_success(self):
        self.failure_count = 0
        self.state = "CLOSED"

class ViperNetworkClient:
    def __init__(self):
        #self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.cb = CircuitBreaker()

        retries = Retry(
            total=5,
            backoff_factor=0.1, 
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        
        # Memory Management: Strict pool size for 4GB RAM environments
        adapter = HTTPAdapter(
            pool_connections=2, 
            pool_maxsize=10, 
            max_retries=retries
        )
        
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)


    def post_data(self, url, payload):
        """
        High-reliability POST method with Circuit Breaker logic.
        """
#        if not self.cb.can_execute():
 #           return None # Fail fast to save Pi CPU/RAM

        try:
            # Exxon-Ready: Using a slightly longer timeout for localhost congestion
            response = self.session.post(url, json=payload, timeout=(3.05, 15))
            response.raise_for_status()            
            # Metrics for performance monitoring
            self.cb.record_success()
            return response
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            self.cb.record_failure()
            return None



