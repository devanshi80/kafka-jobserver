"""
Enhanced processing system for IDS network flow analysis
"""
import queue
import time
import threading
import requests
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enhanced_logger import get_logger
from config_file import config

@dataclass
class ProcessingStats:
    """Statistics for processing operations"""
    items_processed: int = 0
    items_failed: int = 0
    alerts_generated: int = 0
    ml_requests: int = 0
    ml_errors: int = 0
    queue_size: int = 0
    processing_time_total: float = 0.0
    last_error: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    
    def get_processing_rate(self) -> float:
        """Get items processed per second"""
        elapsed = time.time() - self.start_time
        return self.items_processed / elapsed if elapsed > 0 else 0.0

class MLClient:
    """Machine Learning service client with retry logic"""
    
    def __init__(self):
        self.logger = get_logger('ml_client')
        self.ml_config = config.get_ml_config()
        self.url = self.ml_config.get('url', 'http://localhost:8080/predict')
        self.timeout = self.ml_config.get('timeout', 5)
        self.max_retries = self.ml_config.get('max_retries', 2)
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        
        # Statistics
        self.stats = {
            'requests_sent': 0,
            'requests_failed': 0,
            'response_times': [],
            'last_error': None
        }

    def predict(self, flow_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send prediction request to ML service"""
        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.time()
                
                response = self.session.post(
                    self.url,
                    json=flow_data,
                    timeout=self.timeout
                )
                
                response_time = time.time() - start_time
                self.stats['response_times'].append(response_time)
                
                # Keep only last 100 response times
                if len(self.stats['response_times']) > 100:
                    self.stats['response_times'] = self.stats['response_times'][-100:]
                
                if response.status_code == 200:
                    self.stats['requests_sent'] += 1
                    result = response.json()
                    
                    self.logger.debug(f"ML prediction successful in {response_time:.3f}s")
                    return result
                else:
                    self.logger.error(f"ML service returned status {response.status_code}: {response.text}")
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"ML request timeout (attempt {attempt + 1}/{self.max_retries + 1})")
                
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"ML service connection error (attempt {attempt + 1}/{self.max_retries + 1})")
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"ML request failed: {e}")
                
            except Exception as e:
                self.logger.error(f"Unexpected ML client error: {e}")
            
            # Brief delay before retry
            if attempt < self.max_retries:
                time.sleep(0.1 * (attempt + 1))
        
        self.stats['requests_failed'] += 1
        self.stats['last_error'] = f"Failed after {self.max_retries + 1} attempts"
        return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get ML client statistics"""
        avg_response_time = (
            sum(self.stats['response_times']) / len(self.stats['response_times'])
            if self.stats['response_times'] else 0
        )
        
        return {
            **self.stats,
            'avg_response_time': avg_response_time,
            'success_rate': (
                self.stats['requests_sent'] / (self.stats['requests_sent'] + self.stats['requests_failed'])
                if (self.stats['requests_sent'] + self.stats['requests_failed']) > 0 else 0
            )
        }

class AlertDispatcher:
    """Handle security alerts with different severity levels"""
    
    def __init__(self):
        self.logger = get_logger('alert_dispatcher')
        self.stats = {
            'alerts_sent': 0,
            'high_severity': 0,
            'medium_severity': 0,
            'low_severity': 0
        }

    def handle_alert(self, flow_data: Dict[str, Any], prediction: Dict[str, Any]):
        """Process and dispatch security alerts"""
        try:
            alert_data = self._create_alert(flow_data, prediction)
            severity = alert_data.get('severity', 'low')
            
            self.logger.warning(f"🚨 SECURITY ALERT [{severity.upper()}]: {alert_data['alert_type']} "
                              f"from {flow_data.get('src_ip', 'unknown')} to {flow_data.get('dst_ip', 'unknown')}")
            
            # Update statistics
            self.stats['alerts_sent'] += 1
            self.stats[f'{severity}_severity'] += 1
            
            # Here you could add integrations with:
            # - SIEM systems
            # - Email notifications
            # - Slack/Teams webhooks
            # - Security orchestration platforms
            
        except Exception as e:
            self.logger.error(f"❌ Error handling alert: {e}")

    def _create_alert(self, flow_data: Dict[str, Any], prediction: Dict[str, Any]) -> Dict[str, Any]:
        """Create structured alert data"""
        threat_type = prediction.get('result', 'unknown')
        confidence = prediction.get('confidence', 0.0)
        
        # Determine severity based on threat type and confidence
        severity = self._determine_severity(threat_type, confidence)
        
        return {
            'timestamp': time.time(),
            'alert_type': threat_type,
            'severity': severity,
            'confidence': confidence,
            'source_ip': flow_data.get('src_ip'),
            'destination_ip': flow_data.get('dst_ip'),
            'source_port': flow_data.get('src_port'),
            'destination_port': flow_data.get('dst_port'),
            'protocol': flow_data.get('protocol'),
            'packet_count': flow_data.get('packet_count', 0),
            'byte_count': flow_data.get('byte_count', 0),
            'flow_duration': flow_data.get('duration', 0),
            'additional_data': prediction.get('details', {})
        }

    def _determine_severity(self, threat_type: str, confidence: float) -> str:
        """Determine alert severity based on threat type and confidence"""
        high_severity_threats = ['ddos', 'malware', 'intrusion', 'data_exfiltration']
        medium_severity_threats = ['scan', 'reconnaissance', 'suspicious_traffic']
        
        if threat_type.lower() in high_severity_threats and confidence > 0.8:
            return 'high'
        elif threat_type.lower() in high_severity_threats or confidence > 0.6:
            return 'medium'
        else:
            return 'low'

    def get_stats(self) -> Dict[str, Any]:
        """Get alert dispatcher statistics"""
        return self.stats.copy()

class NetworkFlowProcessor:
    """Main processor for network flow analysis"""
    
    def __init__(self):
        self.logger = get_logger('processor')
        self.processing_config = config.get_processing_config()
        
        # Processing queue
        queue_size = self.processing_config.get('queue_size', 10000)
        self.processing_queue = queue.Queue(maxsize=queue_size)
        
        # Components
        self.ml_client = MLClient()
        self.alert_dispatcher = AlertDispatcher()
        
        # Statistics
        self.stats = ProcessingStats()
        
        # Threading control
        self.shutdown_event = threading.Event()
        self.worker_threads = []
        
        # Batch processing
        self.batch_size = self.processing_config.get('batch_size', 10)
        self.processing_timeout = self.processing_config.get('processing_timeout', 30)

    def add_to_queue(self, data: Dict[str, Any]) -> bool:
        """Add flow data to processing queue"""
        try:
            self.processing_queue.put_nowait(data)
            self.stats.queue_size = self.processing_queue.qsize()
            return True
            
        except queue.Full:
            self.logger.warning("⚠️ Processing queue full, dropping data")
            self.stats.items_failed += 1
            return False

    def start_workers(self, num_workers: int = None):
        """Start processing worker threads"""
        if num_workers is None:
            num_workers = self.processing_config.get('worker_threads', 4)
        
        self.logger.info(f"🚀 Starting {num_workers} processing workers...")
        
        for i in range(num_workers):
            worker_thread = threading.Thread(
                target=self._worker_loop,
                name=f"ProcessorWorker-{i+1}",
                daemon=True
            )
            worker_thread.start()
            self.worker_threads.append(worker_thread)
        
        self.logger.info(f"✅ Started {len(self.worker_threads)} processing workers")

    def _worker_loop(self):
        """Main worker loop for processing flow data"""
        worker_name = threading.current_thread().name
        self.logger.info(f"🔧 Worker {worker_name} started")
        
        while not self.shutdown_event.is_set():
            try:
                # Get item from queue with timeout
                try:
                    item = self.processing_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                
                # Process the item
                start_time = time.time()
                success = self._process_flow_data(item, worker_name)
                processing_time = time.time() - start_time
                
                # Update statistics
                self.stats.processing_time_total += processing_time
                self.stats.queue_size = self.processing_queue.qsize()
                
                if success:
                    self.stats.items_processed += 1
                else:
                    self.stats.items_failed += 1
                
                # Mark task as done
                self.processing_queue.task_done()
                
                # Log progress
                if self.stats.items_processed % 100 == 0:
                    self._log_processing_stats()
                
            except Exception as e:
                self.logger.error(f"❌ Worker {worker_name} error: {e}")
                self.stats.items_failed += 1
                self.stats.last_error = str(e)
                time.sleep(0.1)  # Brief pause on error
        
        self.logger.info(f"🛑 Worker {worker_name} stopped")

    def _process_flow_data(self, flow_data: Dict[str, Any], worker_name: str) -> bool:
        """Process individual flow data item"""
        try:
            self.logger.debug(f"🔍 {worker_name} processing flow: "
                            f"{flow_data.get('src_ip', 'unknown')} -> {flow_data.get('dst_ip', 'unknown')}")
            
            # Send to ML service for prediction
            self.stats.ml_requests += 1
            prediction = self.ml_client.predict(flow_data)
            
            if prediction is None:
                self.logger.warning(f"⚠️ ML prediction failed for flow from {flow_data.get('src_ip', 'unknown')}")
                self.stats.ml_errors += 1
                return False
            
            self.logger.debug(f"🤖 ML Prediction: {prediction}")
            
            # Handle alerts for non-benign traffic
            result = prediction.get('result', '').lower()
            if result and result != 'benign' and result != 'normal':
                self.alert_dispatcher.handle_alert(flow_data, prediction)
                self.stats.alerts_generated += 1
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error processing flow data: {e}")
            self.stats.last_error = str(e)
            return False

    def _log_processing_stats(self):
        """Log processing statistics"""
        rate = self.stats.get_processing_rate()
        avg_processing_time = (
            self.stats.processing_time_total / self.stats.items_processed
            if self.stats.items_processed > 0 else 0
        )
        
        self.logger.info(f"📊 Processed: {self.stats.items_processed}, "
                        f"Failed: {self.stats.items_failed}, "
                        f"Rate: {rate:.1f}/s, "
                        f"Queue: {self.stats.queue_size}, "
                        f"Alerts: {self.stats.alerts_generated}, "
                        f"Avg time: {avg_processing_time:.3f}s")

    def stop_workers(self):
        """Stop all processing workers"""
        self.logger.info("🛑 Stopping processing workers...")
        self.shutdown_event.set()
        
        # Wait for workers to finish
        for thread in self.worker_threads:
            thread.join(timeout=5)
        
        self.logger.info("✅ All processing workers stopped")

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics from all components"""
        return {
            'processor': {
                'items_processed': self.stats.items_processed,
                'items_failed': self.stats.items_failed,
                'alerts_generated': self.stats.alerts_generated,
                'processing_rate': self.stats.get_processing_rate(),
                'queue_size': self.stats.queue_size,
                'workers_active': len([t for t in self.worker_threads if t.is_alive()]),
                'last_error': self.stats.last_error
            },
            'ml_client': self.ml_client.get_stats(),
            'alert_dispatcher': self.alert_dispatcher.get_stats()
        }

    def is_healthy(self) -> bool:
        """Health check for the processor"""
        active_workers = len([t for t in self.worker_threads if t.is_alive()])
        queue_not_full = self.stats.queue_size < self.processing_config.get('queue_size', 10000) * 0.9
        
        return active_workers > 0 and queue_not_full

# Global processor instance
processor = NetworkFlowProcessor()

# Legacy compatibility functions
def add_to_queue(data: Dict[str, Any]) -> bool:
    """Legacy function for backward compatibility"""
    return processor.add_to_queue(data)

def process_data():
    """Legacy function - now handled by worker threads"""
    pass  # This is now handled automatically by worker threads
    