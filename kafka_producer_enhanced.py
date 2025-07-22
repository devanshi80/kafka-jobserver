"""
Enhanced Kafka producer for IDS system
"""
import json
import time
import threading
from typing import Dict, Any, Optional, Callable
from kafka import KafkaProducer as KafkaClient
from kafka.errors import KafkaError, KafkaTimeoutError
from logger import get_logger
from config import config

class KafkaProducer:
    """Enhanced Kafka producer with reconnection and monitoring"""

    def __init__(self):
        self.logger = get_logger('kafka.producer')
        self.kafka_config = config.get_kafka_config()
        self.producer = None
        self.connected = False
        self.shutdown_event = threading.Event()
        self.lock = threading.RLock()
        
        # Configuration
        self.topic = self.kafka_config.get('topic', 'network_flows')
        self.bootstrap_servers = self.kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.client_id = self.kafka_config.get('client_id', 'ids-producer')
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0,
            'connection_errors': 0,
            'last_error': None,
            'last_success': None,
            'reconnect_attempts': 0
        }
        
        # Connection monitoring
        self._last_health_check = 0
        self._health_check_interval = 30  # seconds

    def initialize(self) -> bool:
        """Initialize Kafka producer with connection retry"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                if self._create_producer():
                    self.logger.info(f"✅ Kafka producer initialized successfully on attempt {attempt + 1}")
                    return True
                
            except Exception as e:
                self.logger.error(f"❌ Producer init attempt {attempt + 1} failed: {e}")
                self.stats['connection_errors'] += 1
                self.stats['last_error'] = str(e)
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
        
        self.logger.error("❌ Failed to initialize Kafka producer after all retries")
        return False

    def _create_producer(self) -> bool:
        """Create Kafka producer instance"""
        with self.lock:
            try:
                producer_config = self.kafka_config.get('producer', {})
                
                self.logger.info(f"Connecting to Kafka: {self.bootstrap_servers}")
                
                self.producer = KafkaClient(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=self.client_id,
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks=producer_config.get('acks', '1'),
                    retries=producer_config.get('retries', 3),
                    retry_backoff_ms=producer_config.get('retry_backoff_ms', 100),
                    request_timeout_ms=producer_config.get('request_timeout_ms', 30000),
                    batch_size=producer_config.get('batch_size', 16384),
                    linger_ms=producer_config.get('linger_ms', 10),
                    buffer_memory=33554432,  # 32MB
                    max_block_ms=60000,
                    api_version_auto_timeout_ms=30000,
                    security_protocol='PLAINTEXT',
                    connections_max_idle_ms=540000
                )

                self.connected = True
                self._last_health_check = time.time()
                self.stats['last_success'] = time.time()
                
                self.logger.info(f"✅ Producer connected - Topic: {self.topic}")
                return True

            except Exception as e:
                self.logger.error(f"❌ Failed to create producer: {e}")
                self.connected = False
                self.producer = None
                raise

    def send_flow_data(self, flow_data: Dict[str, Any], callback: Optional[Callable] = None) -> bool:
        """Send flow data to Kafka with automatic reconnection"""
        if self.shutdown_event.is_set():
            return False
        
        # Health check
        if not self._health_check():
            return False

        try:
            with self.lock:
                if not self.connected or not self.producer:
                    self.logger.warning("Producer not connected, attempting reconnection...")
                    if not self.initialize():
                        return False

                # Prepare message
                message_key = self._generate_message_key(flow_data)
                
                # Send message
                future = self.producer.send(
                    self.topic,
                    value=flow_data,
                    key=message_key
                )
                
                # Add callback handling
                if callback:
                    future.add_callback(callback)
                else:
                    future.add_callback(self._on_send_success)
                
                future.add_errback(self._on_send_error)

                # Non-blocking send - don't wait for response
                self.stats['messages_sent'] += 1
                message_size = len(json.dumps(flow_data, default=str).encode('utf-8'))
                self.stats['bytes_sent'] += message_size
                
                # Log progress
                if self.stats['messages_sent'] % 100 == 0:
                    self.logger.info(f"📊 Sent {self.stats['messages_sent']} messages "
                                   f"({self.stats['bytes_sent'] / 1024:.1f} KB total)")

                return True

        except KafkaTimeoutError:
            self.logger.error("❌ Kafka send timeout")
            self._handle_connection_error()
            return False
            
        except KafkaError as e:
            self.logger.error(f"❌ Kafka error: {e}")
            self._handle_connection_error()
            return False

        except Exception as e:
            self.logger.error(f"❌ Unexpected error sending message: {e}")
            self.stats['messages_failed'] += 1
            self.stats['last_error'] = str(e)
            return False

    def _generate_message_key(self, flow_data: Dict[str, Any]) -> str:
        """Generate message key for partitioning"""
        src_ip = flow_data.get('src_ip', '')
        dst_ip = flow_data.get('dst_ip', '')
        protocol = flow_data.get('protocol', '')
        
        # Use source IP for partitioning to group flows from same source
        return f"{src_ip}_{protocol}"

    def _health_check(self) -> bool:
        """Periodic health check"""
        current_time = time.time()
        
        if current_time - self._last_health_check > self._health_check_interval:
            self._last_health_check = current_time
            
            if not self.connected or not self.producer:
                self.logger.warning("Health check failed: not connected")
                return self._attempt_reconnect()
        
        return self.connected

    def _attempt_reconnect(self) -> bool:
        """Attempt to reconnect to Kafka"""
        self.logger.info("🔄 Attempting to reconnect...")
        self.stats['reconnect_attempts'] += 1
        
        try:
            self.close()  # Clean up existing connection
            return self.initialize()
        except Exception as e:
            self.logger.error(f"❌ Reconnection failed: {e}")
            return False

    def _handle_connection_error(self):
        """Handle connection errors"""
        self.connected = False
        self.stats['connection_errors'] += 1
        
        # Try immediate reconnection
        if not self._attempt_reconnect():
            self.logger.error("❌ Could not recover connection")

    def _on_send_success(self, record_metadata):
        """Callback for successful message send"""
        self.stats['last_success'] = time.time()

    def _on_send_error(self, exception):
        """Callback for send errors"""
        self.logger.error(f"❌ Message send failed: {exception}")
        self.stats['messages_failed'] += 1
        self.stats['last_error'] = str(exception)

    def flush(self, timeout: Optional[float] = None):
        """Flush pending messages"""
        if self.producer and self.connected:
            try:
                self.producer.flush(timeout=timeout)
                self.logger.debug("📤 Flushed pending messages")
            except Exception as e:
                self.logger.error(f"❌ Error flushing messages: {e}")

    def close(self):
        """Close Kafka producer"""
        self.logger.info("🔒 Closing Kafka producer...")
        self.shutdown_event.set()
        
        with self.lock:
            if self.producer:
                try:
                    self.producer.flush(timeout=10)
                    self.producer.close(timeout=10)
                except Exception as e:
                    self.logger.error(f"❌ Error closing producer: {e}")
                finally:
                    self.producer = None
                    self.connected = False

        self.logger.info("✅ Kafka producer closed")

    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        return {
            **self.stats,
            'connected': self.connected,
            'topic': self.topic,
            'bootstrap_servers': self.bootstrap_servers,
            'uptime': time.time() - self.stats.get('last_success', time.time()) if self.stats.get('last_success') else 0
        }

    def is_healthy(self) -> bool:
        """Health check for monitoring"""
        return self.connected and self.producer is not None