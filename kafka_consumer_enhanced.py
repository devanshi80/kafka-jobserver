"""
Enhanced Kafka consumer for IDS system
"""
import json
import time
import threading
from typing import Dict, Any, Callable, Optional
from kafka import KafkaConsumer as KafkaClient
from kafka.errors import KafkaError, CommitFailedError
from enhanced_logger import get_logger
from config_file import config

class KafkaConsumer:
    """Enhanced Kafka consumer with error handling and monitoring"""

    def __init__(self, message_handler: Callable[[Dict[str, Any]], None]):
        self.logger = get_logger('kafka.consumer')
        self.kafka_config = config.get_kafka_config()
        self.message_handler = message_handler
        
        self.consumer = None
        self.connected = False
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Configuration
        self.topic = self.kafka_config.get('topic', 'network_flows')
        self.bootstrap_servers = self.kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.group_id = self.kafka_config.get('group_id', 'ids_processing_group')
        self.client_id = self.kafka_config.get('client_id', 'ids-consumer')
        
        # Statistics
        self.stats = {
            'messages_received': 0,
            'messages_processed': 0,
            'messages_failed': 0,
            'bytes_received': 0,
            'connection_errors': 0,
            'last_error': None,
            'last_message_time': None,
            'processing_errors': 0
        }
        
        # Performance tracking
        self.batch_size = 0
        self.batch_start_time = time.time()

    def initialize(self) -> bool:
        """Initialize Kafka consumer with retry logic"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                if self._create_consumer():
                    self.logger.info(f"✅ Kafka consumer initialized successfully on attempt {attempt + 1}")
                    return True
                    
            except Exception as e:
                self.logger.error(f"❌ Consumer init attempt {attempt + 1} failed: {e}")
                self.stats['connection_errors'] += 1
                self.stats['last_error'] = str(e)
                
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
        
        self.logger.error("❌ Failed to initialize Kafka consumer after all retries")
        return False

    def _create_consumer(self) -> bool:
        """Create Kafka consumer instance"""
        try:
            consumer_config = self.kafka_config.get('consumer', {})
            
            self.logger.info(f"Connecting consumer to Kafka: {self.bootstrap_servers}")
            self.logger.info(f"Consumer group: {self.group_id}, Topic: {self.topic}")
            
            self.consumer = KafkaClient(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                group_id=self.group_id,
                auto_offset_reset=consumer_config.get('auto_offset_reset', 'latest'),
                enable_auto_commit=consumer_config.get('enable_auto_commit', True),
                auto_commit_interval_ms=consumer_config.get('auto_commit_interval_ms', 5000),
                session_timeout_ms=consumer_config.get('session_timeout_ms', 30000),
                heartbeat_interval_ms=consumer_config.get('heartbeat_interval_ms', 10000),
                max_poll_records=consumer_config.get('max_poll_records', 100),
                max_poll_interval_ms=consumer_config.get('max_poll_interval_ms', 300000),
                fetch_min_bytes=consumer_config.get('fetch_min_bytes', 1024),
                fetch_max_wait_ms=consumer_config.get('fetch_max_wait_ms', 500),
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                security_protocol='PLAINTEXT',
                api_version_auto_timeout_ms=30000,
                connections_max_idle_ms=540000
            )

            self.connected = True
            self.logger.info(f"✅ Consumer connected to topic '{self.topic}'")
            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to create consumer: {e}")
            self.connected = False
            self.consumer = None
            raise

    def start_consuming(self):
        """Start consuming messages from Kafka"""
        if not self.initialize():
            self.logger.error("❌ Cannot start consuming - initialization failed")
            return

        self.running = True
        self.logger.info("🚀 Starting message consumption...")

        try:
            self._consume_loop()
        except Exception as e:
            self.logger.error(f"❌ Critical error in consume loop: {e}")
        finally:
            self.running = False
            self._cleanup()

    def _consume_loop(self):
        """Main consumption loop with error handling"""
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while not self.shutdown_event.is_set() and self.running:
            try:
                if not self.connected or not self.consumer:
                    self.logger.warning("Consumer not connected, attempting reconnection...")
                    if not self.initialize():
                        time.sleep(5)  # Wait before retry
                        continue

                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue  # No messages, continue polling

                # Process batch
                self._process_message_batch(message_batch)
                consecutive_errors = 0  # Reset error counter

            except KeyboardInterrupt:
                self.logger.info("🛑 Received interrupt signal")
                break
                
            except CommitFailedError as e:
                self.logger.error(f"❌ Commit failed: {e}")
                consecutive_errors += 1
                self._handle_consumer_error(e)
                
            except KafkaError as e:
                self.logger.error(f"❌ Kafka error: {e}")
                consecutive_errors += 1
                self._handle_consumer_error(e)
                
            except Exception as e:
                self.logger.error(f"❌ Unexpected error in consume loop: {e}")
                consecutive_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.error(f"❌ Too many consecutive errors ({consecutive_errors}), stopping consumer")
                    break
                
                time.sleep(1)  # Brief pause before retry

    def _process_message_batch(self, message_batch: Dict):
        """Process a batch of messages"""
        total_messages = sum(len(messages) for messages in message_batch.values())
        
        if total_messages > 0:
            self.logger.debug(f"📥 Processing batch of {total_messages} messages")
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    if self.shutdown_event.is_set():
                        return
                    
                    self._process_single_message(message)

            # Log batch statistics
            if self.stats['messages_received'] % 1000 == 0:
                self._log_performance_stats()

    def _process_single_message(self, message):
        """Process a single message"""
        try:
            # Update statistics
            self.stats['messages_received'] += 1
            self.stats['bytes_received'] += len(message.value) if message.value else 0
            self.stats['last_message_time'] = time.time()
            
            # Log message details (debug level)
            self.logger.debug(f"📨 Received message - Partition: {message.partition}, "
                            f"Offset: {message.offset}, Key: {message.key}")
            
            # Handle the message
            if message.value:
                self.message_handler(message.value)
                self.stats['messages_processed'] += 1
            else:
                self.logger.warning("⚠️ Received empty message")

        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Invalid JSON in message: {e}")
            self.stats['messages_failed'] += 1
            
        except Exception as e:
            self.logger.error(f"❌ Error processing message: {e}")
            self.stats['processing_errors'] += 1
            self.stats['last_error'] = str(e)

    def _handle_consumer_error(self, error):
        """Handle consumer errors and attempt recovery"""
        self.stats['connection_errors'] += 1
        self.stats['last_error'] = str(error)
        self.connected = False
        
        # Try to reconnect
        self.logger.info("🔄 Attempting consumer recovery...")
        try:
            if self.consumer:
                self.consumer.close()
            time.sleep(2)  # Brief pause before reconnection
        except:
            pass  # Ignore cleanup errors

    def _log_performance_stats(self):
        """Log performance statistics"""
        current_time = time.time()
        elapsed = current_time - self.batch_start_time
        
        if elapsed > 0:
            messages_per_sec = self.stats['messages_received'] / elapsed
            mb_per_sec = (self.stats['bytes_received'] / 1024 / 1024) / elapsed
            
            self.logger.info(f"📊 Performance: {messages_per_sec:.1f} msg/s, "
                           f"{mb_per_sec:.2f} MB/s, "
                           f"Total processed: {self.stats['messages_processed']}")

    def stop_consuming(self):
        """Stop consuming messages"""
        self.logger.info("🛑 Stopping consumer...")
        self.running = False
        self.shutdown_event.set()

    def _cleanup(self):
        """Clean up resources"""
        if self.consumer:
            try:
                self.consumer.close()
                self.logger.info("✅ Consumer closed successfully")
            except Exception as e:
                self.logger.error(f"❌ Error closing consumer: {e}")
            finally:
                self.consumer = None
                self.connected = False

    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        current_time = time.time()
        uptime = current_time - self.batch_start_time
        
        return {
            **self.stats,
            'connected': self.connected,
            'running': self.running,
            'topic': self.topic,
            'group_id': self.group_id,
            'bootstrap_servers': self.bootstrap_servers,
            'uptime_seconds': uptime,
            'messages_per_second': self.stats['messages_received'] / uptime if uptime > 0 else 0
        }

    def is_healthy(self) -> bool:
        """Health check for monitoring"""
        return self.connected and self.running and self.consumer is not None

def consume_network_flows(message_handler: Callable[[Dict[str, Any]], None]):
    """Convenience function to start consuming with a message handler"""
    consumer = KafkaConsumer(message_handler)
    consumer.start_consuming()
    return consumer