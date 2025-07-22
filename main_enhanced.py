"""
Enhanced main application for IDS Job Server
"""
import sys
import signal
import time
import threading
from typing import Dict, Any
from enhanced_logger import get_logger, IDSLogger
from config_file import config
from kafka_consumer_enhanced import KafkaConsumer
from processor_enhanced import processor
from health_monitor import health_monitor

class IDSJobServer:
    """Main IDS Job Server application"""
    
    def __init__(self):
        # Setup logging first
        IDSLogger.setup_logging()
        self.logger = get_logger('main')
        
        self.logger.info("🚀 Initializing IDS Job Server...")
        
        # Components
        self.kafka_consumer = None
        self.shutdown_event = threading.Event()
        self.running = False
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("✅ IDS Job Server initialized")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        self.logger.info(f"📡 Received {signal_name}, initiating graceful shutdown...")
        self.shutdown()

    def start(self):
        """Start the IDS Job Server"""
        try:
            self.logger.info("🚀 Starting IDS Job Server components...")
            
            # Start health monitoring
            self._start_health_monitoring()
            
            # Start processor workers
            self._start_processor()
            
            # Start Kafka consumer
            self._start_kafka_consumer()
            
            self.running = True
            self.logger.info("✅ IDS Job Server started successfully")
            
            # Main loop
            self._main_loop()
            
        except KeyboardInterrupt:
            self.logger.info("📡 Received keyboard interrupt")
            
        except Exception as e:
            self.logger.error(f"❌ Critical error starting server: {e}")
            sys.exit(1)
            
        finally:
            self.shutdown()

    def _start_health_monitoring(self):
        """Start health monitoring system"""
        try:
            # Register components for monitoring
            health_monitor.register_component('processor', processor.is_healthy)
            health_monitor.register_component('kafka_consumer', 
                                            lambda: self.kafka_consumer.is_healthy() if self.kafka_consumer else False)
            
            # Start monitoring
            health_monitor.start_monitoring()
            
            self.logger.info("✅ Health monitoring started")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start health monitoring: {e}")

    def _start_processor(self):
        """Start the network flow processor"""
        try:
            processing_config = config.get_processing_config()
            num_workers = processing_config.get('worker_threads', 4)
            
            processor.start_workers(num_workers)
            
            # Update health status
            health_monitor.update_component_health('processor', True, 'started')
            
            self.logger.info("✅ Network flow processor started")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start processor: {e}")
            health_monitor.update_component_health('processor', False, 'failed', str(e))
            raise

    def _start_kafka_consumer(self):
        """Start Kafka consumer"""
        try:
            # Create consumer with message handler
            self.kafka_consumer = KafkaConsumer(self._handle_kafka_message)
            
            # Start consumer in background thread
            consumer_thread = threading.Thread(
                target=self.kafka_consumer.start_consuming,
                name="KafkaConsumerThread",
                daemon=True
            )
            consumer_thread.start()
            
            # Give consumer time to initialize
            time.sleep(2)
            
            if self.kafka_consumer.is_healthy():
                health_monitor.update_component_health('kafka_consumer', True, 'consuming')
                self.logger.info("✅ Kafka consumer started")
            else:
                raise Exception("Kafka consumer failed to start properly")
                
        except Exception as e:
            self.logger.error(f"❌ Failed to start Kafka consumer: {e}")
            health_monitor.update_component_health('kafka_consumer', False, 'failed', str(e))
            raise

    def _handle_kafka_message(self, message_data: Dict[str, Any]):
        """Handle incoming Kafka messages"""
        try:
            self.logger.debug(f"📨 Received message from Kafka: {message_data.get('src_ip', 'unknown')} -> {message_data.get('dst_ip', 'unknown')}")
            
            # Add to processing queue
            success = processor.add_to_queue(message_data)
            
            if not success:
                self.logger.warning("⚠️ Failed to queue message for processing")
            
            # Update health status
            health_monitor.update_component_health('kafka_consumer', True, 'consuming')
            
        except Exception as e:
            self.logger.error(f"❌ Error handling Kafka message: {e}")
            health_monitor.update_component_health('kafka_consumer', False, 'error', str(e))

    def _main_loop(self):
        """Main application loop"""
        self.logger.info("🔄 Starting main loop...")
        
        stats_interval = 60  # Log stats every 60 seconds
        last_stats_time = time.time()
        
        while not self.shutdown_event.is_set():
            try:
                # Log system stats periodically
                current_time = time.time()
                if current_time - last_stats_time >= stats_interval:
                    self._log_system_stats()
                    last_stats_time = current_time
                
                # Check system health
                if not self._check_system_health():
                    self.logger.error("❌ System health check failed, initiating shutdown")
                    self.shutdown()
                    break
                
                # Sleep briefly to prevent tight loop
                time.sleep(1)
                
            except KeyboardInterrupt:
                self.logger.info("📡 Received keyboard interrupt in main loop")
                break
                
            except Exception as e:
                self.logger.error(f"❌ Error in main loop: {e}")
                time.sleep(5)  # Brief pause before continuing

    def _log_system_stats(self):
        """Log system statistics"""
        try:
            # Get processor stats
            processor_stats = processor.get_comprehensive_stats()
            
            # Get Kafka consumer stats if available
            kafka_stats = self.kafka_consumer.get_stats() if self.kafka_consumer else {}
            
            # Log summary
            self.logger.info(
                f"📈 SYSTEM STATS:\n"
                f"  PROCESSOR:\n"
                f"    Processed: {processor_stats['processor']['items_processed']}\n"
                f"    Failed: {processor_stats['processor']['items_failed']}\n"
                f"    Alerts: {processor_stats['processor']['alerts_generated']}\n"
                f"    Queue: {processor_stats['processor']['queue_size']}\n"
                f"    Workers: {processor_stats['processor']['workers_active']}\n"
                f"  KAFKA:\n"
                f"    Messages: {kafka_stats.get('messages_received', 0)}\n"
                f"    Processed: {kafka_stats.get('messages_processed', 0)}\n"
                f"    Failed: {kafka_stats.get('messages_failed', 0)}\n"
                f"    Connected: {'Yes' if kafka_stats.get('connected', False) else 'No'}\n"
                f"  ML SERVICE:\n"
                f"    Requests: {processor_stats['ml_client']['requests_sent']}\n"
                f"    Failed: {processor_stats['ml_client']['requests_failed']}\n"
                f"    Avg Time: {processor_stats['ml_client']['avg_response_time']:.3f}s"
            )
            
        except Exception as e:
            self.logger.error(f"❌ Error logging system stats: {e}")

    def _check_system_health(self) -> bool:
        """Check overall system health"""
        try:
            # Get health status from monitor
            health_status = health_monitor.get_system_health()
            
            if not health_status['system_healthy']:
                unhealthy_components = [
                    name for name, comp in health_status['components'].items() 
                    if not comp['healthy']
                ]
                self.logger.error(f"❌ Unhealthy components: {', '.join(unhealthy_components)}")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking system health: {e}")
            return False

    def shutdown(self):
        """Gracefully shutdown the IDS Job Server"""
        if not self.running:
            return
            
        self.logger.info("🛑 Starting graceful shutdown...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop Kafka consumer
        if self.kafka_consumer:
            try:
                self.kafka_consumer.stop_consuming()
                self.logger.info("✅ Kafka consumer stopped")
            except Exception as e:
                self.logger.error(f"❌ Error stopping Kafka consumer: {e}")
        
        # Stop processor
        try:
            processor.stop_workers()
            self.logger.info("✅ Processor workers stopped")
        except Exception as e:
            self.logger.error(f"❌ Error stopping processor: {e}")
        
        # Stop health monitoring
        try:
            health_monitor.stop_monitoring()
            self.logger.info("✅ Health monitoring stopped")
        except Exception as e:
            self.logger.error(f"❌ Error stopping health monitor: {e}")
        
        self.logger.info("🛑 IDS Job Server shutdown complete")

def main():
    """Main entry point for IDS Job Server"""
    server = IDSJobServer()
    server.start()

if __name__ == "__main__":
    main()