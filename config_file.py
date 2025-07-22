"""
Configuration management for IDS Kafka Job Server
"""
import os
import json
from typing import Dict, Any, List

class Config:
    """Centralized configuration management"""
    
    def __init__(self, config_file: str = None):
        self.config_file = config_file or os.getenv('CONFIG_FILE', 'config.json')
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file or environment variables"""
        default_config = {
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "topic": "network_flows",
                "client_id": "ids-job-server",
                "group_id": "ids_processing_group",
                "producer": {
                    "acks": "1",
                    "retries": 3,
                    "request_timeout_ms": 30000,
                    "batch_size": 16384,
                    "linger_ms": 10
                },
                "consumer": {
                    "auto_offset_reset": "latest",
                    "enable_auto_commit": True,
                    "auto_commit_interval_ms": 5000,
                    "session_timeout_ms": 30000,
                    "max_poll_records": 100,
                    "fetch_min_bytes": 1024
                }
            },
            "processing": {
                "queue_size": 10000,
                "worker_threads": 4,
                "batch_size": 10,
                "processing_timeout": 30
            },
            "ml_service": {
                "url": "http://localhost:8080/predict",
                "timeout": 5,
                "max_retries": 2
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": "ids_job_server.log"
            },
            "health_check": {
                "port": 8090,
                "enabled": True
            }
        }
        
        # Try to load from file
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    file_config = json.load(f)
                    default_config.update(file_config)
            except Exception as e:
                print(f"Warning: Could not load config file {self.config_file}: {e}")
        
        # Override with environment variables
        self._override_with_env(default_config)
        
        return default_config
    
    def _override_with_env(self, config: Dict[str, Any]):
        """Override config values with environment variables"""
        # Kafka settings
        if os.getenv('KAFKA_BOOTSTRAP_SERVERS'):
            config['kafka']['bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS').split(',')
        
        if os.getenv('KAFKA_TOPIC'):
            config['kafka']['topic'] = os.getenv('KAFKA_TOPIC')
        
        if os.getenv('KAFKA_GROUP_ID'):
            config['kafka']['group_id'] = os.getenv('KAFKA_GROUP_ID')
        
        # Processing settings
        if os.getenv('WORKER_THREADS'):
            config['processing']['worker_threads'] = int(os.getenv('WORKER_THREADS'))
        
        if os.getenv('QUEUE_SIZE'):
            config['processing']['queue_size'] = int(os.getenv('QUEUE_SIZE'))
        
        # ML service settings
        if os.getenv('ML_SERVICE_URL'):
            config['ml_service']['url'] = os.getenv('ML_SERVICE_URL')
    
    def get(self, key: str, default=None):
        """Get configuration value using dot notation (e.g., 'kafka.topic')"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration"""
        return self._config.get('kafka', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return self._config.get('processing', {})
    
    def get_ml_config(self) -> Dict[str, Any]:
        """Get ML service configuration"""
        return self._config.get('ml_service', {})
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self._config.get('logging', {})

# Global config instance
config = Config()