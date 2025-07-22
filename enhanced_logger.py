"""
Enhanced logging system for IDS Job Server
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional
from config_file import config

class IDSLogger:
    """Centralized logging for IDS components"""
    
    _loggers = {}
    _initialized = False
    
    @classmethod
    def setup_logging(cls):
        """Setup logging configuration"""
        if cls._initialized:
            return
        
        log_config = config.get_logging_config()
        
        # Create formatter
        formatter = logging.Formatter(
            log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        
        # Setup root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_config.get('level', 'INFO')))
        
        # Clear existing handlers
        root_logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # File handler with rotation
        if log_config.get('file'):
            file_handler = RotatingFileHandler(
                log_config['file'],
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
        
        # Suppress some noisy loggers
        logging.getLogger('kafka').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        cls._initialized = True
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """Get logger instance for given name"""
        if not cls._initialized:
            cls.setup_logging()
        
        if name not in cls._loggers:
            cls._loggers[name] = logging.getLogger(f"ids.{name}")
        
        return cls._loggers[name]

def get_logger(name: str) -> logging.Logger:
    """Convenience function to get logger"""
    return IDSLogger.get_logger(name)