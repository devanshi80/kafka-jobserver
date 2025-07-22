"""
Health monitoring system for IDS components
"""
import time
import threading
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from enhanced_logger import get_logger
from config_file import config

@dataclass
class HealthStatus:
    """Health status for a component"""
    name: str
    healthy: bool
    status: str
    last_check: float
    error_count: int = 0
    last_error: Optional[str] = None

class HealthMonitor:
    """System health monitoring and HTTP health check endpoint"""
    
    def __init__(self):
        self.logger = get_logger('health_monitor')
        self.health_config = config.get('health_check', {})
        self.port = self.health_config.get('port', 8090)
        self.enabled = self.health_config.get('enabled', True)
        
        # Component health status
        self.component_health: Dict[str, HealthStatus] = {}
        self.system_start_time = time.time()
        
        # HTTP server
        self.http_server = None
        self.server_thread = None
        self.shutdown_event = threading.Event()
        
        # Monitoring thread
        self.monitor_thread = None
        self.check_interval = 30  # seconds

    def register_component(self, name: str, health_check_func, check_interval: int = 30):
        """Register a component for health monitoring"""
        self.component_health[name] = HealthStatus(
            name=name,
            healthy=True,
            status="initialized",
            last_check=time.time()
        )
        
        # Store the health check function (you might want to store this in a separate dict)
        setattr(self, f"{name}_health_check", health_check_func)
        
        self.logger.info(f"✅ Registered component '{name}' for health monitoring")

    def update_component_health(self, name: str, healthy: bool, status: str, error: str = None):
        """Update component health status"""
        if name in self.component_health:
            component = self.component_health[name]
            component.healthy = healthy
            component.status = status
            component.last_check = time.time()
            
            if not healthy:
                component.error_count += 1
                component.last_error = error
                self.logger.warning(f"⚠️ Component '{name}' unhealthy: {status}")
            else:
                self.logger.debug(f"✅ Component '{name}' healthy: {status}")

    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status"""
        current_time = time.time()
        uptime = current_time - self.system_start_time
        
        # Overall system status
        all_healthy = all(comp.healthy for comp in self.component_health.values())
        
        return {
            "timestamp": current_time,
            "uptime_seconds": uptime,
            "system_healthy": all_healthy,
            "components": {
                name: {
                    "healthy": comp.healthy,
                    "status": comp.status,
                    "last_check": comp.last_check,
                    "seconds_since_check": current_time - comp.last_check,
                    "error_count": comp.error_count,
                    "last_error": comp.last_error
                }
                for name, comp in self.component_health.items()
            }
        }

    def start_monitoring(self):
        """Start health monitoring services"""
        if not self.enabled:
            self.logger.info("Health monitoring disabled")
            return

        self.logger.info(f"🚀 Starting health monitoring on port {self.port}")
        
        # Start HTTP server
        self._start_http_server()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            name="HealthMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        
        self.logger.info("✅ Health monitoring started")

    def _start_http_server(self):
        """Start HTTP server for health checks"""
        try:
            handler = self._create_request_handler()
            self.http_server = HTTPServer(('0.0.0.0', self.port), handler)
            
            self.server_thread = threading.Thread(
                target=self._run_http_server,
                name="HealthHTTPServer",
                daemon=True
            )
            self.server_thread.start()
            
            self.logger.info(f"✅ Health check HTTP server started on port {self.port}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start HTTP server: {e}")

    def _create_request_handler(self):
        """Create HTTP request handler class"""
        health_monitor = self
        
        class HealthRequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/health':
                    self._handle_health_check()
                elif self.path == '/health/detailed':
                    self._handle_detailed_health()
                else:
                    self._handle_not_found()
            
            def _handle_health_check(self):
                """Handle basic health check"""
                health = health_monitor.get_system_health()
                status_code = 200 if health['system_healthy'] else 503
                
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                
                response = {
                    "status": "healthy" if health['system_healthy'] else "unhealthy",
                    "timestamp": health['timestamp'],
                    "uptime": health['uptime_seconds']
                }
                
                self.wfile.write(json.dumps(response).encode())
            
            def _handle_detailed_health(self):
                """Handle detailed health check"""
                health = health_monitor.get_system_health()
                status_code = 200 if health['system_healthy'] else 503
                
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                
                self.wfile.write(json.dumps(health, indent=2).encode())
            
            def _handle_not_found(self):
                """Handle 404 errors"""
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Not Found')
            
            def log_message(self, format, *args):
                """Override to use our logger"""
                health_monitor.logger.debug(f"HTTP: {format % args}")
        
        return HealthRequestHandler

    def _run_http_server(self):
        """Run HTTP server loop"""
        try:
            self.http_server.serve_forever()
        except Exception as e:
            if not self.shutdown_event.is_set():
                self.logger.error(f"❌ HTTP server error: {e}")

    def _monitoring_loop(self):
        """Main monitoring loop"""
        self.logger.info("🔍 Health monitoring loop started")
        
        while not self.shutdown_event.is_set():
            try:
                self._perform_health_checks()
                time.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"❌ Error in monitoring loop: {e}")
                time.sleep(5)  # Brief pause on error

    def _perform_health_checks(self):
        """Perform health checks on all registered components"""
        for name in self.component_health.keys():
            try:
                # Get the health check function
                health_check_func = getattr(self, f"{name}_health_check", None)
                
                if health_check_func and callable(health_check_func):
                    healthy = health_check_func()
                    status = "healthy" if healthy else "unhealthy"
                    self.update_component_health(name, healthy, status)
                else:
                    # If no health check function, assume healthy if recently updated
                    component = self.component_health[name]
                    age = time.time() - component.last_check
                    
                    if age > 300:  # 5 minutes
                        self.update_component_health(
                            name, False, "stale", "No recent health updates"
                        )
                    
            except Exception as e:
                self.logger.error(f"❌ Health check failed for {name}: {e}")
                self.update_component_health(name, False, "error", str(e))

    def stop_monitoring(self):
        """Stop health monitoring"""
        self.logger.info("🛑 Stopping health monitoring...")
        self.shutdown_event.set()
        
        # Stop HTTP server
        if self.http_server:
            try:
                self.http_server.shutdown()
                self.http_server.server_close()
            except Exception as e:
                self.logger.error(f"❌ Error stopping HTTP server: {e}")
        
        # Wait for threads
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(timeout=5)
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        
        self.logger.info("✅ Health monitoring stopped")

# Global health monitor instance
health_monitor = HealthMonitor()