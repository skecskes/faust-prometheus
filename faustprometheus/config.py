

class PrometheusMonitorConfig:
    """
    Prometheus Monitor Config

    Usage:
        from faustprometheus.config import PrometheusMonitorConfig

        monitor_config = PrometheusMonitorConfig(
            path='/metrics',
            namespace='faust',
            subystem='foo-app',
            labels=['v2.3', 'prod']
            )
    """

    def __init__(self,  labels: list = None, namespace: str = '', subsystem: str = '', path: str = '/metrics'):
        self.namespace = namespace
        self.subsystem = subsystem
        if labels is None:
            self.labels = []
        else:
            self.labels = labels
        self.path = path
