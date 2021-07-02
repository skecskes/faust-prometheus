from faustprometheus.config import PrometheusMonitorConfig
from prometheus_client import (Counter, Gauge, Histogram)


class PrometheusMetrics:

    def __init__(self, pm_config: PrometheusMonitorConfig):
        """
        Initialize Prometheus metrics
        """
        # On message received
        self.messages_received = Counter(
            'messages_received',
            'Total messages received',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.active_messages = Gauge(
            'active_messages',
            'Total active messages',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.messages_received_per_topics = Counter(
            'messages_received_per_topic',
            'Messages received per topic',
            ['topic'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.messages_received_per_topics_partition = Gauge(
            'messages_received_per_topics_partition',
            'Messages received per topic/partition',
            ['topic', 'partition'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.events_runtime_latency = Histogram(
            'events_runtime_s',
            'Events runtime in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # On Event Stream in
        self.total_events = Counter(
            'total_events',
            'Total events received',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.total_active_events = Gauge(
            'total_active_events',
            'Total active events',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.total_events_per_stream = Counter(
            'total_events_per_stream',
            'Events received per Stream',
            ['stream'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # On table changes get/set/del keys
        self.table_operations = Counter(
            'table_operations',
            'Total table operations',
            ['table', 'operation'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # On message send
        self.topic_messages_sent = Counter(
            'topic_messages_sent',
            'Total messages sent per topic',
            ['topic'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.total_sent_messages = Counter(
            'total_sent_messages',
            'Total messages sent',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.producer_send_latency = Histogram(
            'producer_send_latency',
            'Producer send latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.total_error_messages_sent = Counter(
            'total_error_messages_sent',
            'Total error messages sent',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.producer_error_send_latency = Histogram(
            'producer_error_send_latency',
            'Producer error send latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # Assignment
        self.assignment_operations = Counter(
            'assignment_operations',
            'Total assigment operations (completed/error)',
            ['operation'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.assign_latency = Histogram(
            'assign_latency',
            'Assignment latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # Rebalances
        self.total_rebalances = Gauge(
            'total_rebalances',
            'Total rebalances',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.total_rebalances_recovering = Gauge(
            'total_rebalances_recovering',
            'Total rebalances recovering',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.rebalance_done_consumer_latency = Histogram(
            'rebalance_done_consumer_latency',
            'Consumer replying that rebalance is done to broker in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.rebalance_done_latency = Histogram(
            'rebalance_done_latency',
            'Rebalance finished latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # Count Metrics by name
        self.count_metrics_by_name = Gauge(
            'metrics_by_name',
            'Total metrics by name',
            ['metric'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # Web
        self.http_status_codes = Counter(
            'http_status_codes',
            'Total http_status code',
            ['status_code'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.http_latency = Histogram(
            'http_latency',
            'Http response latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )

        # Topic/Partition Offsets
        self.topic_partition_end_offset = Gauge(
            'topic_partition_end_offset',
            'Offset ends per topic/partition',
            ['topic', 'partition'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.topic_partition_offset_commited = Gauge(
            'topic_partition_offset_commited',
            'Offset commited per topic/partition',
            ['topic', 'partition'],
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
        self.consumer_commit_latency = Histogram(
            'consumer_commit_latency',
            'Consumer commit latency in seconds',
            namespace=pm_config.namespace,
            subsystem=pm_config.subsystem
        )
