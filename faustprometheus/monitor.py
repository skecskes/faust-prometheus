"""Monitor using Prometheus."""
import typing
import re
from contextlib import suppress
from typing import Mapping
from faustprometheus.config import PrometheusMonitorConfig
from faustprometheus.metrics import PrometheusMetrics

from aiohttp.web import Response

from faust.exceptions import ImproperlyConfigured
from faust import web
from faust.sensors.monitor import Monitor, TPOffsetMapping
from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT
from faust.types import (
    AppT,
    CollectionT,
    EventT,
    Message,
    PendingMessage,
    RecordMetadata,
    StreamT,
    TP,
)

try:
    import prometheus_client
    from prometheus_client import (Counter, Gauge, Histogram, generate_latest, REGISTRY)
except ImportError:  # pragma: no cover
    prometheus_client = None

RE_NORMALIZE = re.compile(r'[\<\>:\s]+')
RE_NORMALIZE_SUBSTITUTION = '_'


class PrometheusMonitor(Monitor):
    """
    Prometheus Faust Sensor.

    This sensor, records statistics using prometheus_client and expose
    them using the aiohttp server running under /metrics by default

    Usage:
        import faust
        from faustprometheus.monitor import PrometheusMonitor

        app = faust.App('example', broker='kafka://')
        app.monitor = PrometheusMonitor(app, config=PrometheusConfig())
    """

    ERROR = 'error'
    COMPLETED = 'completed'
    KEYS_RETRIEVED = 'keys_retrieved'
    KEYS_UPDATED = 'keys_updated'
    KEYS_DELETED = 'keys_deleted'

    def __init__(self, app: AppT, pm_config: PrometheusMonitorConfig = None, **kwargs) -> None:
        self.app = app
        if pm_config is None:
            self.pm_config = PrometheusMonitorConfig
        else:
            self.pm_config = pm_config

        if prometheus_client is None:
            raise ImproperlyConfigured(
                'prometheus_client requires `pip install prometheus_client`.')

        self._python_gc_metrics()
        self._metrics = PrometheusMetrics(self.pm_config)
        self.expose_metrics()
        super().__init__(**kwargs)

    # TODO: for now turn off default python garbage collection metrics.
    #  If needed later, look into implementing them with labels
    def _python_gc_metrics(self, remove: bool = True):
        collectors = REGISTRY._names_to_collectors.values()
        for name in list(collectors):
            with suppress(KeyError):
                REGISTRY.unregister(name)

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        super().on_message_in(tp, offset, message)

        self._metrics.messages_received.inc()
        self._metrics.active_messages.inc()
        self._metrics.messages_received_per_topics.labels(topic=tp.topic).inc()
        self._metrics.messages_received_per_topics_partition.labels(
            topic=tp.topic, partition=tp.partition).set(offset)

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> typing.Optional[typing.Dict]:
        """Call when stream starts processing an event."""
        state = super().on_stream_event_in(tp, offset, stream, event)
        self._metrics.total_events.inc()
        self._metrics.total_active_events.inc()
        self._metrics.total_events_per_stream.labels(
            stream=f'stream.{self._stream_label(stream)}.events').inc()

        return state

    def _normalize(self, name: str,
                   *,
                   pattern: typing.Pattern = RE_NORMALIZE,
                   substitution: str = RE_NORMALIZE_SUBSTITUTION) -> str:
        return pattern.sub(substitution, name)

    def _stream_label(self, stream: StreamT) -> str:
        return self._normalize(
            stream.shortlabel.lstrip('Stream:'),
        ).strip('_').lower()

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: typing.Dict = None) -> None:
        """Call when stream is done processing an event."""
        super().on_stream_event_out(tp, offset, stream, event, state)
        self._metrics.total_active_events.dec()
        self._metrics.events_runtime_latency.observe(
            self.secs_to_ms(self.events_runtime[-1]))

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        super().on_message_out(tp, offset, message)
        self._metrics.active_messages.dec()

    def on_table_get(self, table: CollectionT, key: typing.Any) -> None:
        """Call when value in table is retrieved."""
        super().on_table_get(table, key)
        self._metrics.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_RETRIEVED).inc()

    def on_table_set(self, table: CollectionT, key: typing.Any,
                     value: typing.Any) -> None:
        """Call when new value for key in table is set."""
        super().on_table_set(table, key, value)
        self._metrics.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_UPDATED).inc()

    def on_table_del(self, table: CollectionT, key: typing.Any) -> None:
        """Call when key in a table is deleted."""
        super().on_table_del(table, key)
        self._metrics.table_operations.labels(
            table=f'table.{table.name}',
            operation=self.KEYS_DELETED).inc()

    def on_commit_completed(self, consumer: ConsumerT,
                            state: typing.Any) -> None:
        """Call when consumer commit offset operation completed."""
        super().on_commit_completed(consumer, state)
        self._metrics.consumer_commit_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> typing.Any:
        """Call when message added to producer buffer."""
        self._metrics.topic_messages_sent.labels(topic=f'topic.{topic}').inc()

        return super().on_send_initiated(
            producer, topic, message, keysize, valsize)

    def on_send_completed(self,
                          producer: ProducerT,
                          state: typing.Any,
                          metadata: RecordMetadata) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        self._metrics.total_sent_messages.inc()
        self._metrics.producer_send_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: typing.Any) -> None:
        """Call when producer was unable to publish message."""
        super().on_send_error(producer, exc, state)
        self._metrics.total_error_messages_sent.inc()
        self._metrics.producer_error_send_latency.observe(
            self.ms_since(typing.cast(float, state)))

    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: typing.Dict,
                            exc: BaseException) -> None:
        """Partition assignor did not complete assignor due to error."""
        super().on_assignment_error(assignor, state, exc)
        self._metrics.assignment_operations.labels(operation=self.ERROR).inc()
        self._metrics.assign_latency.observe(
            self.ms_since(state['time_start']))

    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: typing.Dict) -> None:
        """Partition assignor completed assignment."""
        super().on_assignment_completed(assignor, state)
        self._metrics.assignment_operations.labels(operation=self.COMPLETED).inc()
        self._metrics.assign_latency.observe(
            self.ms_since(state['time_start']))

    def on_rebalance_start(self, app: AppT) -> typing.Dict:
        """Cluster rebalance in progress."""
        state = super().on_rebalance_start(app)
        self._metrics.total_rebalances.inc()

        return state

    def on_rebalance_return(self, app: AppT, state: typing.Dict) -> None:
        """Consumer replied assignment is done to broker."""
        super().on_rebalance_return(app, state)
        self._metrics.total_rebalances.dec()
        self._metrics.total_rebalances_recovering.inc()
        self._metrics.rebalance_done_consumer_latency.observe(
            self.ms_since(state['time_return']))

    def on_rebalance_end(self, app: AppT, state: typing.Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        super().on_rebalance_end(app, state)
        self._metrics.total_rebalances_recovering.dec()
        self._metrics.rebalance_done_latency.observe(
            self.ms_since(state['time_end']))

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self._metrics.count_metrics_by_name.labels(metric=metric_name).inc(count)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            self._metrics.topic_partition_offset_commited.labels(
                topic=tp.topic, partition=tp.partition).set(offset)

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
        super().track_tp_end_offset(tp, offset)
        self._metrics.topic_partition_end_offset.labels(
            topic=tp.topic, partition=tp.partition).set(offset)

    def on_web_request_end(self,
                           app: AppT,
                           request: web.Request,
                           response: typing.Optional[web.Response],
                           state: typing.Dict,
                           *,
                           view: web.View = None) -> None:
        """Web server finished working on request."""
        super().on_web_request_end(app, request, response, state, view=view)
        status_code = int(state['status_code'])
        self._metrics.http_status_codes.labels(status_code=status_code).inc()
        self._metrics.http_latency.observe(
            self.ms_since(state['time_end']))

    def expose_metrics(self) -> None:
        """Expose prometheus metrics using the current aiohttp application."""

        @self.app.page(self.pm_config.path)
        async def metrics_handler(self, request):
            headers = {
                'Content-Type': 'text/plain; version=0.0.4; charset=utf-8'
            }

            return Response(
                body=generate_latest(REGISTRY), headers=headers, status=200)

    def __reduce_keywords__(self) -> Mapping:
        pass
