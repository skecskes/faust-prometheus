# Faust Prometheus

This library uses sensors of Faust. Those sensors are events based. Our monitor implements that interface and hooks some actions when those events happen. That allows us to create our own prometheus `Counters`, `Gauges` and `Histograms`.
I created some predefined metrics, but we can expand on them in the future.

## Installation

To use this library in your faust project just add `faustprometheus` or install it through pip:


    pip install faustprometheus
    

## Usage

Add `PrometheusMonitor` to the app as seen below:
 
 
        import faust
        from faustprometheus.monitor import PrometheusMonitor

        app = faust.App('example', broker='kafka://')
        app.monitor = PrometheusMonitor(app)
        
        
You can also configure some global options to monitor through `PrometheusMonitorConfig`,
such as 2 levels of prefixes, through `namespace` and `subsystem`. The path exposing prometheus metrics can be set through `metrics` config, with `/metrics` as default value.
I added labels to config, but are not applied to monitor due to lack of my understanding of faust sensors and prometheus.


        from faustprometheus.config import PrometheusMonitorConfig

        monitor_config = PrometheusMonitorConfig(
            path='/metrics',
            namespace='faust',
            subystem='foo-app',
            labels=['v2.3', 'prod']
            )        
            
## Tests

Library is fully unit tested and can be run by


        pytest
        
