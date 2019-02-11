#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
from sys import exit
from prometheus_client import start_http_server, exposition
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from utils import get_module_logger

logger = get_module_logger(__name__)

class DefaultCollector(object):

    def __init__(self):
        self._metrics = {
          "foo": [],
          "bar": [],
          "test": [],
          "test2": []
        }

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        self._setup_labels()

        self._get_metrics()

        for k,v in self._metrics.items():
            yield v

    def _setup_labels(self):
        # self._metrics["foo"] = GaugeMetricFamily("foo_test_metrics_total", "Test foo metrics total", labels=['foo'])
        # self._metrics["bar"] = GaugeMetricFamily("bar_test_metrics_total", "Test bar metrics total", labels=['bar'])
        # self._metrics["test"] = GaugeMetricFamily("test_metrics_total", "Test metrics total", labels=['foo', 'bar'])
        self._metrics["test2"] = GaugeMetricFamily("test_metrics_total", "Test metrics total")

    def _get_metrics(self):
        # self._metrics["foo"].add_metric(['foo_value'], 20.0)
        # self._metrics['bar'].add_metric(['bar_value'], 3.0)
        # self._metrics['test'].add_metric(['foo_v', 'bar_v'], value=60.0)
        self._metrics['test2'].add_metric([],value=50.0)

def main():
    try:
        REGISTRY.register(DefaultCollector())
        data = exposition.generate_latest(registry=REGISTRY)
        print data
        start_http_server(9230)
        # print("Polling %s. Serving at port: %s" % (args.address, port))
        print("Polling %s. Serving at port: %s" % ("127.0.0.1", "9230"))
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        REGISTRY.unregister(DefaultCollector())
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()