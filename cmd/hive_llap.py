#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re
import time
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily, REGISTRY

import utils
from utils import get_module_logger
from consul import Consul
from common import MetricCol, common_metrics_info

logger = get_module_logger(__name__)

class HiveLlapDaemonMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hive", "llapdaemon")
        self._hadoop_llapdaemon_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_llapdaemon_metrics.setdefault(self._file_list[i], {})


    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        try:
            beans = utils.get_metrics(self._url)
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
            pass
        else:
            # set up all metrics with labels and descriptions.
            self._setup_labels(beans)
    
            # add metric value to every metric.
            self._get_metrics(beans)
    
            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(self._cluster, beans, "hive", "llapdaemon")
            self._hadoop_llapdaemon_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_llapdaemon_metrics[service]:
                    yield self._hadoop_llapdaemon_metrics[service][metric]

    def _setup_executor_labels(self, bean, service):
        for metric in self._metrics[service]:
            if metric in bean:
                if "ExecutorThread" in metric:
                    label = ['cluster', 'host', 'cpu']
                else:
                    label = ['cluster', 'host']
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                self._hadoop_llapdaemon_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, service.lower(), name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)
            else:
                continue

    def _setup_other_labels(self, bean, service):
        label = ["cluster", "host"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                self._hadoop_llapdaemon_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, service.lower(), name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)
            else:
                continue

    def _setup_labels(self, beans):
        # The metrics we want to export.
        for service in self._metrics:
            for i in range(len(beans)):
                if 'LlapDaemonExecutorMetrics' == service and 'LlapDaemonExecutorMetrics' in beans[i]['name']:
                    self._setup_executor_labels(beans[i], service)
                elif service in beans[i]['name']:
                    self._setup_other_labels(beans[i], service)
                else:
                    continue

    def _get_executor_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                if "ExecutorThread" in metric:
                    cpu = "".join(['cpu', metric.split("_")[1]])
                    self._hadoop_llapdaemon_metrics[service][metric].add_metric([self._cluster, host, cpu], bean[metric])
                else:
                    self._hadoop_llapdaemon_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
            else:
                continue

    def _get_other_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_llapdaemon_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
            else:
                continue

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>
        for i in range(len(beans)):
            if 'tag.Hostname' in beans[i]:
                host = beans[i]['tag.Hostname']
                break
            else:
                continue
        for i in range(len(beans)):
            for service in self._metrics:
                if 'LlapDaemonExecutorMetrics' == service and 'LlapDaemonExecutorMetrics' in beans[i]['name']:
                    self._get_executor_metrics(beans[i], service, host)
                elif service in beans[i]['name']:
                    self._get_other_metrics(beans[i], service, host)
                else:
                    continue

def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.llapdaemon_url
        REGISTRY.register(HiveLlapDaemonMetricCollector(cluster, v))

        start_http_server(port)
        # print("Polling %s. Serving at port: %s" % (args.address, port))
        print("Polling %s. Serving at port: %s" % (args.address, port))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()