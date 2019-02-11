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


class HiveServerMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hive", "hiveserver2")
        self._hadoop_hiveserver2_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_hiveserver2_metrics.setdefault(self._file_list[i], {})


    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'
        try:
            count = 0
            # In case no metrics we need in the jmx url, a time sleep and while-loop was set here to wait for the KEY metrics
            while count < 5:
                beans = utils.get_metrics(self._url)
                if 'init_total_count_tables' not in beans:
                    count += 1
                    time.sleep(1)
                    continue
                else:
                    break
        except:
            logger.info("Can't scrape metrics from url: {0}".format(self._url))
        else:
            pass
        finally:
            # set up all metrics with labels and descriptions.
            self._setup_labels(beans)

            # add metric value to every metric.
            self._get_metrics(beans)

            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(self._cluster, beans, "hive", "hiveserver2")
            self._hadoop_hiveserver2_metrics.update(common_metrics())

            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_hiveserver2_metrics[service]:
                    yield self._hadoop_hiveserver2_metrics[service][metric]

    def _setup_node_labels(self, bean, service):
        label = ["cluster", "host", "client_id", "node_id"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                self._hadoop_hiveserver2_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, 'producer_node', name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)

    def _setup_topic_labels(self, bean, service):
        label = ["cluster", "host", "client_id", "topic"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                self._hadoop_hiveserver2_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, 'producer_topic', name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)
    
    def _setup_producer_labels(self, bean, service):
        label = ["cluster", "host", "client_id"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                self._hadoop_hiveserver2_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)
    
    def _setup_other_labels(self, bean, service):
        label = ["cluster", "host"]
        for metric in self._metrics[service]:
            if metric in bean:
                name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                self._hadoop_hiveserver2_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                      self._metrics[service][metric],
                                                                                      labels=label)

    def _setup_labels(self, beans):
        # The metrics we want to export.
        for service in self._metrics:
            for i in range(len(beans)):
                if 'producer-node-metrics' == service and 'type=producer-node-metrics' in beans[i]['name']:
                    self._setup_node_labels(beans[i], service)
                elif 'producer-topic-metrics' == service and 'type=producer-topic-metrics' in beans[i]['name']:
                    self._setup_topic_labels(beans[i], service)
                elif 'producer-metrics' == service and 'type=producer-metrics' in beans[i]['name'] or 'kafka-metrics-count' == service and 'type=kafka-metrics-count' in beans[i]['name']:
                    self._setup_producer_labels(beans[i], service)
                elif service in beans[i]['name']:
                    self._setup_other_labels(beans[i], service)
                else:
                    continue


    def _get_node_metrics(self, bean, service, host):
        client_id = bean['name'].split('client-id=')[1].split(',')[0]
        node_id = bean['name'].split('node-id=')[1].split(',')[0]
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_hiveserver2_metrics[service][metric].add_metric([self._cluster, host, client_id, node_id], bean[metric])
            else:
                continue

    def _get_topic_metrics(self, bean, service, host):
        client_id = bean['name'].split('client-id=')[1].split(',')[0]
        topic = bean['name'].split('topic=')[1].split(',')[0]
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_hiveserver2_metrics[service][metric].add_metric([self._cluster, host, client_id, topic], bean[metric])
            else:
                continue

    def _get_producer_metrics(self, bean, service, host):
        client_id = bean['name'].split('client-id=')[1].split(',')[0]
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_hiveserver2_metrics[service][metric].add_metric([self._cluster, host, client_id], bean[metric])
            else:
                continue

    def _get_other_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_hiveserver2_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
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
                if 'producer-node-metrics' == service and 'type=producer-node-metrics' in beans[i]['name']:
                    self._get_node_metrics(beans[i], service, host)
                elif 'producer-topic-metrics' == service and 'type=producer-topic-metrics' in beans[i]['name']:
                    self._get_topic_metrics(beans[i], service, host)
                elif 'producer-metrics' == service and 'type=producer-metrics' in beans[i]['name'] or 'kafka-metrics-count' == service and 'type=kafka-metrics-count' in beans[i]['name']:
                    self._get_producer_metrics(beans[i], service, host)
                elif service in beans[i]['name']:
                    self._get_other_metrics(beans[i], service, host)
                else:
                    continue


def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.hive_url
        REGISTRY.register(HiveServerMetricCollector(cluster, v))

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