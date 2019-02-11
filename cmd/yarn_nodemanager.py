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



class NodeManagerMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "yarn", "nodemanager")
        self._hadoop_nodemanager_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_nodemanager_metrics.setdefault(self._file_list[i], {})


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
            self._setup_metrics_labels(beans)
    
            # add metric value to every metric.
            self._get_metrics(beans)
    
            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(self._cluster, beans, "yarn", "nodemanager")
            self._hadoop_nodemanager_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_nodemanager_metrics[service]:
                    yield self._hadoop_nodemanager_metrics[service][metric]

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            label = ["cluster", "host"]
            for service in self._metrics:
                if service in beans[i]['name']:
                    for metric in self._metrics[service]:
                        name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()                
                        self._hadoop_nodemanager_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                              self._metrics[service][metric],
                                                                                              labels=label)
                else:
                    continue

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'tag.Hostname' in beans[i]:
                host = beans[i]['tag.Hostname']
                label = [self._cluster, host]
                break
            else:
                continue
        for i in range(len(beans)):
            for service in self._metrics:
                if service in beans[i]['name']:
                    for metric in beans[i]:
                        if metric in self._metrics[service]:
                            self._hadoop_nodemanager_metrics[service][metric].add_metric(label, beans[i][metric])
                        else:
                            pass
                else:
                    continue

def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.nodemanager_url
        REGISTRY.register(NodeManagerMetricCollector(cluster, v))

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