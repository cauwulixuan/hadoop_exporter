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

class MapReduceMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "mapreduce", "jobhistoryserver")
        self._hadoop_jobhistoryserver_metrics = {}
        # for i in range(len(self._file_list)):
        #     self._hadoop_jobhistoryserver_metrics.setdefault(self._file_list[i], {})


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
            # self._setup_metrics_labels()
    
            # add metric value to every metric.
            # self._get_metrics(self._beans)
    
            # update namenode metrics with common metrics
            common_metrics = common_metrics_info(self._cluster, beans, "mapreduce", "jobhistoryserver")
            self._hadoop_jobhistoryserver_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_jobhistoryserver_metrics[service]:
                    yield self._hadoop_jobhistoryserver_metrics[service][metric]



def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.mapreduce2_url
        REGISTRY.register(MapReduceMetricCollector(cluster, v))

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