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


class HBaseRegionServerMetricCollector(MetricCol):
    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hbase", "regionserver")
        self._hadoop_regionserver_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_regionserver_metrics.setdefault(self._file_list[i], {})


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
            common_metrics = common_metrics_info(self._cluster, beans, "hbase", "regionserver")
            self._hadoop_regionserver_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_regionserver_metrics[service]:
                    yield self._hadoop_regionserver_metrics[service][metric]

    def _setup_labels(self, beans):
        for i in range(len(beans)):
            for service in self._metrics:
                if service in beans[i]['name']:
                    for metric in self._metrics[service]:
                        name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                        if 'region_metric' in metric:
                            label = ['cluster', 'host', 'region']
                        elif 'table_metric' in metric:
                            label = ['cluster', 'host', 'table']
                        elif 'User_metric' in metric:
                            label = ['cluster', 'host', 'user']
                        else:
                            label = ['cluster', 'host']
                        self._hadoop_regionserver_metrics[service][metric] = GaugeMetricFamily("_".join([self._prefix, service.lower(), name]),
                                                                                                self._metrics[service][metric],
                                                                                                labels=label)

    def _get_regions_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                if "_region_" in metric and 'metric' in metric:
                    region = metric.split("_metric_")[0].split("region_")[1]
                    key = "".join(["region", metric.split(region)[-1]])
                    self._hadoop_regionserver_metrics[service][key].add_metric([self._cluster, host, region], bean[metric])
                elif metric in self._metrics[service]:
                    self._hadoop_regionserver_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
                else:
                    continue
            else:
                continue

    def _get_tables_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                if "_table_" in metric and 'metric' in metric:
                    table = metric.split("_metric_")[0].split("table_")[1]
                    key = "".join(["table", metric.split(table)[-1]])
                    self._hadoop_regionserver_metrics[service][key].add_metric([self._cluster, host, table], bean[metric])
                elif metric in self._metrics[service]:
                    self._hadoop_regionserver_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
                else:
                    continue
            else:
                continue

    def _get_users_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                if "User_" in metric and '_metric_' in metric:
                    user = metric.split("_metric_")[0].split("User_")[1]
                    key = "".join(["User", metric.split(user)[-1]])
                    self._hadoop_regionserver_metrics[service][key].add_metric([self._cluster, host, user], bean[metric])
                elif metric in self._metrics[service]:
                    self._hadoop_regionserver_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
                else:
                    continue
            else:
                continue

    def _get_other_metrics(self, bean, service, host):
        for metric in bean:
            if metric in self._metrics[service]:
                self._hadoop_regionserver_metrics[service][metric].add_metric([self._cluster, host], bean[metric])
            else:
                continue

    def _get_metrics(self, beans):
        
        for i in range(len(beans)):
            if 'tag.Hostname' in beans[i]:
                host = beans[i]['tag.Hostname']
                break
            else:
                continue

        for i in range(len(beans)):
            for service in self._metrics:
                if 'Regions' == service and 'sub={0}'.format(service) in beans[i]['name']:
                    self._get_regions_metrics(beans[i], service, host)
                elif 'Tables' == service and 'sub={0}'.format(service) in beans[i]['name']:
                    self._get_tables_metrics(beans[i], service, host)
                elif 'Users' == service and 'sub={0}'.format(service) in beans[i]['name']:
                    self._get_users_metrics(beans[i], service, host)
                elif 'sub={0}'.format(service) in beans[i]['name']:
                    self._get_other_metrics(beans[i], service, host)
                else:
                    continue


def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.regionserver_url
        REGISTRY.register(HBaseRegionServerMetricCollector(cluster, v))

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

