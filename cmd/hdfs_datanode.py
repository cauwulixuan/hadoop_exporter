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

class DataNodeMetricCollector(MetricCol):
    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hdfs", "datanode")
        self._hadoop_datanode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_datanode_metrics.setdefault(self._file_list[i], {})


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
            common_metrics = common_metrics_info(self._cluster, beans, "hdfs", "datanode")
            self._hadoop_datanode_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_datanode_metrics[service]:
                    yield self._hadoop_datanode_metrics[service][metric]

    def _setup_dninfo_labels(self):
        for metric in self._metrics['DataNodeInfo']:
            if 'ActorState' in metric:
                label = ["cluster", "version", "host"]
                name = "_".join([self._prefix, 'actor_state'])
            elif 'VolumeInfo' in metric:
                label = ["cluster", "version", "path", "state"]
                name = "_".join([self._prefix, 'volume_state'])
            else:
                label = ["cluster", "version"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join([self._prefix, snake_case])
            self._hadoop_datanode_metrics['DataNodeInfo'][metric] = GaugeMetricFamily(name,
                                                                                      self._metrics['DataNodeInfo'][metric],
                                                                                      labels=label)
            
    def _setup_dnactivity_labels(self):
        block_flag, client_flag = 1, 1
        for metric in self._metrics['DataNodeActivity']:
            if 'Blocks' in metric:
                if block_flag:
                    label = ['cluster', 'host', 'oper']
                    key = "Blocks"
                    name = "block_operations_total"
                    descriptions = "Total number of blocks in different oprations"
                    block_flag = 0
                else:
                    continue
            elif 'Client' in metric:
                if client_flag:
                    label = ['cluster', 'host', 'oper', 'client']
                    key = "Client"
                    name = "from_client_total"
                    descriptions = "Total number of each operations from different client"
                    client_flag = 0
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ['cluster', 'host']
                key = metric
                name = snake_case                    
                descriptions = self._metrics['DataNodeActivity'][metric]
            self._hadoop_datanode_metrics['DataNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                       descriptions,
                                                                                       labels=label)

    def _setup_dnvolume_labels(self):
        iorate_num_flag, iorate_avg_flag = 1, 1
        for metric in self._metrics['DataNodeVolume']:
            if 'IoRateNumOps' in metric:
                if iorate_num_flag:
                    label = ['cluster', 'host', 'oper']
                    iorate_num_flag = 0
                    key = "IoRateNumOps"
                    name = "file_io_operations_total"
                    descriptions = "The number of each file io operations within an interval time of metric"
                else:
                    continue
            elif 'IoRateAvgTime' in metric:
                if iorate_avg_flag:
                    label = ['cluster', 'host', 'oper']
                    iorate_avg_flag = 0
                    key = "IoRateAvgTime"
                    name = "file_io_operations_milliseconds"
                    descriptions = "Mean time of each file io operations in milliseconds"
                else:
                    continue
            else:
                label = ['cluster', 'host']
                key = metric
                descriptions = self._metrics['DataNodeVolume'][metric]
                if 'NumOps' in metric:
                    name = "_".join([re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("NumOps")[0]).lower(), "total"])
                elif 'AvgTime' in metric:
                    name = "_".join([re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("AvgTime")[0]).lower(), "time_milliseconds"])
                else:
                    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            self._hadoop_datanode_metrics['DataNodeVolume'][key] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                     descriptions,
                                                                                     labels = label)
    
    def _setup_fsdatasetstate_labels(self):
        for metric in self._metrics['FSDatasetState']:
            label = ['cluster', 'host']
            if "Num" in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("Num")[1]).lower()
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            self._hadoop_datanode_metrics['FSDatasetState'][metric] = GaugeMetricFamily("_".join([self._prefix, snake_case]),
                                                                                        self._metrics['FSDatasetState'][metric],
                                                                                        labels = label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self._setup_dninfo_labels()
            if 'DataNodeActivity' in self._metrics:
                self._setup_dnactivity_labels()
            if 'DataNodeVolume' in self._metrics:
                self._setup_dnvolume_labels()
            if 'FSDatasetState' in self._metrics:
                self._setup_fsdatasetstate_labels()

    def _get_dninfo_metrics(self, bean):
        for metric in self._metrics['DataNodeInfo']:
            version = bean['Version']
            if 'ActorState' in metric:
                if 'BPServiceActorInfo' in bean:
                    actor_info_list = yaml.safe_load(bean['BPServiceActorInfo'])
                    for j in range(len(actor_info_list)):
                        host = actor_info_list[j]['NamenodeAddress'].split(':')[0]
                        label = [self._cluster, version, host]
                        if 'state' == "RUNNING":
                            value = 1.0
                        else:
                            value = 0.0
                else:
                    continue
            elif 'VolumeInfo' in metric:
                if 'VolumeInfo' in bean:
                    volume_info_dict = yaml.safe_load(bean['VolumeInfo'])
                    for k, v in volume_info_dict.items():
                        path = k
                        for key, val in v.items():
                            state = key
                            label = [self._cluster, version, path, state]
                            value = val
                else:
                    continue
            else:
                label = [self._cluster, version]
                value = bean[metric]
            self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)

    def _get_dnactivity_metrics(self, bean):
        for metric in self._metrics['DataNodeActivity']:
            host = bean['tag.Hostname']
            label = [self._cluster, host]
            if 'Blocks' in metric:
                oper = metric.split("Blocks")[1]
                label.append(oper)
                key = "Blocks"
            elif 'Client' in metric:
                oper = metric.split("Client")[0].split("From")[0]
                client = metric.split("Client")[0].split("From")[1]
                label.extend([oper, client])
                key = "Client"
            else:
                key = metric
            self._hadoop_datanode_metrics['DataNodeActivity'][key].add_metric(label,
                                                                              bean[metric] if metric in bean else 0)

    def _get_dnvolume_metrics(self, bean):
        for metric in self._metrics['DataNodeVolume']:
            host = bean['tag.Hostname']
            label = [self._cluster, host]
            if 'IoRateNumOps' in metric:
                oper = metric.split("IoRateNumOps")[0]
                label.append(oper)
                key = "IoRateNumOps"
            elif 'IoRateAvgTime' in metric:
                oper = metric.split("IoRateAvgTime")[0]
                label.append(oper)
                key = "IoRateAvgTime"
            else:
                key = metric
            self._hadoop_datanode_metrics['DataNodeVolume'][key].add_metric(label,
                                                                            bean[metric] if metric in bean else 0)

    def _get_fsdatasetstate_metrics(self, bean):
        for metric in self._metrics['FSDatasetState']:
            host = bean['tag.Hostname']
            label = [self._cluster, host]
            self._hadoop_datanode_metrics['FSDatasetState'][metric].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'DataNodeInfo' in beans[i]['name']:
                self._get_dninfo_metrics(beans[i])
            if 'DataNodeActivity' in beans[i]['name']:
                self._get_dnactivity_metrics(beans[i])
            if 'DataNodeVolume' in beans[i]['name']:
                self._get_dnvolume_metrics(beans[i])
            if 'FSDatasetState' in beans[i]['name'] and 'FSDatasetState' in beans[i]['modelerType']:
                self._get_fsdatasetstate_metrics(beans[i])
                    
def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.datanode_url
        REGISTRY.register(DataNodeMetricCollector(cluster, v))

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