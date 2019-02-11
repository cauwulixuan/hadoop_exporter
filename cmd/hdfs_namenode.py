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


class NameNodeMetricCollector(MetricCol):

    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hdfs", "namenode")
        self._hadoop_namenode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_namenode_metrics.setdefault(self._file_list[i], {})


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
            common_metrics = common_metrics_info(self._cluster, beans, "hdfs", "namenode")
            self._hadoop_namenode_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_namenode_metrics[service]:
                    yield self._hadoop_namenode_metrics[service][metric]

    def _setup_nnactivity_labels(self):
        num_namenode_flag,avg_namenode_flag,ops_namenode_flag = 1,1,1
        for metric in self._metrics['NameNodeActivity']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            label = ["cluster", "method"]
            if "NumOps" in metric:
                if num_namenode_flag:
                    key = "MethodNumOps"
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_method_ops_total"]),
                                                                                               "Total number of the times the method is called.",
                                                                                               labels=label)
                    num_namenode_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_namenode_flag:
                    key = "MethodAvgTime"
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_method_avg_time_milliseconds"]),
                                                                                               "Average turn around time of the method in milliseconds.",
                                                                                               labels=label)
                    avg_namenode_flag = 0
                else:
                    continue
            else:
                if ops_namenode_flag:
                    ops_namenode_flag = 0
                    key = "Operations"
                    self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily("_".join([self._prefix, "nnactivity_operations_total"]),
                                                                                               "Total number of each operation.",
                                                                                               labels=label)
                else:
                    continue

    def _setup_startupprogress_labels(self):
        sp_count_flag,sp_elapsed_flag,sp_total_flag,sp_complete_flag = 1,1,1,1
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "ElapsedTime" == metric:
                key = "ElapsedTime"
                name = "total_elapsed_time_milliseconds"
                descriptions = "Total elapsed time in milliseconds."
            elif "PercentComplete" == metric:
                key = "PercentComplete"
                name = "complete_rate"
                descriptions = "Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0)."
             
            elif "Count" in metric:                    
                if sp_count_flag:
                    sp_count_flag = 0
                    key = "PhaseCount"
                    name = "phase_count"
                    label = ["cluster", "phase"]
                    descriptions = "Total number of steps completed in the phase."
                else:
                    continue
            elif "ElapsedTime" in metric:
                if sp_elapsed_flag:
                    sp_elapsed_flag = 0
                    key = "PhaseElapsedTime"
                    name = "phase_elapsed_time_milliseconds"
                    label = ["cluster", "phase"]
                    descriptions = "Total elapsed time in the phase in milliseconds."
                else:
                    continue
            elif "Total" in metric:
                if sp_total_flag:
                    sp_total_flag = 0
                    key = "PhaseTotal"
                    name = "phase_total"
                    label = ["cluster", "phase"]
                    descriptions = "Total number of steps in the phase."
                else:
                    continue
            elif "PercentComplete" in metric:
                if sp_complete_flag:
                    sp_complete_flag = 0
                    key = "PhasePercentComplete"
                    name = "phase_complete_rate"
                    label = ["cluster", "phase"]
                    descriptions = "Current rate completed in the phase  (The max value is not 100 but 1.0)."                    
                else:
                    continue                
            else:
                key = metric
                name = snake_case
                label = ["cluster"]
                descriptions = self._metrics['StartupProgress'][metric]            
            self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily("_".join([self._prefix, "startup_process", name]),
                                                                                      descriptions,
                                                                                      labels = label)

    def _setup_fsnamesystem_labels(self):
        cap_flag = 1
        for metric in self._metrics['FSNamesystem']:
            if metric.startswith('Capacity'):
                if cap_flag:
                    cap_flag = 0
                    key = "capacity"
                    label = ["cluster", "mode"]
                    name = "capacity_bytes"
                    descriptions = "Current DataNodes capacity in each mode in bytes"
                else:
                    continue    
            else:
                key = metric
                label = ["cluster"]
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                descriptions = self._metrics['FSNamesystem'][metric]
            self._hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily("_".join([self._prefix, "fsname_system", name]),
                                                                                   descriptions,
                                                                                   labels = label)

    def _setup_fsnamesystem_state_labels(self):
        num_flag = 1
        for metric in self._metrics['FSNamesystemState']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'DataNodes' in metric:
                if num_flag:
                    num_flag = 0
                    key = "datanodes_num"
                    label = ["cluster", "state"]
                    descriptions = "Number of datanodes in each state"
                else:
                    continue
            else:
                key = metric
                label = ["cluster"]
                descriptions = self._metrics['FSNamesystemState'][metric]
            self._hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily("_".join([self._prefix, "fsname_system", snake_case]),
                                                                                        descriptions,
                                                                                        labels = label)

    def _setup_retrycache_labels(self):
        cache_flag = 1
        for metric in self._metrics['RetryCache']:
            if cache_flag:
                cache_flag = 0
                key = "cache"
                label = ["cluster", "mode"]
                self._hadoop_namenode_metrics['RetryCache'][key] = GaugeMetricFamily("_".join([self._prefix, "cache_total"]), 
                                                                                     "Total number of RetryCache in each mode", 
                                                                                     labels = label)
            else:
                continue

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self._setup_nnactivity_labels()
    
            if 'StartupProgress' in beans[i]['name']:
                self._setup_startupprogress_labels()
                    
            if 'FSNamesystem' in beans[i]['name']:
                self._setup_fsnamesystem_labels()
    
            if 'FSNamesystemState' in beans[i]['name']:
                self._setup_fsnamesystem_state_labels()
    
            if 'RetryCache' in beans[i]['name']:
                self._setup_retrycache_labels()


    def _get_nnactivity_metrics(self, bean):
        for metric in self._metrics['NameNodeActivity']:
            if "NumOps" in metric:
                method = metric.split('NumOps')[0]
                label = [self._cluster, method]
                key = "MethodNumOps"
            elif "AvgTime" in metric:
                method = metric.split('AvgTime')[0]
                label = [self._cluster, method]
                key = "MethodAvgTime"
            else:
                if "Ops" in metric:
                    method = metric.split('Ops')[0]
                else:
                    method = metric
                label = [self._cluster, method]                    
                key = "Operations"
            self._hadoop_namenode_metrics['NameNodeActivity'][key].add_metric(label,
                                                                              bean[metric] if metric in bean else 0)

    def _get_startupprogress_metrics(self, bean):
        for metric in self._metrics['StartupProgress']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if "Count" in metric:
                key = "PhaseCount"
                phase = metric.split("Count")[0]
                label = [self._cluster, phase]
            elif "ElapsedTime" in metric and "ElapsedTime" != metric:
                key = "PhaseElapsedTime"
                phase = metric.split("ElapsedTime")[0]
                label = [self._cluster, phase]
            elif "Total" in metric:
                key = "PhaseTotal"
                phase = metric.split("Total")[0]
                label = [self._cluster, phase]
            elif "PercentComplete" in metric and "PercentComplete" != metric:
                key = "PhasePercentComplete"
                phase = metric.split("PercentComplete")[0]
                label = [self._cluster, phase]
            else:
                key = metric
                label = [self._cluster]
            self._hadoop_namenode_metrics['StartupProgress'][key].add_metric(label,
                                                                             bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_metrics(self, bean):
        for metric in self._metrics['FSNamesystem']:
            key = metric
            if 'HAState' in metric:
                label = [self._cluster]
                if 'initializing' == bean['tag.HAState']:
                    value = 0.0
                elif 'active' == bean['tag.HAState']:
                    value = 1.0
                elif 'standby' == bean['tag.HAState']:
                    value = 2.0
                elif 'stopping' == bean['tag.HAState']:
                    value = 3.0
                else:
                    value = 9999
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, value)
            elif metric.startswith("Capacity"):
                key = 'capacity'
                mode = metric.split("Capacity")[1]
                label = [self._cluster, mode]
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)
            else:
                label = [self._cluster]
                self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, bean[metric] if metric in bean else 0)

    def _get_fsnamesystem_state_metrics(self, bean):
        for metric in self._metrics['FSNamesystemState']:
            label = [self._cluster]
            key = metric
            if 'FSState' in metric:
                if 'Safemode' == bean['FSState']:
                    value = 0.0
                elif 'Operational' == bean['FSState']:
                    value = 1.0
                else:
                    value = 9999
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, value)
            elif "TotalSyncTimes" in metric:
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, float(re.sub('\s', '', bean[metric])) if metric in bean and bean[metric] else 0)
            elif "DataNodes" in metric:
                key = 'datanodes_num'
                state = metric.split("DataNodes")[0].split("Num")[1]
                label = [self._cluster, state]
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

            else:
                self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_retrycache_metrics(self, bean):
        for metric in self._metrics['RetryCache']:
            key = "cache"
            label = [self._cluster, metric.split('Cache')[1]]
            self._hadoop_namenode_metrics['RetryCache'][key].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)


    def _get_metrics(self, beans):
        for i in range(len(beans)):
            if 'NameNodeActivity' in beans[i]['name']:
                self._get_nnactivity_metrics(beans[i])
            if 'StartupProgress' in beans[i]['name']:
                self._get_startupprogress_metrics(beans[i])
            if 'FSNamesystem' in beans[i]['name'] and 'FSNamesystemState' not in beans[i]['name']:
                self._get_fsnamesystem_metrics(beans[i])
            if 'FSNamesystemState' in beans[i]['name']:
                self._get_fsnamesystem_state_metrics(beans[i])
            if 'RetryCache' in beans[i]['name']:
                self._get_retrycache_metrics(beans[i])
                    


def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.namenode_url
        REGISTRY.register(NameNodeMetricCollector(cluster, v))

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