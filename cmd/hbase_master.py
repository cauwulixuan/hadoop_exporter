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


class HBaseMasterMetricCollector(MetricCol):
    def __init__(self, cluster, url):
        MetricCol.__init__(self, cluster, url, "hbase", "master")
        self._hadoop_hbase_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_hbase_metrics.setdefault(self._file_list[i], {})


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
            common_metrics = common_metrics_info(self._cluster, beans, "hbase", "master")
            self._hadoop_hbase_metrics.update(common_metrics())
    
            for i in range(len(self._merge_list)):
                service = self._merge_list[i]
                for metric in self._hadoop_hbase_metrics[service]:
                    yield self._hadoop_hbase_metrics[service][metric]

    def _setup_server_labels(self):
        for metric in self._metrics['Server']:
            label = ["cluster", "host"]
            name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'RegionServersState' in metric:
                label.append('server')
            elif 'numRegionServers' in metric:
                name = 'live_region'
            elif 'numDeadRegionServers' in metric:
                name = 'dead_region'
            else:
                pass
            self._hadoop_hbase_metrics['Server'][metric] = GaugeMetricFamily("_".join([self._prefix, 'server', name]),
                                                                             self._metrics['Server'][metric],
                                                                             labels=label)

    def _setup_balancer_labels(self):
        balancer_flag = 1
        for metric in self._metrics['Balancer']:
            label = ["cluster", "host"]
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self._hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                   self._metrics['Balancer'][metric],
                                                                                   labels=label)
            elif 'BalancerCluster' in metric:
                if balancer_flag:
                    balancer_flag = 0
                    name = 'balancer_cluster_latency_microseconds'
                    key = 'BalancerCluster'
                    self._hadoop_hbase_metrics['Balancer'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                        "The percentile of balancer cluster latency in microseconds",
                                                                                        labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join(['balancer', snake_case])
                self._hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                   self._metrics['Balancer'][metric],
                                                                                   labels=label)

    def _setup_assignmentmanger_labels(self):
        bulkassign_flag, assign_flag = 1, 1
        for metric in self._metrics['AssignmentManger']:
            label = ["cluster", "host"]            
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self._hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                           self._metrics['AssignmentManger'][metric],
                                                                                           labels=label)
            elif 'BulkAssign' in metric:
                if bulkassign_flag:
                    bulkassign_flag = 0
                    name = 'bulkassign_latency_microseconds'
                    key = 'BulkAssign'
                    self._hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                                "The percentile of bulkassign latency in microseconds",
                                                                                                labels=label)
                else:
                    continue
            elif 'Assign' in metric:
                if assign_flag:
                    assign_flag = 0
                    name = 'assign_latency_microseconds'
                    key = 'Assign'
                    self._hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                                "The percentile of assign latency in microseconds",
                                                                                                labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join(['assignmentmanger', snake_case])
                self._hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                           self._metrics['AssignmentManger'][metric],
                                                                                           labels=label)

    def _setup_ipc_labels(self):
        total_calltime_flag, response_size_flag, process_calltime_flag, queue_calltime_flag, request_size_flag, exception_flag = 1, 1, 1, 1, 1, 1
        for metric in self._metrics['IPC']:
            label = ["cluster", "host"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = "_".join(['ipc', snake_case])
                self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                              self._metrics['IPC'][metric],
                                                                              labels=label)
            elif 'RangeCount_' in metric:
                name = metric.replace("-", "_").lower()
                self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self._prefix, 'ipc', name]),
                                                                              self._metrics['IPC'][metric],
                                                                              labels=label)
            elif 'TotalCallTime' in metric:
                if total_calltime_flag:
                    total_calltime_flag = 0
                    name = 'ipc_total_calltime_latency_microseconds'
                    key = 'TotalCallTime'
                    self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                   "The percentile of total calltime latency in microseconds",
                                                                                   labels=label)
                else:
                    continue
            elif 'ResponseSize' in metric:
                if response_size_flag:
                    response_size_flag = 0
                    name = 'ipc_response_size_bytes'
                    key = 'ResponseSize'
                    self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                   "The percentile of response size in bytes",
                                                                                   labels=label)
    
                else:
                    continue
            elif 'ProcessCallTime' in metric:
                if process_calltime_flag:
                    process_calltime_flag = 0
                    name = 'ipc_prcess_calltime_latency_microseconds'
                    key = 'ProcessCallTime'
                    self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                   "The percentile of process calltime latency in microseconds",
                                                                                   labels=label)
                else:
                    continue
            elif 'RequestSize' in metric:
                if request_size_flag:
                    request_size_flag = 0
                    name = 'ipc_request_size_bytes'
                    key = 'RequestSize'
                    self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                   "The percentile of request size in bytes",
                                                                                   labels=label)
    
                else:
                    continue
            elif 'QueueCallTime' in metric:
                if queue_calltime_flag:
                    queue_calltime_flag = 0
                    name = 'ipc_queue_calltime_latency_microseconds'
                    key = 'QueueCallTime'
                    self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                   "The percentile of queue calltime latency in microseconds",
                                                                                   labels=label)
            elif 'exceptions' in metric:
                if exception_flag:
                    exception_flag = 0
                    name = 'ipc_exceptions_total'
                    key = 'exceptions'
                    label.append("type")
                    self._hadoop_hbase_metrics['IPC'][key] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                               "Exceptions caused by requests",
                                                                               labels = label)
                else:
                    continue
            else:
                name = "_".join(['ipc', snake_case])
                self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                              self._metrics['IPC'][metric],
                                                                              labels=label)

    def _setup_filesystem_labels(self):
        hlog_split_time_flag, metahlog_split_time_flag, hlog_split_size_flag, metahlog_split_size_flag = 1, 1, 1, 1
        for metric in self._metrics['FileSystem']:
            label = ["cluster", "host"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = snake_case
                self._hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                     self._metrics['FileSystem'][metric],
                                                                                     labels=label)
            elif 'MetaHlogSplitTime' in metric:
                if metahlog_split_time_flag:
                    metahlog_split_time_flag = 0
                    name = 'metahlog_split_time_latency_microseconds'
                    key = 'MetaHlogSplitTime'
                    self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                          "The percentile of time latency it takes to finish splitMetaLog()",
                                                                                          labels=label)

                else:
                    continue
            elif 'HlogSplitTime' in metric:
                if hlog_split_time_flag:
                    hlog_split_time_flag = 0
                    name = 'hlog_split_time_latency_microseconds'
                    key = 'HlogSplitTime'
                    self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                          "The percentile of time latency it takes to finish WAL.splitLog()",
                                                                                          labels=label)
                else:
                    continue
            elif 'MetaHlogSplitSize' in metric:
                if metahlog_split_size_flag:
                    metahlog_split_size_flag = 0
                    name = 'metahlog_split_size_bytes'
                    key = 'MetaHlogSplitSize'
                    self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                          "The percentile of hbase:meta WAL files size being split",
                                                                                          labels=label)

                else:
                    continue
            elif 'HlogSplitSize' in metric:
                if hlog_split_size_flag:
                    hlog_split_size_flag = 0
                    name = 'hlog_split_size_bytes'
                    key = 'HlogSplitSize'
                    self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self._prefix, name]),
                                                                                          "The percentile of WAL files size being split",
                                                                                          labels=label)
                else:
                    continue                
            else:
                name = snake_case
                self._hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily("_".join([self._prefix, name]),
                                                                                     self._metrics['FileSystem'][metric],
                                                                                     labels=label)

    def _setup_metrics_labels(self, beans):
        # The metrics we want to export.
        for i in range(len(beans)):
            if 'Server' in beans[i]['name']:
                self._setup_server_labels()
            elif 'Balancer' in beans[i]['name']:
                self._setup_balancer_labels()
            elif 'AssignmentManger' in beans[i]['name']:
                self._setup_assignmentmanger_labels()
            elif 'IPC' in beans[i]['name']:
                self._setup_ipc_labels()
            elif 'FileSystem' in beans[i]['name']:
                self._setup_filesystem_labels()
            else:
                continue



    def _get_server_metrics(self, bean):
        host = bean['tag.Hostname']
        label = [self._cluster, host]
        for metric in self._metrics['Server']:
            if 'RegionServersState' in metric:
                if 'tag.liveRegionServers' in bean and bean['tag.liveRegionServers']:
                    live_region_servers = yaml.safe_load(bean['tag.liveRegionServers'])
                    live_region_list = live_region_servers.split(';')
                    for j in range(len(live_region_list)):
                        server = live_region_list[j].split(',')[0]
                        label = [self._cluster, host, server]
                        self._hadoop_hbase_metrics['Server'][metric].add_metric(label, 1.0)
                elif 'tag.deadRegionServers' in bean and bean['tag.deadRegionServers']:
                    dead_region_servers = yaml.safe_load(bean['tag.deadRegionServers'])
                    dead_region_list = dead_region_servers.split(';')
                    for j in range(len(dead_region_list)):
                        server = dead_region_list[j].split(',')[0]
                        label = [self._cluster, host, server]
                        self._hadoop_hbase_metrics['Server'][metric].add_metric(label + [server], 0.0)
                else:
                    pass
            elif 'ActiveMaster' in metric:
                if 'tag.isActiveMaster' in bean:
                    state = bean['tag.isActiveMaster']
                    value = float(bool(state))
                    self._hadoop_hbase_metrics['Server'][metric].add_metric(label, value)
            else:
                self._hadoop_hbase_metrics['Server'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

    def _get_balancer_metrics(self, bean):
        host = bean['tag.Hostname']
        label = [self._cluster, host]

        balancer_sum = 0.0
        balancer_value, balancer_percentile = [], []

        for metric in self._metrics['Balancer']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                self._hadoop_hbase_metrics['Balancer'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            elif 'BalancerCluster' in metric:
                if '_num_ops' in metric:
                    balancer_count = bean[metric]
                    key = 'BalancerCluster'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    balancer_percentile.append(str(float(per) / 100.0))
                    balancer_value.append(bean[metric])
                    balancer_sum += bean[metric]
            else:
                self._hadoop_hbase_metrics['Balancer'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
        balancer_bucket = zip(balancer_percentile, balancer_value)
        balancer_bucket.sort()
        balancer_bucket.append(("+Inf", balancer_count))
        self._hadoop_hbase_metrics['Balancer'][key].add_metric(label, buckets = balancer_bucket, sum_value = balancer_sum)

    def _get_assignmentmanger_metrics(self, bean):
        host = bean['tag.Hostname']
        label = [self._cluster, host]

        bulkassign_sum, assign_sum = 0.0, 0.0
        bulkassign_value, bulkassign_percentile, assign_value, assign_percentile = [], [], [], []

        for metric in self._metrics['AssignmentManger']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                self._hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            elif 'BulkAssign' in metric:
                if '_num_ops' in metric:
                    bulkassign_count = bean[metric]
                    bulkassign_key = 'BulkAssign'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    bulkassign_percentile.append(str(float(per) / 100.0))
                    bulkassign_value.append(bean[metric])
                    bulkassign_sum += bean[metric]
            elif 'Assign' in metric:
                if '_num_ops' in metric:
                    assign_count = bean[metric]
                    assign_key = 'Assign'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    assign_percentile.append(str(float(per) / 100.0))
                    assign_value.append(bean[metric])
                    assign_sum += bean[metric]
            else:
                self._hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
        bulkassign_bucket = zip(bulkassign_percentile, bulkassign_value)
        bulkassign_bucket.sort()
        bulkassign_bucket.append(("+Inf", bulkassign_count))
        assign_bucket = zip(assign_percentile, assign_value)
        assign_bucket.sort()
        assign_bucket.append(("+Inf", assign_count))
        self._hadoop_hbase_metrics['AssignmentManger'][bulkassign_key].add_metric(label, buckets = bulkassign_bucket, sum_value = bulkassign_sum)
        self._hadoop_hbase_metrics['AssignmentManger'][assign_key].add_metric(label, buckets = assign_bucket, sum_value = assign_sum)

    def _get_ipc_metrics(self, bean):
        host = bean['tag.Hostname']
        label = [self._cluster, host]

        total_calltime_sum, process_calltime_sum, queue_calltime_sum, response_sum, request_sum = 0.0, 0.0, 0.0, 0.0, 0.0
        total_calltime_value, process_calltime_value, queue_calltime_value, response_value, request_value = [], [], [], [], []
        total_calltime_percentile, process_calltime_percentile, queue_calltime_percentile, response_percentile, request_percentile = [], [], [], [], []

        for metric in self._metrics['IPC']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                self._hadoop_hbase_metrics['IPC'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            elif 'TotalCallTime' in metric:
                if '_num_ops' in metric:
                    total_calltime_count = bean[metric]
                    total_calltime_key = 'TotalCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    total_calltime_percentile.append(str(float(per) / 100.0))
                    total_calltime_value.append(bean[metric])
                    total_calltime_sum += bean[metric]
            elif 'ProcessCallTime' in metric:
                if '_num_ops' in metric:
                    process_calltime_count = bean[metric]
                    process_calltime_key = 'ProcessCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    process_calltime_percentile.append(str(float(per) / 100.0))
                    process_calltime_value.append(bean[metric])
                    process_calltime_sum += bean[metric]
            elif 'QueueCallTime' in metric:
                if '_num_ops' in metric:
                    queue_calltime_count = bean[metric]
                    queue_calltime_key = 'QueueCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    queue_calltime_percentile.append(str(float(per) / 100.0))
                    queue_calltime_value.append(bean[metric])
                    queue_calltime_sum += bean[metric]
            elif 'ResponseSize' in metric:
                if '_num_ops' in metric:
                    response_count = bean[metric]
                    response_key = 'ResponseSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    response_percentile.append(str(float(per) / 100.0))
                    response_value.append(bean[metric])
                    response_sum += bean[metric]
            elif 'RequestSize' in metric:
                if '_num_ops' in metric:
                    request_count = bean[metric]
                    request_key = 'RequestSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    request_percentile.append(str(float(per) / 100.0))
                    request_value.append(bean[metric])
                    request_sum += bean[metric]
            elif 'exceptions' in metric:
                key = 'exceptions'
                new_label = label
                if 'exceptions' == metric:
                    type = "sum"
                else:
                    type = metric.split(".")[1]
                self._hadoop_hbase_metrics['IPC'][key].add_metric(new_label + [type],
                                                                  bean[metric] if metric in bean and bean[metric] else 0)
            else:
                self._hadoop_hbase_metrics['IPC'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

        total_calltime_bucket = zip(total_calltime_percentile, total_calltime_value)
        total_calltime_bucket.sort()
        total_calltime_bucket.append(("+Inf", total_calltime_count))
        process_calltime_bucket = zip(process_calltime_percentile, process_calltime_value)
        process_calltime_bucket.sort()
        process_calltime_bucket.append(("+Inf", process_calltime_count))
        queue_calltime_bucket = zip(queue_calltime_percentile, queue_calltime_value)
        queue_calltime_bucket.sort()
        queue_calltime_bucket.append(("+Inf", queue_calltime_count))
        response_bucket = zip(response_percentile, response_value)
        response_bucket.sort()
        response_bucket.append(("+Inf", response_count))
        request_bucket = zip(request_percentile, request_value)
        request_bucket.sort()
        request_bucket.append(("+Inf", request_count))
        
        self._hadoop_hbase_metrics['IPC'][total_calltime_key].add_metric(label, buckets = total_calltime_bucket, sum_value = total_calltime_sum)
        self._hadoop_hbase_metrics['IPC'][process_calltime_key].add_metric(label, buckets = process_calltime_bucket, sum_value = process_calltime_sum)
        self._hadoop_hbase_metrics['IPC'][queue_calltime_key].add_metric(label, buckets = queue_calltime_bucket, sum_value = queue_calltime_sum)
        self._hadoop_hbase_metrics['IPC'][response_key].add_metric(label, buckets = response_bucket, sum_value = response_sum)
        self._hadoop_hbase_metrics['IPC'][request_key].add_metric(label, buckets = request_bucket, sum_value = request_sum)

    def _get_filesystem_metrics(self, bean):
        host = bean['tag.Hostname']
        label = [self._cluster, host]

        hlog_split_time_sum, metahlog_split_time_sum, hlog_split_size_sum, metahlog_split_size_sum = 0.0, 0.0, 0.0, 0.0
        hlog_split_time_value, metahlog_split_time_value, hlog_split_size_value, metahlog_split_size_value = [], [], [], []
        hlog_split_time_percentile, metahlog_split_time_percentile, hlog_split_size_percentile, metahlog_split_size_percentile = [], [], [], []

        for metric in self._metrics['FileSystem']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                self._hadoop_hbase_metrics['FileSystem'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)
            elif 'MetaHlogSplitTime' in metric:
                if '_num_ops' in metric:
                    metahlog_split_time_count = bean[metric]
                    metahlog_split_time_key = 'MetaHlogSplitTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    metahlog_split_time_percentile.append(str(float(per) / 100.0))
                    metahlog_split_time_value.append(bean[metric])
                    metahlog_split_time_sum += bean[metric]
            elif 'HlogSplitTime' in metric:
                if '_num_ops' in metric:
                    hlog_split_time_count = bean[metric]
                    hlog_split_time_key = 'HlogSplitTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    hlog_split_time_percentile.append(str(float(per) / 100.0))
                    hlog_split_time_value.append(bean[metric])
                    hlog_split_time_sum += bean[metric]   
            elif 'MetaHlogSplitSize' in metric:
                if '_num_ops' in metric:
                    metahlog_split_size_count = bean[metric]
                    metahlog_split_size_key = 'MetaHlogSplitSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    metahlog_split_size_percentile.append(str(float(per) / 100.0))
                    metahlog_split_size_value.append(bean[metric])
                    metahlog_split_size_sum += bean[metric]                     
            elif 'HlogSplitSize' in metric:
                if '_num_ops' in metric:
                    hlog_split_size_count = bean[metric]
                    hlog_split_size_key = 'HlogSplitSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    hlog_split_size_percentile.append(str(float(per) / 100.0))
                    hlog_split_size_value.append(bean[metric])
                    hlog_split_size_sum += bean[metric]                        
            else:
                self._hadoop_hbase_metrics['FileSystem'][metric].add_metric(label, bean[metric] if metric in bean and bean[metric] else 0)

        hlog_split_time_bucket = zip(hlog_split_time_percentile, hlog_split_time_value)
        hlog_split_time_bucket.sort()
        hlog_split_time_bucket.append(("+Inf", hlog_split_time_count))
        metahlog_split_time_bucket = zip(metahlog_split_time_percentile, metahlog_split_time_value)
        metahlog_split_time_bucket.sort()
        metahlog_split_time_bucket.append(("+Inf", metahlog_split_time_count))
        hlog_split_size_bucket = zip(hlog_split_size_percentile, hlog_split_size_value)
        hlog_split_size_bucket.sort()
        hlog_split_size_bucket.append(("+Inf", hlog_split_size_count))
        metahlog_split_size_bucket = zip(metahlog_split_size_percentile, metahlog_split_size_value)
        metahlog_split_size_bucket.sort()
        metahlog_split_size_bucket.append(("+Inf", metahlog_split_size_count))                    
        
        self._hadoop_hbase_metrics['FileSystem'][hlog_split_time_key].add_metric(label, buckets = hlog_split_time_bucket, sum_value = hlog_split_time_sum)
        self._hadoop_hbase_metrics['FileSystem'][metahlog_split_time_key].add_metric(label, buckets = metahlog_split_time_bucket, sum_value = metahlog_split_time_sum)
        self._hadoop_hbase_metrics['FileSystem'][hlog_split_size_key].add_metric(label, buckets = hlog_split_size_bucket, sum_value = hlog_split_size_sum)
        self._hadoop_hbase_metrics['FileSystem'][metahlog_split_size_key].add_metric(label, buckets = metahlog_split_size_bucket, sum_value = metahlog_split_size_sum)

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):
            if 'Server' in beans[i]['name'] and 'Master' in beans[i]['name']:
                self._get_server_metrics(beans[i])
                    
            elif 'Balancer' in beans[i]['name']:
                self._get_balancer_metrics(beans[i])
                    
            elif 'AssignmentManger' in beans[i]['name']:
                self._get_assignmentmanger_metrics(beans[i])
                    
            elif 'IPC' in beans[i]['name']:
                self._get_ipc_metrics(beans[i])
                    
            elif 'FileSystem' in beans[i]['name']:
                self._get_filesystem_metrics(beans[i])
            else:
                continue

            


def main():
    try:
        args = utils.parse_args()
        port = int(args.port)
        cluster = args.cluster
        v = args.hbase_url
        REGISTRY.register(HBaseMasterMetricCollector(cluster, v))

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