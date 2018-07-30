#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import time
import requests
import argparse
from pprint import pprint

import json
import os
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, SummaryMetricFamily, HistogramMetricFamily, REGISTRY

import utils
from utils import get_module_logger
from consul import Consul

from config import Config

logger = get_module_logger(__name__)


class MetricCol(object):
    '''
    MetricCol is a super class of all kinds of MetricsColleter classes. It setup common params like cluster, url, component and service.
    '''
    def __init__(self, cluster, url, component, service):
        '''
        @param cluster: Cluster name, registered in the config file or ran in the command-line.
        @param url: All metrics are scraped in the url, corresponding to each component. 
                    e.g. "hdfs" metrics can be scraped in http://ip:50070/jmx.
                         "yarn" metrics can be scraped in http://ip:8088/jmx.
        @param component: Component name. e.g. "hdfs", "yarn", "mapreduce", "hive", "hbase".
        @param service: Service name. e.g. "namenode", "resourcemanager", "mapreduce".
        '''
        self._cluster = cluster
        self._url = url.rstrip('/')
        self._component = component
        self._prefix = 'hadoop_{0}_'.format(service)

    def collect(self):
        '''
        This method needs to be override by all subclasses.

        # request_data from url/jmx
        metrics = get_metrics(self._base_url)
        beans = metrics['beans']

        # initial the metircs
        self._setup_metrics_labels()

        # add metrics
        self._get_metrics(beans)
        '''
        pass

    def _setup_metrics_labels(self):
        pass

    def _get_metrics(self, metrics):
        pass

"""
def common_metrics_info(cluster, beans, service):

    tmp_metrics = {}
    common_metrics = {}
    _cluster = cluster
    _prefix = 'hadoop_{0}_'.format(service)
    _metrics_type = utils.get_file_list("common")

    for i in range(len(_metrics_type)):
        common_metrics.setdefault(_metrics_type[i], {})
        tmp_metrics.setdefault(_metrics_type[i], utils.read_json_file("common", _metrics_type[i]))

    def setup_labels():
        '''
        预处理，分析各个模块的特点，进行分类，添加label
        '''
        for metric in tmp_metrics["JvmMetrics"]:
            '''
            处理JvmMetrics模块
            '''
            snake_case = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'Mem' in metric:
                name = snake_case + 'ebibytes'
                label = ["cluster", "mode"]
                if "Used" in metric:
                    key = "jvm_mem_used_mebibytes"
                    descriptions = "Current memory used in mebibytes."
                elif "Committed" in metric:
                    key = "jvm_mem_committed_mebibytes"
                    descriptions = "Current memory used in mebibytes."
                elif "Max" in metric:
                    key = "jvm_mem_max_size_mebibytes"
                    descriptions = "Current memory used in mebibytes."
                else:
                    key = name
                    descriptions = tmp_metrics['JvmMetrics'][metric]
            elif 'Gc' in metric:
                label = ["cluster", "type"]
                if "GcCount" in metric:
                    key = "jvm_gc_count"
                    descriptions = "GC count of each type GC."
                elif "GcTimeMillis" in metric:
                    key = "jvm_gc_time_milliseconds"
                    descriptions = "Each type GC time in msec."
                elif "ThresholdExceeded" in metric:
                    key = "jvm_gc_exceeded_threshold_total"
                    descriptions = "Number of times that the GC threshold is exceeded."
                else:
                    key = snake_case
                    label = ["cluster"]
                    descriptions = tmp_metrics['JvmMetrics'][metric]
            elif 'Threads' in metric:
                label = ["cluster", "state"]
                key = "jvm_threads_state_total"
                descriptions = "Current number of different threads."
            elif 'Log' in metric:
                label = ["cluster", "level"]
                key = "jvm_log_level_total"
                descriptions = "Total number of each level logs."
            else:
                label = ["cluster"]
                key = snake_case
                descriptions = tmp_metrics['JvmMetrics'][metric]
            common_metrics['JvmMetrics'][key] = GaugeMetricFamily(_prefix + key,
                                                                  descriptions,
                                                                  labels=label)

        num_rpc_flag, avg_rpc_flag = 1, 1
        for metric in tmp_metrics["RpcActivity"]:
            '''
            处理RpcActivity, 一个url里可能有多个RpcActivity模块，可以根据tag.port进行区分
            '''
            snake_case = "rpc_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            label = ["cluster", "tag"]
            if "NumOps" in metric:
                if num_rpc_flag:
                    key = "MethodNumOps"
                    label.append("method")
                    common_metrics['RpcActivity'][key] = GaugeMetricFamily(_prefix + "rpc_method_called_total",
                                                                           "Total number of the times the method is called.",
                                                                           labels = label)
                    num_rpc_flag = 0
                else:
                    continue
            elif "AvgTime" in metric:
                if avg_rpc_flag:
                    key = "MethodAvgTime"
                    label.append("method")
                    common_metrics['RpcActivity'][key] = GaugeMetricFamily(_prefix + "rpc_method_avg_time_milliseconds",
                                                                           "Average turn around time of the method in milliseconds.",
                                                                           labels = label)
                    avg_rpc_flag = 0
                else:
                    continue
            else:
                key = metric
                common_metrics['RpcActivity'][key] = GaugeMetricFamily(_prefix + snake_case,
                                                                       tmp_metrics['RpcActivity'][metric],
                                                                       labels = label)
        
        for metric in tmp_metrics['RpcDetailedActivity']:
            label = ["cluster", "tag"]
            if "NumOps" in metric:
                key = "NumOps"
                label.append("method")
                name = _prefix + 'rpc_detailed_method_called_total'
            elif "AvgTime" in metric:
                key = "AvgTime"
                label.append("method")
                name = _prefix  + 'rpc_detailed_method_avg_time_milliseconds'
            else:
                pass
            common_metrics['RpcDetailedActivity'][key] = GaugeMetricFamily(name,
                                                                           tmp_metrics['RpcDetailedActivity'][metric],
                                                                           labels = label)

        ugi_num_flag, ugi_avg_flag = 1, 1
        for metric in tmp_metrics['UgiMetrics']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if ugi_num_flag:
                    key = 'NumOps'
                    label + ["method","state"] if 'Login' in metric else label.append("method")
                    ugi_num_flag = 0
                    common_metrics['UgiMetrics'][key] = GaugeMetricFamily(_prefix + 'ugi_method_called_total',
                                                                          "Total number of the times the method is called.",
                                                                          labels = label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if ugi_avg_flag:
                    key = 'AvgTime'
                    label + ["method", "state"] if 'Login' in metric else label.append("method")
                    ugi_avg_flag = 0
                    common_metrics['UgiMetrics'][key] = GaugeMetricFamily(_prefix + 'ugi_method_avg_time_milliseconds',
                                                                          "Average turn around time of the method in milliseconds.",
                                                                          labels = label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                common_metrics['UgiMetrics'][metric] = GaugeMetricFamily(_prefix  + 'ugi_' + snake_case,
                                                                         tmp_metrics['UgiMetrics'][metric],
                                                                         labels = label)

        metric_num_flag, metric_avg_flag = 1, 1
        for metric in tmp_metrics['MetricsSystem']:
            label = ["cluster"]
            if 'NumOps' in metric:
                if metric_num_flag:
                    key = 'NumOps'
                    label.append("oper")
                    metric_num_flag = 0
                    common_metrics['MetricsSystem'][key] = GaugeMetricFamily(_prefix + 'metrics_operations_total',
                                                                             "Total number of operations",
                                                                             labels = label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if metric_avg_flag:
                    key = 'AvgTime'
                    label.append("oper")
                    metric_avg_flag = 0
                    common_metrics['MetricsSystem'][key] = GaugeMetricFamily(_prefix + 'metrics_method_avg_time_milliseconds',
                                                                             "Average turn around time of the operations in milliseconds.",
                                                                             labels = label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                common_metrics['MetricsSystem'][metric] = GaugeMetricFamily(_prefix  + 'metrics_' + snake_case,
                                                                            tmp_metrics['MetricsSystem'][metric],
                                                                            labels = label)
        return common_metrics

    def get_metrics():
        '''
        给setup_labels模块的输出结果进行赋值，从url中获取对应的数据，挨个赋值
        '''
        common_metrics = setup_labels()
        for i in range(len(beans)):
            if 'JvmMetrics' in beans[i]['name']:
                # 记录JvmMetrics在bean中的位置
                for metric in tmp_metrics['JvmMetrics']:
                    label = [_cluster]
                    name = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()

                    if 'Mem' in metric:
                        if "NonHeap" in metric:
                            label.append("nonheap")
                        elif "MemHeap" in metric:
                            label.append("heap")
                        elif "Max" in metric:
                            label.append("max")
                        else:
                            pass
                        if "Used" in metric:
                            key = "jvm_mem_used_mebibytes"
                        elif "Committed" in metric:
                            key = "jvm_mem_committed_mebibytes"
                        elif "Max" in metric:
                            key = "jvm_mem_max_size_mebibytes"
                        else:
                            key = name + 'ebibytes'
                        
                    elif 'Gc' in metric:
                        if "GcCount" in metric:
                            key = "jvm_gc_count"
                        elif "GcTimeMillis" in metric:
                            key = "jvm_gc_time_milliseconds"
                        elif "ThresholdExceeded" in metric:
                            key = "jvm_gc_exceeded_threshold_total"
                        else:
                            key = name
                        if "ParNew" in metric:
                            label.append("ParNew")
                        elif "ConcurrentMarkSweep" in metric:
                            label.append("ConcurrentMarkSweep")
                        elif "Warn" in metric:
                            label.append("Warn")
                        elif "Info" in metric:
                            label.append("Info")
                        else:
                            pass
                    elif 'Threads' in metric:
                        key = "jvm_threads_state_total"
                        label.append(metric.split("Threads")[1])
                    elif 'Log' in metric:
                        key = "jvm_log_level_total"
                        label.append(metric.split("Log")[1])
                    else:
                        key = name
                    common_metrics['JvmMetrics'][key].add_metric(label,
                                                                 beans[i][metric] if metric in beans[i] else 0)

            if 'RpcActivity' in beans[i]['name']:
                rpc_tag = beans[i]['tag.port']
                for metric in tmp_metrics['RpcActivity']:
                    label = [_cluster, rpc_tag]
                    if "NumOps" in metric:
                        label.append(metric.split('NumOps')[0])
                        key = "MethodNumOps"
                    elif "AvgTime" in metric:
                        label.append(metric.split('AvgTime')[0])
                        key = "MethodAvgTime"
                    else:
                        label.append(metric)
                        key = metric
                    common_metrics['RpcActivity'][key].add_metric(label,
                                                                  beans[i][metric] if metric in beans[i] else 0)
            if 'RpcDetailedActivity' in beans[i]['name']:
                detail_tag = beans[i]['tag.port']
                for metric in beans[i]:
                    label = [_cluster, detail_tag]
                    if "NumOps" in metric:
                        key = "NumOps"
                        label.append(metric.split('NumOps')[0])
                    elif "AvgTime" in metric:
                        key = "AvgTime"
                        label.append(metric.split("AvgTime")[0])
                    else:
                        pass
                    common_metrics['RpcDetailedActivity'][key].add_metric(label,
                                                                          beans[i][metric])

            if 'UgiMetrics' in beans[i]['name']:
                for metric in tmp_metrics['UgiMetrics']:
                    label = [_cluster]
                    if 'NumOps' in metric:
                        key = 'NumOps'
                        if 'Login' in metric:
                            method = 'Login'
                            state = metric.split('Login')[1].split('NumOps')[0]
                            common_metrics['UgiMetrics'][key].add_metric(label + [method, state], beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        else:
                            label.append(metric.split('NumOps')[0])
                            common_metrics['UgiMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    elif 'AvgTime' in metric:
                        key = 'AvgTime'
                        if 'Login' in metric:
                            method = 'Login'
                            state = metric.split('Login')[1].split('AvgTime')[0]
                            common_metrics['UgiMetrics'][key].add_metric(label + [method, state], beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        else:
                            label.append(metric.split('AvgTime')[0])
                            common_metrics['UgiMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    else:
                        common_metrics['UgiMetrics'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

            if 'MetricsSystem' in beans[i]['name'] and "sub=Stats" in beans[i]['name']:
                for metric in tmp_metrics['MetricsSystem']:
                    label = [_cluster]
                    if 'NumOps' in metric:
                        key = 'NumOps'
                        label.append(metric.split('NumOps')[0])
                        common_metrics['MetricsSystem'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    elif 'AvgTime' in metric:
                        key = 'AvgTime'
                        label.append(metric.split('AvgTime')[0])
                        common_metrics['MetricsSystem'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    else:
                        common_metrics['MetricsSystem'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

        return common_metrics

    return get_metrics
"""

"""

class CommonMetricCollector(object):
    '''
    All other collector contains these metrics collected in the CommonMetricCollector,
    including JvmMetrics, MetricSystem, RpcActivity, RpcDetailedActivity, UgiMetrics
    '''
    def __init__(self, cluster, beans, service):
        '''
        @param cluster: cluster name, registered in the config file or ran in the command-line.
        @param url: All metrics are scraped in the url, corresponding to each component. 
                    e.g. "hdfs" metrics can be scraped in http://ip:50070/jmx.
                         "yarn" metrics can be scraped in http://ip:8088/jmx.
        @param component: Component name. e.g. "hdfs", "yarn", "mapreduce", "hive", "hbase".
        @param service: Service name. e.g. "namenode", "resourcemanager", "mapreduce".
        '''
        self.tmp_metrics = {}
        self.common_metrics = {}
        self._cluster = cluster
        self._beans = beans
        self._prefix = 'hadoop_{0}_'.format(service)
        self._metrics_type = utils.get_file_list("common")
    
        for i in range(len(self._metrics_type)):
            self.common_metrics.setdefault(self._metrics_type[i], {})
            self.tmp_metrics.setdefault(self._metrics_type[i], utils.read_json_file("common", self._metrics_type[i]))

        self.setup_labels()
        self.get_metrics()

    def setup_labels(self):
        '''
        预处理，分析各个模块的特点，进行分类，添加label
        '''
        if 'JvmMetrics' in self.tmp_metrics:
            for metric in self.tmp_metrics["JvmMetrics"]:
                '''
                处理JvmMetrics模块
                '''
                snake_case = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if 'Mem' in metric:
                    name = snake_case + 'ebibytes'
                    label = ["cluster", "mode"]
                    if "Used" in metric:
                        key = "jvm_mem_used_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    elif "Committed" in metric:
                        key = "jvm_mem_committed_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    elif "Max" in metric:
                        key = "jvm_mem_max_size_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    else:
                        key = name
                        descriptions = self.tmp_metrics['JvmMetrics'][metric]
                elif 'Gc' in metric:
                    label = ["cluster", "type"]
                    if "GcCount" in metric:
                        key = "jvm_gc_count"
                        descriptions = "GC count of each type GC."
                    elif "GcTimeMillis" in metric:
                        key = "jvm_gc_time_milliseconds"
                        descriptions = "Each type GC time in msec."
                    elif "ThresholdExceeded" in metric:
                        key = "jvm_gc_exceeded_threshold_total"
                        descriptions = "Number of times that the GC threshold is exceeded."
                    else:
                        key = snake_case
                        label = ["cluster"]
                        descriptions = self.tmp_metrics['JvmMetrics'][metric]
                elif 'Threads' in metric:
                    label = ["cluster", "state"]
                    key = "jvm_threads_state_total"
                    descriptions = "Current number of different threads."
                elif 'Log' in metric:
                    label = ["cluster", "level"]
                    key = "jvm_log_level_total"
                    descriptions = "Total number of each level logs."
                else:
                    label = ["cluster"]
                    key = snake_case
                    descriptions = self.tmp_metrics['JvmMetrics'][metric]
                self.common_metrics['JvmMetrics'][key] = GaugeMetricFamily(self._prefix + key,
                                                                           descriptions,
                                                                           labels=label)

        if 'RpcActivity' in self.tmp_metrics:
            num_rpc_flag, avg_rpc_flag = 1, 1
            for metric in self.tmp_metrics["RpcActivity"]:
                '''
                处理RpcActivity, 一个url里可能有多个RpcActivity模块，可以根据tag.port进行区分
                '''
                snake_case = "rpc_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster", "tag"]
                if "NumOps" in metric:
                    if num_rpc_flag:
                        key = "MethodNumOps"
                        label.append("method")
                        self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + "rpc_method_called_total",
                                                                                    "Total number of the times the method is called.",
                                                                                    labels = label)
                        num_rpc_flag = 0
                    else:
                        continue
                elif "AvgTime" in metric:
                    if avg_rpc_flag:
                        key = "MethodAvgTime"
                        label.append("method")
                        self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + "rpc_method_avg_time_milliseconds",
                                                                                    "Average turn around time of the method in milliseconds.",
                                                                                    labels = label)
                        avg_rpc_flag = 0
                    else:
                        continue
                else:
                    key = metric
                    self.common_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + snake_case,
                                                                                self.tmp_metrics['RpcActivity'][metric],
                                                                                labels = label)
        
        if 'RpcDetailedActivity' in self.tmp_metrics:
            for metric in self.tmp_metrics['RpcDetailedActivity']:
                label = ["cluster", "tag"]
                if "NumOps" in metric:
                    key = "NumOps"
                    label.append("method")
                    name = self._prefix + 'rpc_detailed_method_called_total'
                elif "AvgTime" in metric:
                    key = "AvgTime"
                    label.append("method")
                    name = self._prefix  + 'rpc_detailed_method_avg_time_milliseconds'
                else:
                    pass
                self.common_metrics['RpcDetailedActivity'][key] = GaugeMetricFamily(name,
                                                                                    self.tmp_metrics['RpcDetailedActivity'][metric],
                                                                                    labels = label)

        if 'UgiMetrics' in self.tmp_metrics:
            ugi_num_flag, ugi_avg_flag = 1, 1
            for metric in self.tmp_metrics['UgiMetrics']:
                label = ["cluster"]
                if 'NumOps' in metric:
                    if ugi_num_flag:
                        key = 'NumOps'
                        label + ["method","state"] if 'Login' in metric else label.append("method")
                        ugi_num_flag = 0
                        self.common_metrics['UgiMetrics'][key] = GaugeMetricFamily(self._prefix + 'ugi_method_called_total',
                                                                                   "Total number of the times the method is called.",
                                                                                   labels = label)
                    else:
                        continue
                elif 'AvgTime' in metric:
                    if ugi_avg_flag:
                        key = 'AvgTime'
                        label + ["method", "state"] if 'Login' in metric else label.append("method")
                        ugi_avg_flag = 0
                        self.common_metrics['UgiMetrics'][key] = GaugeMetricFamily(self._prefix + 'ugi_method_avg_time_milliseconds',
                                                                                   "Average turn around time of the method in milliseconds.",
                                                                                   labels = label)
                    else:
                        continue
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    self.common_metrics['UgiMetrics'][metric] = GaugeMetricFamily(self._prefix  + 'ugi_' + snake_case,
                                                                                  self.tmp_metrics['UgiMetrics'][metric],
                                                                                  labels = label)

        if 'MetricsSystem' in self.tmp_metrics:
            metric_num_flag, metric_avg_flag = 1, 1
            for metric in self.tmp_metrics['MetricsSystem']:
                label = ["cluster"]
                if 'NumOps' in metric:
                    if metric_num_flag:
                        key = 'NumOps'
                        label.append("oper")
                        metric_num_flag = 0
                        self.common_metrics['MetricsSystem'][key] = GaugeMetricFamily(self._prefix + 'metrics_operations_total',
                                                                                      "Total number of operations",
                                                                                      labels = label)
                    else:
                        continue
                elif 'AvgTime' in metric:
                    if metric_avg_flag:
                        key = 'AvgTime'
                        label.append("oper")
                        metric_avg_flag = 0
                        self.common_metrics['MetricsSystem'][key] = GaugeMetricFamily(self._prefix + 'metrics_method_avg_time_milliseconds',
                                                                                      "Average turn around time of the operations in milliseconds.",
                                                                                      labels = label)
                    else:
                        continue
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    self.common_metrics['MetricsSystem'][metric] = GaugeMetricFamily(self._prefix  + 'metrics_' + snake_case,
                                                                                     self.tmp_metrics['MetricsSystem'][metric],
                                                                                     labels = label)


    def get_metrics(self):
        '''
        给setup_labels模块的输出结果进行赋值，从url中获取对应的数据，挨个赋值
        '''
        for i in range(len(self._beans)):
            if 'JvmMetrics' in self._beans[i]['name']:
                # 记录JvmMetrics在bean中的位置
                for metric in self.tmp_metrics['JvmMetrics']:
                    label = [self._cluster]
                    name = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()

                    if 'Mem' in metric:
                        if "NonHeap" in metric:
                            label.append("nonheap")
                        elif "MemHeap" in metric:
                            label.append("heap")
                        elif "Max" in metric:
                            label.append("max")
                        else:
                            pass
                        if "Used" in metric:
                            key = "jvm_mem_used_mebibytes"
                        elif "Committed" in metric:
                            key = "jvm_mem_committed_mebibytes"
                        elif "Max" in metric:
                            key = "jvm_mem_max_size_mebibytes"
                        else:
                            key = name + 'ebibytes'
                        
                    elif 'Gc' in metric:
                        if "GcCount" in metric:
                            key = "jvm_gc_count"
                        elif "GcTimeMillis" in metric:
                            key = "jvm_gc_time_milliseconds"
                        elif "ThresholdExceeded" in metric:
                            key = "jvm_gc_exceeded_threshold_total"
                        else:
                            key = name
                        if "ParNew" in metric:
                            label.append("ParNew")
                        elif "ConcurrentMarkSweep" in metric:
                            label.append("ConcurrentMarkSweep")
                        elif "Warn" in metric:
                            label.append("Warn")
                        elif "Info" in metric:
                            label.append("Info")
                        else:
                            pass
                    elif 'Threads' in metric:
                        key = "jvm_threads_state_total"
                        label.append(metric.split("Threads")[1])
                    elif 'Log' in metric:
                        key = "jvm_log_level_total"
                        label.append(metric.split("Log")[1])
                    else:
                        key = name
                    self.common_metrics['JvmMetrics'][key].add_metric(label,
                                                                      self._beans[i][metric] if metric in self._beans[i] else 0)

            if 'RpcActivity' in self._beans[i]['name']:
                rpc_tag = self._beans[i]['tag.port']
                for metric in self.tmp_metrics['RpcActivity']:
                    label = [self._cluster, rpc_tag]
                    if "NumOps" in metric:
                        label.append(metric.split('NumOps')[0])
                        key = "MethodNumOps"
                    elif "AvgTime" in metric:
                        label.append(metric.split('AvgTime')[0])
                        key = "MethodAvgTime"
                    else:
                        label.append(metric)
                        key = metric
                    self.common_metrics['RpcActivity'][key].add_metric(label,
                                                                       self._beans[i][metric] if metric in self._beans[i] else 0)
            if 'RpcDetailedActivity' in self._beans[i]['name']:
                detail_tag = self._beans[i]['tag.port']
                for metric in self._beans[i]:
                    label = [self._cluster, detail_tag]
                    if "NumOps" in metric:
                        key = "NumOps"
                        label.append(metric.split('NumOps')[0])
                    elif "AvgTime" in metric:
                        key = "AvgTime"
                        label.append(metric.split("AvgTime")[0])
                    else:
                        pass
                    self.common_metrics['RpcDetailedActivity'][key].add_metric(label,
                                                                               self._beans[i][metric])

            if 'UgiMetrics' in self._beans[i]['name']:
                for metric in self.tmp_metrics['UgiMetrics']:
                    label = [self._cluster]
                    if 'NumOps' in metric:
                        key = 'NumOps'
                        if 'Login' in metric:
                            method = 'Login'
                            state = metric.split('Login')[1].split('NumOps')[0]
                            self.common_metrics['UgiMetrics'][key].add_metric(label + [method, state], self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        else:
                            label.append(metric.split('NumOps')[0])
                            self.common_metrics['UgiMetrics'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                    elif 'AvgTime' in metric:
                        key = 'AvgTime'
                        if 'Login' in metric:
                            method = 'Login'
                            state = metric.split('Login')[1].split('AvgTime')[0]
                            self.common_metrics['UgiMetrics'][key].add_metric(label + [method, state], self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        else:
                            label.append(metric.split('AvgTime')[0])
                            self.common_metrics['UgiMetrics'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                    else:
                        self.common_metrics['UgiMetrics'][metric].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)

            if 'MetricsSystem' in self._beans[i]['name'] and "sub=Stats" in self._beans[i]['name']:
                for metric in self.tmp_metrics['MetricsSystem']:
                    label = [self._cluster]
                    if 'NumOps' in metric:
                        key = 'NumOps'
                        label.append(metric.split('NumOps')[0])
                        self.common_metrics['MetricsSystem'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                    elif 'AvgTime' in metric:
                        key = 'AvgTime'
                        label.append(metric.split('AvgTime')[0])
                        self.common_metrics['MetricsSystem'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                    else:
                        self.common_metrics['MetricsSystem'][metric].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)

"""

class NameNodeMetricsCollector(MetricCol):

    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().HDFS_ACTIVE_URL, "HDFS", "namenode")
        # self._url = "{0}?qry=Hadoop:service=NameNode,name=*".format(self._base_url)
        self._file_list = utils.get_file_list("namenode")
        self._common_file = utils.get_file_list("common")
        self._merge_list = self._file_list + self._common_file
        self._metrics_value = utils.get_metrics(self._url)
        if self._metrics_value and 'beans' in self._metrics_value:
            self._beans = self._metrics_value['beans']
        else:
            self._beans = []

        self._namenode_metrics = {}
        self._common_metrics = {}
        self._hadoop_namenode_metrics = {}

        for i in range(len(self._common_file)):
            self._common_metrics.setdefault(self._common_file[i], utils.read_json_file("common", self._common_file[i])) 

        for i in range(len(self._file_list)):
            self._namenode_metrics.setdefault(self._file_list[i], utils.read_json_file("namenode", self._file_list[i]))
        
        self._metrics = self._common_metrics.copy()
        self._metrics.update(self._namenode_metrics)

        for i in range(len(self._merge_list)):
            self._hadoop_namenode_metrics.setdefault(self._merge_list[i], {})

        pprint(self._metrics)
        pprint(self._hadoop_namenode_metrics)

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'


        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # # update namenode metrics with common metrics
        # common = CommonMetricCollector(self._cluster, self._beans, "namenode")
        # pprint(common.common_metrics)
        # self._hadoop_namenode_metrics.update(common.common_metrics)

        # pprint(self._hadoop_namenode_metrics)
        # print "=================================="

        # # yield all metrics
        # pprint(self._merge_list)
        # self._merge_list = self._file_list
        for i in range(len(self._common_file)):
            service = self._common_file[i]
            for metric in self._hadoop_namenode_metrics[service]:
                yield self._hadoop_namenode_metrics[service][metric]
        # for i in range(len(self._file_list)):
        #     service = self._file_list[i]
        #     for metric in self._hadoop_namenode_metrics[service]:
        #         yield self._hadoop_namenode_metrics[service][metric]

        # for i in range(len(self._merge_list)):
        #     service = self._merge_list[i]
        #     for metric in self._hadoop_namenode_metrics[service]:
        #         yield self._hadoop_namenode_metrics[service][metric]


    def _setup_metrics_labels(self):
        # The metrics we want to export.

        if 'NameNodeActivity' in self._metrics:
            num_namenode_flag,avg_namenode_flag,ops_namenode_flag = 1,1,1
            for metric in self._metrics['NameNodeActivity']:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster", "method"]
                if "NumOps" in metric:
                    if num_namenode_flag:
                        key = "MethodNumOps"
                        self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(self._prefix + "nnactivity_method_ops_total",
                                                                                                   "Total number of the times the method is called.",
                                                                                                   labels=label)
                        num_namenode_flag = 0
                    else:
                        continue
                elif "AvgTime" in metric:
                    if avg_namenode_flag:
                        key = "MethodAvgTime"
                        self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(self._prefix + "nnactivity_method_avg_time_milliseconds",
                                                                                                   "Average turn around time of the method in milliseconds.",
                                                                                                   labels=label)
                        avg_namenode_flag = 0
                    else:
                        continue
                else:
                    if ops_namenode_flag:
                        key = "Operations"
                        self._hadoop_namenode_metrics['NameNodeActivity'][key] = GaugeMetricFamily(self._prefix  + "nnactivity_operations_total",
                                                                                                   "Total number of each operation.",
                                                                                                   labels=label)
                        ops_namenode_flag = 0

        if 'StartupProgress' in self._metrics:            
            sp_count_flag,sp_elapsed_flag,sp_total_flag,sp_complete_flag = 1,1,1,1
            for metric in self._metrics['StartupProgress']:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster"]
                if "ElapsedTime" == metric:
                    key = "ElapsedTime"
                    name = "total_elapsed_time_milliseconds"
                    descriptions = "Total elapsed time in milliseconds."
                    self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                              descriptions,
                                                                                              labels = label)
                elif "PercentComplete" == metric:
                    key = "PercentComplete"
                    name = "complete_rate"
                    descriptions = "Current rate completed in NameNode startup progress  (The max value is not 100 but 1.0)."
                    self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                              descriptions,
                                                                                              labels = label)
                 
                elif "Count" in metric:                    
                    if sp_count_flag:
                        key = "PhaseCount"
                        name = "phase_count"
                        label.append("phase")
                        descriptions = "Total number of steps completed in the phase."
                        self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                                  descriptions,
                                                                                                  labels = label)
                    sp_count_flag = 0

                elif "ElapsedTime" in metric:
                    if sp_elapsed_flag:
                        key = "PhaseElapsedTime"
                        name = "phase_elapsed_time_milliseconds"
                        label.append("phase")
                        descriptions = "Total elapsed time in the phase in milliseconds."
                        self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                                  descriptions,
                                                                                                  labels = label)
                    sp_elapsed_flag = 0

                elif "Total" in metric:
                    if sp_total_flag:
                        key = "PhaseTotal"
                        name = "phase_total"
                        label.append("phase")
                        descriptions = "Total number of steps in the phase."
                        self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                                  descriptions,
                                                                                                  labels = label)
                    sp_total_flag = 0

                elif "PercentComplete" in metric:
                    if sp_complete_flag:
                        key = "PhasePercentComplete"
                        name = "phase_complete_rate"
                        label.append("phase")
                        descriptions = "Current rate completed in the phase  (The max value is not 100 but 1.0)."
                        self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                                  descriptions,
                                                                                                  labels = label)
                    sp_complete_flag = 0
                else:
                    key = metric
                    name = snake_case
                    descriptions = self._metrics['StartupProgress'][metric]
                    self._hadoop_namenode_metrics['StartupProgress'][key] = GaugeMetricFamily(self._prefix + "startup_process_" + name,
                                                                                              descriptions,
                                                                                              labels = label)

        if 'FSNamesystem' in self._metrics:
            cap_flag = 1
            for metric in self._metrics['FSNamesystem']:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster"]
                if metric.startswith('Capacity'):
                    if cap_flag:
                        key = "capacity"
                        label.append("mode")
                        descriptions = "Current DataNodes capacity in each mode in bytes"
                        self._hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily(self._prefix + "fsname_system_capacity_bytes",
                                                                                               descriptions,
                                                                                               labels = label)
                        cap_flag = 0
                    else:
                        continue

                else:
                    key = metric
                    descriptions = self._metrics['FSNamesystem'][metric]
                    self._hadoop_namenode_metrics['FSNamesystem'][key] = GaugeMetricFamily(self._prefix + "fsname_system_" + snake_case,
                                                                                           descriptions,
                                                                                           labels = label)

        if 'FSNamesystemState' in self._metrics:
            num_flag = 1
            for metric in self._metrics['FSNamesystemState']:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster"]
                if 'DataNodes' in metric:
                    if num_flag:
                        key = "datanodes_num"
                        label.append("state")
                        descriptions = "Number of datanodes in each state"
                        self._hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily(self._prefix + "fsname_system_datanodes_count",
                                                                                                    descriptions,
                                                                                                    labels = label)
                        num_flag = 0
                    else:
                        continue
                else:
                    key = metric
                    descriptions = self._metrics['FSNamesystemState'][metric]
                    self._hadoop_namenode_metrics['FSNamesystemState'][key] = GaugeMetricFamily(self._prefix + "fsname_system_" + snake_case,
                                                                                                descriptions,
                                                                                                labels = label)

        if 'RetryCache' in self._metrics:
            cache_flag = 1
            for metric in self._metrics['RetryCache']:
                if cache_flag:
                    key = "cache"
                    label.append("mode")
                    self._hadoop_namenode_metrics['RetryCache'][key] = GaugeMetricFamily(self._prefix + "cache_total", 
                                                                                         "Total number of RetryCache in each mode", 
                                                                                         labels = label)
                    cache_flag = 0
                else:
                    continue

        if 'JvmMetrics' in self._metrics:
            for metric in self._metrics["JvmMetrics"]:
                '''
                处理JvmMetrics模块
                '''
                snake_case = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if 'Mem' in metric:
                    name = snake_case + 'ebibytes'
                    label = ["cluster", "mode"]
                    if "Used" in metric:
                        key = "jvm_mem_used_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    elif "Committed" in metric:
                        key = "jvm_mem_committed_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    elif "Max" in metric:
                        key = "jvm_mem_max_size_mebibytes"
                        descriptions = "Current memory used in mebibytes."
                    else:
                        key = name
                        descriptions = self._metrics['JvmMetrics'][metric]
                elif 'Gc' in metric:
                    label = ["cluster", "type"]
                    if "GcCount" in metric:
                        key = "jvm_gc_count"
                        descriptions = "GC count of each type GC."
                    elif "GcTimeMillis" in metric:
                        key = "jvm_gc_time_milliseconds"
                        descriptions = "Each type GC time in msec."
                    elif "ThresholdExceeded" in metric:
                        key = "jvm_gc_exceeded_threshold_total"
                        descriptions = "Number of times that the GC threshold is exceeded."
                    else:
                        key = snake_case
                        label = ["cluster"]
                        descriptions = self._metrics['JvmMetrics'][metric]
                elif 'Threads' in metric:
                    label = ["cluster", "state"]
                    key = "jvm_threads_state_total"
                    descriptions = "Current number of different threads."
                elif 'Log' in metric:
                    label = ["cluster", "level"]
                    key = "jvm_log_level_total"
                    descriptions = "Total number of each level logs."
                else:
                    label = ["cluster"]
                    key = snake_case
                    descriptions = self._metrics['JvmMetrics'][metric]
                self._hadoop_namenode_metrics['JvmMetrics'][key] = GaugeMetricFamily(self._prefix + key,
                                                                                     descriptions,
                                                                                     labels=label)

        if 'RpcActivity' in self._metrics:
            num_rpc_flag, avg_rpc_flag = 1, 1
            for metric in self._metrics["RpcActivity"]:
                '''
                处理RpcActivity, 一个url里可能有多个RpcActivity模块，可以根据tag.port进行区分
                '''
                snake_case = "rpc_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster", "tag"]
                if "NumOps" in metric:
                    if num_rpc_flag:
                        key = "MethodNumOps"
                        label.append("method")
                        self._hadoop_namenode_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + "rpc_method_called_total",
                                                                                              "Total number of the times the method is called.",
                                                                                              labels = label)
                        num_rpc_flag = 0
                    else:
                        continue
                elif "AvgTime" in metric:
                    if avg_rpc_flag:
                        key = "MethodAvgTime"
                        label.append("method")
                        self._hadoop_namenode_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + "rpc_method_avg_time_milliseconds",
                                                                                              "Average turn around time of the method in milliseconds.",
                                                                                              labels = label)
                        avg_rpc_flag = 0
                    else:
                        continue
                else:
                    key = metric
                    self._hadoop_namenode_metrics['RpcActivity'][key] = GaugeMetricFamily(self._prefix + snake_case,
                                                                                          self._metrics['RpcActivity'][metric],
                                                                                          labels = label)
        
        if 'RpcDetailedActivity' in self._metrics:
            for metric in self._metrics['RpcDetailedActivity']:
                snake_case = "rpc_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster", "tag"]
                if "NumOps" in metric:
                    key = "NumOps"
                    label.append("method")
                    name = self._prefix + 'rpc_detailed_method_called_total'
                elif "AvgTime" in metric:
                    key = "AvgTime"
                    label.append("method")
                    name = self._prefix  + 'rpc_detailed_method_avg_time_milliseconds'
                else:
                    key = metric
                    name = self._prefix + snake_case
                self._hadoop_namenode_metrics['RpcDetailedActivity'][key] = GaugeMetricFamily(name,
                                                                                              self._metrics['RpcDetailedActivity'][metric],
                                                                                              labels = label)

        if 'UgiMetrics' in self._metrics:
            ugi_num_flag, ugi_avg_flag = 1, 1
            for metric in self._metrics['UgiMetrics']:
                label = ["cluster"]
                if 'NumOps' in metric:
                    if ugi_num_flag:
                        key = 'NumOps'
                        label + ["method","state"] if 'Login' in metric else label.append("method")
                        ugi_num_flag = 0
                        self._hadoop_namenode_metrics['UgiMetrics'][key] = GaugeMetricFamily(self._prefix + 'ugi_method_called_total',
                                                                                   "Total number of the times the method is called.",
                                                                                   labels = label)
                    else:
                        continue
                elif 'AvgTime' in metric:
                    if ugi_avg_flag:
                        key = 'AvgTime'
                        label + ["method", "state"] if 'Login' in metric else label.append("method")
                        ugi_avg_flag = 0
                        self._hadoop_namenode_metrics['UgiMetrics'][key] = GaugeMetricFamily(self._prefix + 'ugi_method_avg_time_milliseconds',
                                                                                   "Average turn around time of the method in milliseconds.",
                                                                                   labels = label)
                    else:
                        continue
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    self._hadoop_namenode_metrics['UgiMetrics'][metric] = GaugeMetricFamily(self._prefix  + 'ugi_' + snake_case,
                                                                                  self._metrics['UgiMetrics'][metric],
                                                                                  labels = label)

        if 'MetricsSystem' in self._metrics:
            metric_num_flag, metric_avg_flag = 1, 1
            for metric in self._metrics['MetricsSystem']:
                label = ["cluster"]
                if 'NumOps' in metric:
                    if metric_num_flag:
                        key = 'NumOps'
                        label.append("oper")
                        metric_num_flag = 0
                        self._hadoop_namenode_metrics['MetricsSystem'][key] = GaugeMetricFamily(self._prefix + 'metrics_operations_total',
                                                                                      "Total number of operations",
                                                                                      labels = label)
                    else:
                        continue
                elif 'AvgTime' in metric:
                    if metric_avg_flag:
                        key = 'AvgTime'
                        label.append("oper")
                        metric_avg_flag = 0
                        self._hadoop_namenode_metrics['MetricsSystem'][key] = GaugeMetricFamily(self._prefix + 'metrics_method_avg_time_milliseconds',
                                                                                      "Average turn around time of the operations in milliseconds.",
                                                                                      labels = label)
                    else:
                        continue
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    self._hadoop_namenode_metrics['MetricsSystem'][metric] = GaugeMetricFamily(self._prefix  + 'metrics_' + snake_case,
                                                                                     self._metrics['MetricsSystem'][metric],
                                                                                     labels = label)



    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):

            if 'NameNodeActivity' in beans[i]['name']:
                if 'NameNodeActivity' in self._metrics:
                    for metric in self._metrics['NameNodeActivity']:
                        label = [self._cluster]
                        if "NumOps" in metric:
                            label.append(metric.split('NumOps')[0])
                            key = "MethodNumOps"
                        elif "AvgTime" in metric:
                            label.append(metric.split('AvgTime')[0])
                            key = "MethodAvgTime"
                        else:
                            if "Ops" in metric:
                                label.append(metric.split('Ops')[0])
                            else:
                                label.append(metric)
                            key = "Operations"
                        self._hadoop_namenode_metrics['NameNodeActivity'][key].add_metric(label,
                                                                                          beans[i][metric] if metric in beans[i] else 0)

            if 'StartupProgress' in beans[i]['name']:
                if 'StartupProgress' in self._metrics:
                    for metric in self._metrics['StartupProgress']:
                        label = [self._cluster]
                        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                        if "Count" in metric:
                            key = "PhaseCount"
                            label.append(metric.split("Count")[0])
                        elif "ElapsedTime" in metric and "ElapsedTime" != metric:
                            key = "PhaseElapsedTime"
                            label.append(metric.split("ElapsedTime")[0])
                        elif "Total" in metric:
                            key = "PhaseTotal"
                            label.append(metric.split("Total")[0])
                        elif "PercentComplete" in metric and "PercentComplete" != metric:
                            key = "PhasePercentComplete"
                            label.append(metric.split("PercentComplete")[0])
                        else:
                            key = metric
                        self._hadoop_namenode_metrics['StartupProgress'][key].add_metric(label,
                                                                                         beans[i][metric] if metric in beans[i] else 0)

            if 'FSNamesystem' in beans[i]['name'] and 'FSNamesystemState' not in beans[i]['name']:
                if 'FSNamesystem' in self._metrics:
                    for metric in self._metrics['FSNamesystem']:
                        label = [self._cluster]
                        key = metric
                        if 'HAState' in metric:
                            if 'initializing' == beans[i]['tag.HAState']:
                                value = 0.0
                            elif 'active' == beans[i]['tag.HAState']:
                                value = 1.0
                            elif 'standby' == beans[i]['tag.HAState']:
                                value = 2.0
                            elif 'stopping' == beans[i]['tag.HAState']:
                                value = 3.0
                            else:
                                value = 9999
                            self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, value)
                        elif metric.startswith("Capacity"):
                            key = 'capacity'
                            label.append(metric.split("Capacity")[1])
                            self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)
                        else:
                            self._hadoop_namenode_metrics['FSNamesystem'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)

            if 'FSNamesystemState' in beans[i]['name']:
                if 'FSNamesystemState' in self._metrics:
                    for metric in self._metrics['FSNamesystemState']:
                        label = [self._cluster]
                        key = metric
                        if 'FSState' in metric:
                            if 'Safemode' == beans[i]['FSState']:
                                value = 0.0
                            elif 'Operational' == beans[i]['FSState']:
                                value = 1.0
                            else:
                                value = 9999
                            self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, value)
                        elif "TotalSyncTimes" in metric:
                            self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, float(re.sub('\s', '', beans[i][metric])) if metric in beans[i] and beans[i][metric] else 0)
                        elif "DataNodes" in metric:
                            key = 'datanodes_num'
                            label.append(metric.split("DataNodes")[0].split("Num")[1])
                            self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

                        else:
                            self._hadoop_namenode_metrics['FSNamesystemState'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

            if 'RetryCache' in beans[i]['name']:
                if 'RetryCache' in self._metrics:
                    for metric in self._metrics['RetryCache']:
                        key = "cache"
                        label = [self._cluster, metric.split('Cache')[1]]
                        self._hadoop_namenode_metrics['RetryCache'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)           


            if 'JvmMetrics' in self._beans[i]['name']:
                # 记录JvmMetrics在bean中的位置
                if 'JvmMetrics' in self._metrics:
                    for metric in self._metrics['JvmMetrics']:
                        label = [self._cluster]
                        name = "jvm_" + re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()

                        if 'Mem' in metric:
                            if "NonHeap" in metric:
                                label.append("nonheap")
                            elif "MemHeap" in metric:
                                label.append("heap")
                            elif "Max" in metric:
                                label.append("max")
                            else:
                                pass
                            if "Used" in metric:
                                key = "jvm_mem_used_mebibytes"
                            elif "Committed" in metric:
                                key = "jvm_mem_committed_mebibytes"
                            elif "Max" in metric:
                                key = "jvm_mem_max_size_mebibytes"
                            else:
                                key = name + 'ebibytes'

                        elif 'Gc' in metric:
                            if "GcCount" in metric:
                                key = "jvm_gc_count"
                            elif "GcTimeMillis" in metric:
                                key = "jvm_gc_time_milliseconds"
                            elif "ThresholdExceeded" in metric:
                                key = "jvm_gc_exceeded_threshold_total"
                            else:
                                key = name
                            if "ParNew" in metric:
                                label.append("ParNew")
                            elif "ConcurrentMarkSweep" in metric:
                                label.append("ConcurrentMarkSweep")
                            elif "Warn" in metric:
                                label.append("Warn")
                            elif "Info" in metric:
                                label.append("Info")
                            else:
                                pass
                        elif 'Threads' in metric:
                            key = "jvm_threads_state_total"
                            label.append(metric.split("Threads")[1])
                        elif 'Log' in metric:
                            key = "jvm_log_level_total"
                            label.append(metric.split("Log")[1])
                        else:
                            key = name
                        self._hadoop_namenode_metrics['JvmMetrics'][key].add_metric(label,
                                                                          self._beans[i][metric] if metric in self._beans[i] else 0)

            if 'RpcActivity' in self._beans[i]['name']:
                if 'RpcActivity' in self._metrics:
                    rpc_tag = self._beans[i]['tag.port']
                    for metric in self._metrics['RpcActivity']:
                        label = [self._cluster, rpc_tag]
                        if "NumOps" in metric:
                            label.append(metric.split('NumOps')[0])
                            key = "MethodNumOps"
                        elif "AvgTime" in metric:
                            label.append(metric.split('AvgTime')[0])
                            key = "MethodAvgTime"
                        else:
                            label.append(metric)
                            key = metric
                        self._hadoop_namenode_metrics['RpcActivity'][key].add_metric(label,
                                                                           self._beans[i][metric] if metric in self._beans[i] else 0)
            if 'RpcDetailedActivity' in self._beans[i]['name']:
                if 'RpcDetailedActivity' in self._metrics:
                    detail_tag = self._beans[i]['tag.port']
                    # tag.port = '8020'
                    for metric in self._beans[i]:
                        if metric[0].isupper():
                            label = [self._cluster, detail_tag]
                            if "NumOps" in metric:
                                key = "NumOps"
                                label.append(metric.split('NumOps')[0])
                            elif "AvgTime" in metric:
                                key = "AvgTime"
                                label.append(metric.split("AvgTime")[0])
                            else:
                                pass                                
                            self._hadoop_namenode_metrics['RpcDetailedActivity'][key].add_metric(label,
                                                                                       self._beans[i][metric])
                        else:
                            pass

            if 'UgiMetrics' in self._beans[i]['name']:
                if 'UgiMetrics' in self._metrics:
                    for metric in self._metrics['UgiMetrics']:
                        label = [self._cluster]
                        if 'NumOps' in metric:
                            key = 'NumOps'
                            if 'Login' in metric:
                                method = 'Login'
                                state = metric.split('Login')[1].split('NumOps')[0]
                                self._hadoop_namenode_metrics['UgiMetrics'][key].add_metric(label + [method, state], self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                            else:
                                label.append(metric.split('NumOps')[0])
                                self._hadoop_namenode_metrics['UgiMetrics'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        elif 'AvgTime' in metric:
                            key = 'AvgTime'
                            if 'Login' in metric:
                                method = 'Login'
                                state = metric.split('Login')[1].split('AvgTime')[0]
                                self._hadoop_namenode_metrics['UgiMetrics'][key].add_metric(label + [method, state], self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                            else:
                                label.append(metric.split('AvgTime')[0])
                                self._hadoop_namenode_metrics['UgiMetrics'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        else:
                            self._hadoop_namenode_metrics['UgiMetrics'][metric].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)

            if 'MetricsSystem' in self._beans[i]['name'] and "sub=Stats" in self._beans[i]['name']:
                if 'MetricsSystem' in self._metrics:
                    for metric in self._metrics['MetricsSystem']:
                        label = [self._cluster]
                        if 'NumOps' in metric:
                            key = 'NumOps'
                            label.append(metric.split('NumOps')[0])
                            self._hadoop_namenode_metrics['MetricsSystem'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        elif 'AvgTime' in metric:
                            key = 'AvgTime'
                            label.append(metric.split('AvgTime')[0])
                            self._hadoop_namenode_metrics['MetricsSystem'][key].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)
                        else:
                            self._hadoop_namenode_metrics['MetricsSystem'][metric].add_metric(label, self._beans[i][metric] if metric in self._beans[i] and self._beans[i][metric] else 0)


class HBaseMetricsCollector(MetricCol):

    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().HBASE_URL, "hbase")
        self._url = "http://10.110.13.43:16010/jmx?qry=Hadoop:service=HBase,name=Master,sub=IPC"
        self._file_list = utils.get_file_list("hbase")

        self._metrics = {}
        self._hadoop_hbase_metrics = {}

        for i in range(len(self._file_list)):
            self._metrics.setdefault(self._file_list[i], utils.read_json_file("hbase", self._file_list[i]))
            self._hadoop_hbase_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'
        metrics = utils.get_metrics(self._url)
        beans = metrics['beans']

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(beans)

        # yield all metrics
        for i in range(len(self._file_list)):
            service = self._file_list[i]
            for metric in self._hadoop_hbase_metrics[service]:
                yield self._hadoop_hbase_metrics[service][metric]

    def _setup_metrics_labels(self):
        if 'IPC' in self._metrics:
            ipc_flag = 1
            for metric in self._metrics['IPC']:
                if 'TotalCallTime' in metric:
                    if ipc_flag:
                        key = 'TotalCallTime'
                        ipc_flag = 0
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily('hadoop_hbase_call_time_total',
                                                                                     'Total call time counts in each quantile')
                    else:
                        continue
                else:
                    pass

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):
            if 'IPC' in beans[i]['name']:
                if 'IPC' in self._metrics:
                    count_value = 0
                    sum_value = 0
                    for metric in self._metrics['IPC']:
                        key = 'TotalCallTime'
                        if 'TotalCallTime' in metric and 'percentile' in metric:
                            count_value += 1
                            sum_value += beans[i][metric]
                        self._hadoop_hbase_metrics['IPC'][key].add_metric([], count_value, sum_value)



def main():
    try:
        args = utils.parse_args()
        port = int(args.port)

        REGISTRY.register(NameNodeMetricsCollector(args.cluster))
        # REGISTRY.register(HBaseMetricsCollector(args.cluster,Config().HDFS_ACTIVE_URL))

        c = Consul(host='10.110.13.216')
        # Register Service
        # address = '192.168.0.106'
        # address = '10.9.11.95'
        c.agent.service.register('hadoop_python_test2323',
                                 service_id='consul_python_test2323',
                                 address='10.9.11.95',
                                 port=port,
                                 tags=['hadoop'])
        start_http_server(port)
        print("Polling %s. Serving at port: %s" % (args.address, port))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        c.agent.service.deregister(service_id='consul_python_test2323')
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()