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

from config.config import Config

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
                         "resourcemanager" metrics can be scraped in http://ip:8088/jmx.
        @param component: Component name. e.g. "hdfs", "resourcemanager", "mapreduce", "hive", "hbase".
        @param service: Service name. e.g. "namenode", "resourcemanager", "mapreduce".
        '''
        self._cluster = cluster
        self._url = url.rstrip('/')
        self._component = component
        self._prefix = 'hadoop_{0}_'.format(service)

        self._file_list = utils.get_file_list(service)
        self._common_file = utils.get_file_list("common")
        self._merge_list = self._file_list + self._common_file
        self._metrics_value = utils.get_metrics(self._url)
        if self._metrics_value and 'beans' in self._metrics_value:
            self._beans = self._metrics_value['beans']
        else:
            self._beans = []

        self._metrics = {}
        for i in range(len(self._file_list)):
            self._metrics.setdefault(self._file_list[i], utils.read_json_file(service, self._file_list[i]))

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

        for metric in tmp_metrics['OperatingSystem']:
            label = ["cluster"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            common_metrics['OperatingSystem'][metric] = GaugeMetricFamily(_prefix + snake_case, 
                                                                          tmp_metrics['OperatingSystem'][metric],
                                                                          labels=label)


        num_rpc_flag, avg_rpc_flag = 1, 1
        for metric in tmp_metrics["RpcActivity"]:
            '''
            处理RpcActivity, 一个url里可能有多个RpcActivity模块，可以根据tag.port进行区分
            '''
            if 'Rpc' in metric:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            else:
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
                    label.extend(["method","state"]) if 'Login' in metric else label.append("method")
                    ugi_num_flag = 0
                    common_metrics['UgiMetrics'][key] = GaugeMetricFamily(_prefix + 'ugi_method_called_total',
                                                                          "Total number of the times the method is called.",
                                                                          labels = label)
                else:
                    continue
            elif 'AvgTime' in metric:
                if ugi_avg_flag:
                    key = 'AvgTime'
                    label.extend(["method", "state"]) if 'Login' in metric else label.append("method")
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

        for metric in tmp_metrics['Runtime']:
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            common_metrics['Runtime'][metric] = GaugeMetricFamily(_prefix + snake_case + "_seconds", 
                                                                  tmp_metrics['Runtime'][metric], 
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

            if 'OperatingSystem' in beans[i]['name']:
                for metric in tmp_metrics['OperatingSystem']:
                    label = [_cluster]
                    common_metrics['OperatingSystem'][metric].add_metric(label,
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
                    if metric[0].isupper():
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
                            label.extend([method, state])
                            common_metrics['UgiMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        else:
                            method = metric.split('NumOps')[0]
                            label.append(method)
                            common_metrics['UgiMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    elif 'AvgTime' in metric:
                        key = 'AvgTime'
                        if 'Login' in metric:
                            method = 'Login'
                            state = metric.split('Login')[1].split('AvgTime')[0]
                            label.extend([method, state])
                            common_metrics['UgiMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        else:
                            method = metric.split('AvgTime')[0]
                            label.append(method)
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


            if 'Runtime' in beans[i]['name']:
                for metric in tmp_metrics['Runtime']:
                    label = [_cluster]
                    common_metrics['Runtime'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)


        return common_metrics

    return get_metrics


class NameNodeMetricsCollector(MetricCol):

    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().HDFS_ACTIVE_URL, "HDFS", "namenode")
        self._hadoop_namenode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_namenode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "namenode")
        self._hadoop_namenode_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_namenode_metrics[service]:
                yield self._hadoop_namenode_metrics[service][metric]


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

class ResourceManagerMetricsCollector(MetricCol):

    NODE_STATE = {
        'NEW': 1, 
        'RUNNING': 2, 
        'UNHEALTHY': 3, 
        'DECOMMISSIONED': 4, 
        'LOST': 5, 
        'REBOOTED': 6,
    }
    
    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().YARN_ACTIVE_URL, "YARN", "resourcemanager")
        self._hadoop_resourcemanager_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_resourcemanager_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "resourcemanager")
        self._hadoop_resourcemanager_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_resourcemanager_metrics[service]:
                yield self._hadoop_resourcemanager_metrics[service][metric]

    def _setup_metrics_labels(self):
        # The metrics we want to export.

        if 'RMNMInfo' in self._metrics:
            for metric in self._metrics['RMNMInfo']:
                label = ["cluster", "host", "version", "rack"]
                if 'NumContainers' in metric:
                    name = self._prefix + 'node_containers_total'
                elif 'State' in metric:
                    name = self._prefix + 'node_state'
                elif 'UsedMemoryMB' in metric:
                    name = self._prefix + 'node_memory_used'
                elif 'AvailableMemoryMB' in metric:
                    name = self._prefix + 'node_memory_available'
                # elif 'UsedVirtualCores' in metric:
                #     name = self._prefix + 'node_virtual_cores_used'
                # elif 'AvailableVirtualCores' in metric:
                #     name = self._prefix + 'node_virtual_cores_available'
                else:
                    pass
                self._hadoop_resourcemanager_metrics['RMNMInfo'][metric] = GaugeMetricFamily(name,
                                                                                             self._metrics['RMNMInfo'][metric],
                                                                                             labels=label)

        if 'QueueMetrics' in self._metrics:
            running_flag = 1
            for metric in self._metrics['QueueMetrics']:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                label = ["cluster"]
                if "running_" in metric:
                    if running_flag:
                        running_flag = 0
                        label.append("elapsed_time")
                        key = "running_app"
                        name = "running_app_total"
                        descriptions = "Current number of running applications in each elapsed time ( < 60min, 60min < x < 300min, 300min < x < 1440min and x > 1440min )"
                        self._hadoop_resourcemanager_metrics['QueueMetrics'][key] = GaugeMetricFamily(self._prefix + name,
                                                                                                      descriptions,
                                                                                                      labels=label)
                    else:
                        continue
                else:
                    self._hadoop_resourcemanager_metrics['QueueMetrics'][metric] = GaugeMetricFamily(self._prefix + snake_case,
                                                                                                     self._metrics['QueueMetrics'][metric],
                                                                                                     labels=label)


        if 'ClusterMetrics' in self._metrics:
            nm_flag, cm_num_flag, cm_avg_flag = 1,1,1
            for metric in self._metrics['ClusterMetrics']:
                label = ["cluster"]
                if "NMs" in metric:
                    if nm_flag:
                        nm_flag = 0
                        label.append("status")
                        key = "NMs"
                        name = "nodemanager_total"
                        descriptions = "Current number of NodeManagers in each status"
                    else:
                        continue
                elif "NumOps" in metric:
                    if cm_num_flag:
                        cm_num_flag = 0
                        label.append("oper")
                        key = "NumOps"
                        name = "ams_total"
                        descriptions = "Total number of Applications Masters in each operation"
                    else:
                        continue
                elif "AvgTime" in metric:
                    if cm_avg_flag:
                        cm_avg_flag = 0
                        label.append("oper")
                        key = "AvgTime"
                        name = "average_time_milliseconds"
                        descriptions = "Average time in milliseconds AM spends in each operation"
                    else:
                        continue
                else:
                    continue
                self._hadoop_resourcemanager_metrics['ClusterMetrics'][key] = GaugeMetricFamily(self._prefix + name,
                                                                                                 descriptions,
                                                                                                 labels=label)

                

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):

            if 'RMNMInfo' in beans[i]['name']:
                if 'RMNMInfo' in self._metrics:
                    for metric in self._metrics['RMNMInfo']:
                        live_nm_list = yaml.safe_load(beans[i]['LiveNodeManagers'])
                        for j in range(len(live_nm_list)):
                            host = live_nm_list[j]['HostName']
                            version = live_nm_list[j]['NodeManagerVersion']
                            rack = live_nm_list[j]['Rack']
                            label = [self._cluster, host, version, rack]
                            if 'State' == metric:
                                value = self.NODE_STATE[live_nm_list[j]['State']]
                            else:
                                value = live_nm_list[j][metric]
                            self._hadoop_resourcemanager_metrics['RMNMInfo'][metric].add_metric(label, value)

            if 'QueueMetrics' in beans[i]['name'] and 'root' == beans[i]['tag.Queue']:
                if 'QueueMetrics' in self._metrics:
                    for metric in self._metrics['QueueMetrics']:
                        label = [self._cluster]
                        snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                        if "running_0" in metric:
                            key = "running_app"
                            label.append("0to60")
                        elif "running_60" in metric:
                            key = "running_app"
                            label.append("60to300")
                        elif "running_300" in metric:
                            key = "running_app"
                            label.append("300to1440")
                        elif "running_1440" in metric:
                            key = "running_app"
                            label.append("1440up")
                        else:
                            key = metric
                        self._hadoop_resourcemanager_metrics['QueueMetrics'][key].add_metric(label,
                                                                                             beans[i][metric] if metric in beans[i] else 0)

            if 'ClusterMetrics' in beans[i]['name']:
                if 'ClusterMetrics' in self._metrics:
                    for metric in self._metrics['ClusterMetrics']:
                        label = [self._cluster]
                        if "NMs" in metric:
                            label.append(metric.split('NMs')[0].split('Num')[1])
                            key = "NMs"
                            # self._hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)
                        elif "NumOps" in metric:
                            key = "NumOps"
                            label.append(metric.split("DelayNumOps")[0].split('AM')[1])
                            # self._hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)
                        elif "AvgTime" in metric:
                            key = "AvgTime"
                            label.append(metric.split("DelayAvgTime")[0].split('AM')[1])
                            # self._hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)
                        else:
                            continue
                        self._hadoop_resourcemanager_metrics['ClusterMetrics'][key].add_metric(label, beans[i][metric] if metric in beans[i] else 0)


class MapReduceMetricsCollector(MetricCol):

    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().MAPREDUCE2_URL, "MAPREDUCE", "jobhistoryserver")
        self._hadoop_jobhistoryserver_metrics = {}
        # for i in range(len(self._file_list)):
        #     self._hadoop_jobhistoryserver_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        # self._setup_metrics_labels()

        # add metric value to every metric.
        # self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "jobhistoryserver")
        self._hadoop_jobhistoryserver_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_jobhistoryserver_metrics[service]:
                yield self._hadoop_jobhistoryserver_metrics[service][metric]


class DataNodeMetricCollector(MetricCol):
    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().DATA_NODE1_URL, "DATANODE", "datanode")
        self._hadoop_datanode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_datanode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "datanode")
        self._hadoop_datanode_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_datanode_metrics[service]:
                yield self._hadoop_datanode_metrics[service][metric]

    def _setup_metrics_labels(self):
        # The metrics we want to export.

        if 'DataNodeInfo' in self._metrics:
            for metric in self._metrics['DataNodeInfo']:
                label = ["cluster", "version"]
                if 'ActorState' in metric:
                    label.append("host")
                    name = self._prefix + 'actor_state'
                elif 'VolumeInfo' in metric:
                    label.extend(["path", "state"])
                    name = self._prefix + 'volume_state'

                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    name = self._prefix + snake_case
                self._hadoop_datanode_metrics['DataNodeInfo'][metric] = GaugeMetricFamily(name,
                                                                                          self._metrics['DataNodeInfo'][metric],
                                                                                          labels=label)
        
        if 'DataNodeActivity' in self._metrics:
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
                self._hadoop_datanode_metrics['DataNodeActivity'][key] = GaugeMetricFamily(self._prefix + name,
                                                                                           descriptions,
                                                                                           labels=label)
                    
        if 'DataNodeVolume' in self._metrics:
            iorate_num_flag, iorate_avg_flag = 1, 1
            for metric in self._metrics['DataNodeVolume']:
                label = ['cluster', 'host']
                if 'IoRateNumOps' in metric:
                    if iorate_num_flag:
                        label.append('oper')
                        iorate_num_flag = 0
                        key = "IoRateNumOps"
                        name = "file_io_operations_total"
                        descriptions = "The number of each file io operations within an interval time of metric"
                    else:
                        continue
                elif 'IoRateAvgTime' in metric:
                    if iorate_avg_flag:
                        label.append('oper')
                        iorate_avg_flag = 0
                        key = "IoRateAvgTime"
                        name = "file_io_operations_milliseconds"
                        descriptions = "Mean time of each file io operations in milliseconds"
                    else:
                        continue
                else:
                    key = metric
                    descriptions = self._metrics['DataNodeVolume'][metric]
                    if 'NumOps' in metric:
                        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("NumOps")[0]).lower() + "_total"
                    elif 'AvgTime' in metric:
                        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("AvgTime")[0]).lower() + "_time_milliseconds"
                    else:
                        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self._hadoop_datanode_metrics['DataNodeVolume'][key] = GaugeMetricFamily(self._prefix + name, 
                                                                                         descriptions,
                                                                                         labels = label)

        if 'FSDatasetState' in self._metrics:
            for metric in self._metrics['FSDatasetState']:
                label = ['cluster', 'host']
                if "Num" in metric:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric.split("Num")[1]).lower()
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self._hadoop_datanode_metrics['FSDatasetState'][metric] = GaugeMetricFamily(self._prefix + snake_case,
                                                                                            self._metrics['FSDatasetState'][metric],
                                                                                            labels = label)

                

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):

            if 'DataNodeInfo' in beans[i]['name']:
                if 'DataNodeInfo' in self._metrics:
                    for metric in self._metrics['DataNodeInfo']:
                        version = beans[i]['Version']
                        if 'BPServiceActorInfo' in beans[i]:
                            actor_info_list = yaml.safe_load(beans[i]['BPServiceActorInfo'])
                            for j in range(len(actor_info_list)):
                                host = actor_info_list[j]['NamenodeAddress'].split(':')[0]
                                label = [self._cluster, version, host]
                                if 'state' == "RUNNING":
                                    value = 1.0
                                else:
                                    value = 0.0
                                # self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)
                        elif 'VolumeInfo' in beans[i]:
                            volume_info_dict = yaml.safe_load(beans[i]['VolumeInfo'])
                            for k, v in volume_info_dict:
                                path = k
                                for key, value in v:
                                    state = key
                                    label = [self._cluster, version, path, state]
                                    # self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)
                        else:
                            label = [self._cluster, version]
                            value = beans[i][metric]
                        self._hadoop_datanode_metrics['DataNodeInfo'][metric].add_metric(label, value)

            if 'DataNodeActivity' in beans[i]['name']:
                if 'DataNodeActivity' in self._metrics:
                    for metric in self._metrics['DataNodeActivity']:
                        label = [self._cluster]
                        host = beans[i]['tag.Hostname']
                        label.append(host)
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
                                                                                          beans[i][metric] if metric in beans[i] else 0)

            if 'DataNodeVolume' in beans[i]['name']:
                if 'DataNodeVolume' in self._metrics:
                    for metric in self._metrics['DataNodeVolume']:
                        label = [self._cluster]
                        host = beans[i]['tag.Hostname']
                        label.append(host)
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
                                                                                        beans[i][metric] if metric in beans[i] else 0)

            if 'FSDatasetState' in beans[i]['name'] and 'FSDatasetState' in beans[i]['modelerType']:
                if 'FSDatasetState' in self._metrics:
                    for metric in self._metrics['FSDatasetState']:
                        label = [self._cluster]
                        host = beans[i]['tag.Hostname']
                        label.append(host)
                        self._hadoop_datanode_metrics['FSDatasetState'][metric].add_metric(label, beans[i][metric] if metric in beans[i] else 0)


class JournalNodeMetricCollector(MetricCol):
    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().JOURNAL_NODE1_URL, "JOURNALNODE", "journalnode")
        self._hadoop_journalnode_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_journalnode_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "journalnode")
        self._hadoop_journalnode_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_journalnode_metrics[service]:
                yield self._hadoop_journalnode_metrics[service][metric]

    def _setup_metrics_labels(self):
        # The metrics we want to export.

        if 'Journal-prod' in self._metrics:
            prod_num_flag, a_60_latency_flag, a_300_latency_flag, a_3600_latency_flag = 1, 1, 1, 1
            for metric in self._metrics['Journal-prod']:
                label = ["cluster", "host"]
                if 'Syncs60s' in metric:
                    if a_60_latency_flag:
                        a_60_latency_flag = 0
                        key = "Syncs60"
                        name = self._prefix + 'sync60s_latency_microseconds'
                        descriptions = "The percentile of sync latency in microseconds in 60s granularity"
                        self._hadoop_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                      descriptions,
                                                                                                      labels=label)
                    else:
                        continue
                elif 'Syncs300s' in metric:
                    if a_300_latency_flag:
                        a_300_latency_flag = 0
                        key = "Syncs300"
                        name = self._prefix + 'sync300s_latency_microseconds'
                        descriptions = "The percentile of sync latency in microseconds in 300s granularity"
                        self._hadoop_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                      descriptions,
                                                                                                      labels=label)
                    else:
                        continue
                elif 'Syncs3600s' in metric:
                    if a_3600_latency_flag:
                        a_3600_latency_flag = 0
                        key = "Syncs3600"
                        name = self._prefix + 'sync3600s_latency_microseconds'
                        descriptions = "The percentile of sync latency in microseconds in 3600s granularity"
                        self._hadoop_journalnode_metrics['Journal-prod'][key] = HistogramMetricFamily(name,
                                                                                                      descriptions,
                                                                                                      labels=label)
                    else:
                        continue
                else:
                    snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                    self._hadoop_journalnode_metrics['Journal-prod'][metric] = GaugeMetricFamily(self._prefix + snake_case,
                                                                                                 self._metrics['Journal-prod'][metric],
                                                                                                 labels=label)

        

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):
            if 'Journal-prod' in beans[i]['name']:
                if 'Journal-prod' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    a_60_sum, a_300_sum, a_3600_sum = 0.0, 0.0, 0.0
                    a_60_value, a_300_value, a_3600_value = [], [], []
                    a_60_percentile, a_300_percentile, a_3600_percentile = [], [], []

                    for metric in beans[i]:
                        if metric[0].isupper():
                            '''
                            different sync times corresponding to the same percentile
                            for instance:
                                sync = 60, percentile can be [50, 75, 95, 99]
                                sync = 300, percentile still can be [50, 75, 95, 99]
                            Therefore, here is the method to distinguish these metrics from each sync times.
                            '''
                            if "Syncs60s" in metric:                                
                                if 'NumOps' in metric:
                                    a_60_count = beans[i][metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                                    a_60_percentile.append(str(float(tmp[1]) / 100.0))
                                    a_60_value.append(beans[i][metric])
                                    a_60_sum += beans[i][metric]
                            elif 'Syncs300' in metric:
                                if 'NumOps' in metric:
                                    a_300_count = beans[i][metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                                    a_300_percentile.append(str(float(tmp[1]) / 100.0))
                                    a_300_value.append(beans[i][metric])
                                    a_300_sum += beans[i][metric]
                            elif 'Syncs3600' in metric:
                                if 'NumOps' in metric:
                                    a_3600_count = beans[i][metric]
                                else:
                                    tmp = metric.split("thPercentileLatencyMicros")[0].split("Syncs")[1].split("s")
                                    a_3600_percentile.append(str(float(tmp[1]) / 100.0))
                                    a_3600_value.append(beans[i][metric])
                                    a_3600_sum += beans[i][metric]
                            else:
                                key = metric
                                self._hadoop_journalnode_metrics['Journal-prod'][key].add_metric(label, beans[i][metric])
                    a_60_bucket = zip(a_60_percentile, a_60_value)
                    a_300_bucket = zip(a_300_percentile, a_300_value)
                    a_3600_bucket = zip(a_3600_percentile, a_3600_value)
                    a_60_bucket.sort()
                    a_300_bucket.sort()
                    a_3600_bucket.sort()
                    a_60_bucket.append(("+Inf", a_60_count))
                    a_300_bucket.append(("+Inf", a_300_count))
                    a_3600_bucket.append(("+Inf", a_3600_count))
                    self._hadoop_journalnode_metrics['Journal-prod']['Syncs60'].add_metric(label, buckets = a_60_bucket, sum_value = a_60_sum)
                    self._hadoop_journalnode_metrics['Journal-prod']['Syncs300'].add_metric(label, buckets = a_300_bucket, sum_value = a_300_sum)
                    self._hadoop_journalnode_metrics['Journal-prod']['Syncs3600'].add_metric(label, buckets = a_3600_bucket, sum_value = a_3600_sum)


class HBaseMetricCollector(MetricCol):
    def __init__(self, cluster):
        MetricCol.__init__(self, cluster, Config().HBASE_ACTIVE_URL, "HBASE", "hbase")
        self._hadoop_hbase_metrics = {}
        for i in range(len(self._file_list)):
            self._hadoop_hbase_metrics.setdefault(self._file_list[i], {})

    def collect(self):
        # Request data from ambari Collect Host API
        # Request exactly the System level information we need from node
        # beans returns a type of 'List'

        # set up all metrics with labels and descriptions.
        self._setup_metrics_labels()

        # add metric value to every metric.
        self._get_metrics(self._beans)

        # update namenode metrics with common metrics
        common_metrics = common_metrics_info(self._cluster, self._beans, "hbase")
        self._hadoop_hbase_metrics.update(common_metrics())

        for i in range(len(self._merge_list)):
            service = self._merge_list[i]
            for metric in self._hadoop_hbase_metrics[service]:
                yield self._hadoop_hbase_metrics[service][metric]

    def _setup_metrics_labels(self):
        # The metrics we want to export.

        if 'Server' in self._metrics:
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
                self._hadoop_hbase_metrics['Server'][metric] = GaugeMetricFamily(self._prefix + 'server_' + name,
                                                                                 self._metrics['Server'][metric],
                                                                                 labels=label)
        if 'Balancer' in self._metrics:
            balancer_flag = 1
            for metric in self._metrics['Balancer']:
                label = ["cluster", "host"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                    name = snake_case
                    self._hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                       self._metrics['Balancer'][metric],
                                                                                       labels=label)
                elif 'BalancerCluster' in metric:
                    if balancer_flag:
                        balancer_flag = 0
                        name = 'balancer_cluster_latency_microseconds'
                        key = 'BalancerCluster'
                        self._hadoop_hbase_metrics['Balancer'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                               "The percentile of balancer cluster latency in microseconds",
                                                                                               labels=label)

                    else:
                        continue
                else:
                    name = 'balancer' + snake_case
                    self._hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                       self._metrics['Balancer'][metric],
                                                                                       labels=label)
        if 'AssignmentManger' in self._metrics:
            bulkassign_flag, assign_flag = 1, 1
            for metric in self._metrics['AssignmentManger']:
                label = ["cluster", "host"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                    name = snake_case
                    self._hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                               self._metrics['AssignmentManger'][metric],
                                                                                               labels=label)
                elif 'BulkAssign' in metric:
                    if bulkassign_flag:
                        bulkassign_flag = 0
                        name = 'bulkassign_latency_microseconds'
                        key = 'BulkAssign'
                        self._hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                                    "The percentile of bulkassign latency in microseconds",
                                                                                                    labels=label)

                    else:
                        continue
                elif 'Assign' in metric:
                    if assign_flag:
                        assign_flag = 0
                        name = 'assign_latency_microseconds'
                        key = 'Assign'
                        self._hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                                    "The percentile of assign latency in microseconds",
                                                                                                    labels=label)

                    else:
                        continue
                else:
                    name = 'assignmentmanger_' + snake_case
                    self._hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                               self._metrics['AssignmentManger'][metric],
                                                                                               labels=label)
        if 'IPC' in self._metrics:
            total_calltime_flag, response_size_flag, process_calltime_flag, queue_calltime_flag, request_size_flag, exception_flag = 1, 1, 1, 1, 1, 1
            for metric in self._metrics['IPC']:
                label = ["cluster", "host"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                    name = 'ipc_' + snake_case
                    self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                  self._metrics['IPC'][metric],
                                                                                  labels=label)
                elif 'RangeCount_' in metric:
                    name = metric.replace("-", "_").lower()
                    self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily(self._prefix + 'ipc' + name,
                                                                                  self._metrics['IPC'][metric],
                                                                                  labels=label)
                elif 'TotalCallTime' in metric:
                    if total_calltime_flag:
                        total_calltime_flag = 0
                        name = 'ipc_total_calltime_latency_microseconds'
                        key = 'TotalCallTime'
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                       "The percentile of total calltime latency in microseconds",
                                                                                       labels=label)
                    else:
                        continue
                elif 'ResponseSize' in metric:
                    if response_size_flag:
                        response_size_flag = 0
                        name = 'ipc_response_size_bytes'
                        key = 'ResponseSize'
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                       "The percentile of response size in bytes",
                                                                                       labels=label)

                    else:
                        continue
                elif 'ProcessCallTime' in metric:
                    if process_calltime_flag:
                        process_calltime_flag = 0
                        name = 'ipc_prcess_calltime_latency_microseconds'
                        key = 'ProcessCallTime'
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                       "The percentile of process calltime latency in microseconds",
                                                                                       labels=label)
                    else:
                        continue
                elif 'RequestSize' in metric:
                    if request_size_flag:
                        request_size_flag = 0
                        name = 'ipc_request_size_bytes'
                        key = 'RequestSize'
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                       "The percentile of request size in bytes",
                                                                                       labels=label)

                    else:
                        continue
                elif 'QueueCallTime' in metric:
                    if queue_calltime_flag:
                        queue_calltime_flag = 0
                        name = 'ipc_queue_calltime_latency_microseconds'
                        key = 'QueueCallTime'
                        self._hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                       "The percentile of queue calltime latency in microseconds",
                                                                                       labels=label)
                elif 'exceptions' in metric:
                    if exception_flag:
                        exception_flag = 0
                        name = 'ipc_exceptions_total'
                        key = 'exceptions'
                        label.append("type")
                        self._hadoop_hbase_metrics['IPC'][key] = GaugeMetricFamily(self._prefix + name, 
                                                                                   "Exceptions caused by requests",
                                                                                   labels = label)
                    else:
                        continue
                else:
                    name = 'ipc_' + snake_case
                    self._hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                  self._metrics['IPC'][metric],
                                                                                  labels=label)
        if 'FileSystem' in self._metrics:
            hlog_split_time_flag, metahlog_split_time_flag, hlog_split_size_flag, metahlog_split_size_flag = 1, 1, 1, 1
            for metric in self._metrics['FileSystem']:
                label = ["cluster", "host"]
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                    name = snake_case
                    self._hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                         self._metrics['FileSystem'][metric],
                                                                                         labels=label)
                elif 'MetaHlogSplitTime' in metric:
                    if metahlog_split_time_flag:
                        metahlog_split_time_flag = 0
                        name = 'metahlog_split_time_latency_microseconds'
                        key = 'MetaHlogSplitTime'
                        self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                              "The percentile of time latency it takes to finish splitMetaLog()",
                                                                                              labels=label)

                    else:
                        continue
                elif 'HlogSplitTime' in metric:
                    if hlog_split_time_flag:
                        hlog_split_time_flag = 0
                        name = 'hlog_split_time_latency_microseconds'
                        key = 'HlogSplitTime'
                        self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                              "The percentile of time latency it takes to finish WAL.splitLog()",
                                                                                              labels=label)
                    else:
                        continue
                elif 'MetaHlogSplitSize' in metric:
                    if metahlog_split_size_flag:
                        metahlog_split_size_flag = 0
                        name = 'metahlog_split_size_bytes'
                        key = 'MetaHlogSplitSize'
                        self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                              "The percentile of hbase:meta WAL files size being split",
                                                                                              labels=label)

                    else:
                        continue
                elif 'HlogSplitSize' in metric:
                    if hlog_split_size_flag:
                        hlog_split_size_flag = 0
                        name = 'hlog_split_size_bytes'
                        key = 'HlogSplitSize'
                        self._hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily(self._prefix + name,
                                                                                              "The percentile of WAL files size being split",
                                                                                              labels=label)
                    else:
                        continue                
                else:
                    name = snake_case
                    self._hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily(self._prefix + name,
                                                                                         self._metrics['FileSystem'][metric],
                                                                                         labels=label)

    def _get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>

        for i in range(len(beans)):
            if 'Server' in beans[i]['name'] and 'Master' in beans[i]['name']:
                if 'Server' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    for metric in self._metrics['Server']:
                        if 'RegionServersState' in metric:
                            if 'tag.liveRegionServers' in beans[i] and beans[i]['tag.liveRegionServers']:
                                live_region_servers = yaml.safe_load(beans[i]['tag.liveRegionServers'])
                                live_region_list = live_region_servers.split(';')
                                for j in range(len(live_region_list)):
                                    live_label = label
                                    server = live_region_list[j].split(',')[0]
                                    self._hadoop_hbase_metrics['Server'][metric].add_metric(live_label + [server], 1.0)
                            else:
                                pass
                            if 'tag.deadRegionServers' in beans[i] and beans[i]['tag.deadRegionServers']:
                                dead_region_servers = yaml.safe_load(beans[i]['tag.deadRegionServers'])
                                dead_region_list = dead_region_servers.split(';')
                                for j in range(len(dead_region_list)):
                                    dead_label = label
                                    server = dead_region_list[j].split(',')[0]
                                    self._hadoop_hbase_metrics['Server'][metric].add_metric(dead_label + [server], 0.0)
                            else:
                                pass
                        elif 'ActiveMaster' in metric:
                            if 'tag.isActiveMaster' in beans[i]:
                                state = beans[i]['tag.isActiveMaster']
                                value = float(bool(state))
                                self._hadoop_hbase_metrics['Server'][metric].add_metric(label, value)
                        else:
                            self._hadoop_hbase_metrics['Server'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

            if 'Balancer' in beans[i]['name']:
                if 'Balancer' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    balancer_sum = 0.0
                    balancer_value, balancer_percentile = [], []

                    for metric in self._metrics['Balancer']:
                        if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                            self._hadoop_hbase_metrics['Balancer'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        elif 'BalancerCluster' in metric:
                            if '_num_ops' in metric:
                                balancer_count = beans[i][metric]
                                key = 'BalancerCluster'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                balancer_percentile.append(str(float(per) / 100.0))
                                balancer_value.append(beans[i][metric])
                                balancer_sum += beans[i][metric]
                        else:
                            self._hadoop_hbase_metrics['Balancer'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    balancer_bucket = zip(balancer_percentile, balancer_value)
                    balancer_bucket.sort()
                    balancer_bucket.append(("+Inf", balancer_count))
                    self._hadoop_hbase_metrics['Balancer'][key].add_metric(label, buckets = balancer_bucket, sum_value = balancer_sum)

            if 'AssignmentManger' in beans[i]['name']:
                if 'AssignmentManger' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    bulkassign_sum, assign_sum = 0.0, 0.0
                    bulkassign_value, bulkassign_percentile, assign_value, assign_percentile = [], [], [], []

                    for metric in self._metrics['AssignmentManger']:
                        if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                            self._hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        elif 'BulkAssign' in metric:
                            if '_num_ops' in metric:
                                bulkassign_count = beans[i][metric]
                                bulkassign_key = 'BulkAssign'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                bulkassign_percentile.append(str(float(per) / 100.0))
                                bulkassign_value.append(beans[i][metric])
                                bulkassign_sum += beans[i][metric]
                        elif 'Assign' in metric:
                            if '_num_ops' in metric:
                                assign_count = beans[i][metric]
                                assign_key = 'Assign'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                assign_percentile.append(str(float(per) / 100.0))
                                assign_value.append(beans[i][metric])
                                assign_sum += beans[i][metric]
                        else:
                            self._hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                    bulkassign_bucket = zip(bulkassign_percentile, bulkassign_value)
                    bulkassign_bucket.sort()
                    bulkassign_bucket.append(("+Inf", bulkassign_count))
                    assign_bucket = zip(assign_percentile, assign_value)
                    assign_bucket.sort()
                    assign_bucket.append(("+Inf", assign_count))
                    self._hadoop_hbase_metrics['AssignmentManger'][bulkassign_key].add_metric(label, buckets = bulkassign_bucket, sum_value = bulkassign_sum)
                    self._hadoop_hbase_metrics['AssignmentManger'][assign_key].add_metric(label, buckets = assign_bucket, sum_value = assign_sum)

            if 'IPC' in beans[i]['name']:
                if 'IPC' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    total_calltime_sum, process_calltime_sum, queue_calltime_sum, response_sum, request_sum = 0.0, 0.0, 0.0, 0.0, 0.0
                    total_calltime_value, process_calltime_value, queue_calltime_value, response_value, request_value = [], [], [], [], []
                    total_calltime_percentile, process_calltime_percentile, queue_calltime_percentile, response_percentile, request_percentile = [], [], [], [], []

                    for metric in self._metrics['IPC']:
                        if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                            self._hadoop_hbase_metrics['IPC'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        elif 'TotalCallTime' in metric:
                            if '_num_ops' in metric:
                                total_calltime_count = beans[i][metric]
                                total_calltime_key = 'TotalCallTime'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                total_calltime_percentile.append(str(float(per) / 100.0))
                                total_calltime_value.append(beans[i][metric])
                                total_calltime_sum += beans[i][metric]
                        elif 'ProcessCallTime' in metric:
                            if '_num_ops' in metric:
                                process_calltime_count = beans[i][metric]
                                process_calltime_key = 'ProcessCallTime'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                process_calltime_percentile.append(str(float(per) / 100.0))
                                process_calltime_value.append(beans[i][metric])
                                process_calltime_sum += beans[i][metric]
                        elif 'QueueCallTime' in metric:
                            if '_num_ops' in metric:
                                queue_calltime_count = beans[i][metric]
                                queue_calltime_key = 'QueueCallTime'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                queue_calltime_percentile.append(str(float(per) / 100.0))
                                queue_calltime_value.append(beans[i][metric])
                                queue_calltime_sum += beans[i][metric]
                        elif 'ResponseSize' in metric:
                            if '_num_ops' in metric:
                                response_count = beans[i][metric]
                                response_key = 'ResponseSize'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                response_percentile.append(str(float(per) / 100.0))
                                response_value.append(beans[i][metric])
                                response_sum += beans[i][metric]
                        elif 'RequestSize' in metric:
                            if '_num_ops' in metric:
                                request_count = beans[i][metric]
                                request_key = 'RequestSize'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                request_percentile.append(str(float(per) / 100.0))
                                request_value.append(beans[i][metric])
                                request_sum += beans[i][metric]
                        elif 'exceptions' in metric:
                            key = 'exceptions'
                            new_label = label
                            if 'exceptions' == metric:
                                type = "sum"
                            else:
                                type = metric.split(".")[1]
                            self._hadoop_hbase_metrics['IPC'][key].add_metric(new_label + [type],
                                                                              beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        else:
                            self._hadoop_hbase_metrics['IPC'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

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

            if 'FileSystem' in beans[i]['name']:
                if 'FileSystem' in self._metrics:
                    label = [self._cluster]
                    host = beans[i]['tag.Hostname']
                    label.append(host)

                    hlog_split_time_sum, metahlog_split_time_sum, hlog_split_size_sum, metahlog_split_size_sum = 0.0, 0.0, 0.0, 0.0
                    hlog_split_time_value, metahlog_split_time_value, hlog_split_size_value, metahlog_split_size_value = [], [], [], []
                    hlog_split_time_percentile, metahlog_split_time_percentile, hlog_split_size_percentile, metahlog_split_size_percentile = [], [], [], []

                    for metric in self._metrics['FileSystem']:
                        if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                            self._hadoop_hbase_metrics['FileSystem'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)
                        elif 'MetaHlogSplitTime' in metric:
                            if '_num_ops' in metric:
                                metahlog_split_time_count = beans[i][metric]
                                metahlog_split_time_key = 'MetaHlogSplitTime'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                metahlog_split_time_percentile.append(str(float(per) / 100.0))
                                metahlog_split_time_value.append(beans[i][metric])
                                metahlog_split_time_sum += beans[i][metric]
                        elif 'HlogSplitTime' in metric:
                            if '_num_ops' in metric:
                                hlog_split_time_count = beans[i][metric]
                                hlog_split_time_key = 'HlogSplitTime'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                hlog_split_time_percentile.append(str(float(per) / 100.0))
                                hlog_split_time_value.append(beans[i][metric])
                                hlog_split_time_sum += beans[i][metric]   
                        elif 'MetaHlogSplitSize' in metric:
                            if '_num_ops' in metric:
                                metahlog_split_size_count = beans[i][metric]
                                metahlog_split_size_key = 'MetaHlogSplitSize'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                metahlog_split_size_percentile.append(str(float(per) / 100.0))
                                metahlog_split_size_value.append(beans[i][metric])
                                metahlog_split_size_sum += beans[i][metric]                     
                        elif 'HlogSplitSize' in metric:
                            if '_num_ops' in metric:
                                hlog_split_size_count = beans[i][metric]
                                hlog_split_size_key = 'HlogSplitSize'
                            else:
                                per = metric.split("_")[1].split("th")[0]
                                hlog_split_size_percentile.append(str(float(per) / 100.0))
                                hlog_split_size_value.append(beans[i][metric])
                                hlog_split_size_sum += beans[i][metric]                        
                        else:
                            self._hadoop_hbase_metrics['FileSystem'][metric].add_metric(label, beans[i][metric] if metric in beans[i] and beans[i][metric] else 0)

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

def main():
    try:
        args = utils.parse_args()
        port = int(args.port)

        REGISTRY.register(NameNodeMetricsCollector(args.cluster))
        REGISTRY.register(ResourceManagerMetricsCollector(args.cluster))
        REGISTRY.register(MapReduceMetricsCollector(args.cluster))
        REGISTRY.register(DataNodeMetricCollector(args.cluster))
        REGISTRY.register(JournalNodeMetricCollector(args.cluster))
        REGISTRY.register(HBaseMetricCollector(args.cluster))

        c = Consul(host='10.110.13.216')
        c.agent.service.register('hadoop_exporter_nn',
                                 service_id='hadoop_exporter_nn',
                                 address='10.9.11.95',
                                 port=port,
                                 tags=['hadoop'])
        start_http_server(port)
        print("Polling %s. Serving at port: %s" % (args.address, port))
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        c.agent.service.deregister(service_id='hadoop_exporter_nn')
        print(" Interrupted")
        exit(0)


if __name__ == "__main__":
    main()