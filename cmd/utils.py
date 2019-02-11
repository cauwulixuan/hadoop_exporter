#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import socket
import requests
import argparse
import logging
import yaml
from subprocess import Popen, PIPE

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def get_module_logger(mod_name):
    '''
    define a common logger template to record log.
    @param mod_name log module name.
    @return logger.
    '''
    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.DEBUG)
    # 设置日志文件handler，并设置记录级别
    path = os.path.dirname(os.path.abspath(__file__))
    par_path = os.path.dirname(path)
    fh = logging.FileHandler(os.path.join(par_path, "hadoop_exporter.log"))
    fh.setLevel(logging.INFO)

    # 设置终端输出handler，并设置记录级别
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)

    # 设置日志格式
    fmt = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d]-[%(levelname)s]: %(message)s')
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)

    # 添加handler到logger对象
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

logger = get_module_logger(__name__)

def get_metrics(url):
    '''
    :param url: The jmx url, e.g. http://host1:50070/jmx,http://host1:8088/jmx, http://host2:19888/jmx...
    :return a dict of all metrics scraped in the jmx url.
    '''
    result = []
    try:
        s = requests.session()
        response = s.get(url, auth=("admin", "admin"), timeout=5)  # , params=params, auth=(self._user, self._password))
    except Exception as e:
        logger.warning("error in func: get_metrics, error msg: %s"%e)
        result = []
    else:    
        if response.status_code != requests.codes.ok:
            logger.warning("Get {0} failed, response code is: {1}.".format(url, response.status_code))
            result = []
        rlt = response.json()
        logger.debug(rlt)
        if rlt and "beans" in rlt:
            result = rlt['beans']
        else:
            logger.warning("No metrics get in the {0}.".format(url))
            result = []
    finally:
        s.close()
    return result

def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_hostname():
    '''
    get hostname via socket.
    @return a string of hostname
    '''
    try:
        host = socket.getfqdn()
    except Exception as e:
        logger.info("get hostname failed, error msg: {0}".format(e))
        return None
    else:
        return host

def read_json_file(path_name, file_name):
    '''
    read metric json files.
    '''
    path = os.path.dirname(os.path.realpath(__file__))
    parent_path = os.path.dirname(path)
    metric_path = os.path.join(parent_path, path_name)
    metric_name = "{0}.json".format(file_name)
    try:
        with open(os.path.join(metric_path, metric_name), 'r') as f:
            metrics = yaml.safe_load(f)
            return metrics
    except Exception as e:
        logger.info("read metrics json file failed, error msg is: %s" %e)
        return {}

def get_file_list(file_path_name):
    '''
    This function is to get all .json file name in the specified file_path_name.
    @param file_path: The file path name, e.g. namenode, ugi, resourcemanager ...
    @return a list of file name.
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    parent_path = os.path.dirname(path)
    json_path = os.path.join(parent_path, file_path_name)
    try:
        files = os.listdir(json_path)
    except OSError:
        logger.info("No such file or directory: '%s'" %json_path)
        return []
    else:
        rlt = []
        for i in range(len(files)):
            rlt.append(files[i].split(".json")[0])
        return rlt


def get_node_info(url):
    '''
    Firstly, I know how many nodes in the cluster.
    Secondly, exporter was installed in every node in the cluster.
    Therefore, how many services were installed in each node should be a key factor.
    In this function, a list of node info, including hostname, service jmx url should be returned.
    '''
    url = url.rstrip()
    host = get_hostname()
    node_info = {}
    try:
        s = requests.session()
        response = s.get(url, timeout = 120)
    except Exception as e:
        logger.info("Error happened while requests url {0}, error msg : {1}".format(url, e))
        node_info = {}
    else:
        if response.status_code != requests.codes.ok:
            logger.info("Get {0} failed, response code is: {1}.".format(url, response.status_code))
            node_info = {}
        result = response.json()
        logger.debug(result)
        if result:
            for k,v in result.items():
                for i in range(len(v)):
                    if host in v[i]:
                        node_info.setdefault(k, v[i][host])
                    else:
                        continue
            logger.debug(node_info)
        else:
            logger.info("No metrics get in the {0}.".format(url))
            node_info = {}
    finally:
        s.close()
    return node_info


def parse_args():

    parser = argparse.ArgumentParser(
        description = 'hadoop node exporter args, including url, metrics_path, address, port and cluster.'
    )
    parser.add_argument(
        '-c','--cluster',
        required = False,
        metavar = 'cluster_name',
        help = 'Hadoop cluster labels. (default "cluster_indata")',
        default = 'cluster_indata'
    )
    parser.add_argument(
        '-s','--services-api',
        required = True,
        metavar = 'services_api',
        help = 'Services api to scrape each hadoop jmx url. (default "127.0.0.1:9035")',
        default = '127.0.0.1:9035'
    )
    parser.add_argument(
        '-hdfs', '--namenode-url',
        required=False,
        metavar='namenode_jmx_url',
        help='Hadoop hdfs metrics URL. (default "http://indata-10-110-13-115.indata.com:50070/jmx")',
        default="http://indata-10-110-13-115.indata.com:50070/jmx"
    )
    parser.add_argument(
        '-rm', '--resourcemanager-url',
        required=False,
        metavar='resourcemanager_jmx_url',
        help='Hadoop resourcemanager metrics URL. (default "http://indata-10-110-13-116.indata.com:8088/jmx")',
        default="http://indata-10-110-13-116.indata.com:8088/jmx"
    )
    parser.add_argument(
        '-dn', '--datanode-url',
        required=False,
        metavar='datanode_jmx_url',
        help='Hadoop datanode metrics URL. (default "http://indata-10-110-13-114.indata.com:1022/jmx")',
        default="http://indata-10-110-13-114.indata.com:1022/jmx"
    )
    parser.add_argument(
        '-jn', '--journalnode-url',
        required=False,
        metavar='journalnode_jmx_url',
        help='Hadoop journalnode metrics URL. (default "http://indata-10-110-13-114.indata.com:8480/jmx")',
        default="http://indata-10-110-13-114.indata.com:8480/jmx"
    )
    parser.add_argument(
        '-mr', '--mapreduce2-url',
        required=False,
        metavar='mapreduce2_jmx_url',
        help='Hadoop mapreduce2 metrics URL. (default "http://indata-10-110-13-116.indata.com:19888/jmx")',
        default="http://indata-10-110-13-116.indata.com:19888/jmx"
    )
    parser.add_argument(
        '-nm', '--nodemanager-url',
        required=False,
        metavar='nodemanager_jmx_url',
        help='Hadoop nodemanager metrics URL. (default "http://indata-10-110-13-114.indata.com:8042/jmx")',
        default="http://indata-10-110-13-114.indata.com:8042/jmx"
    )
    parser.add_argument(
        '-hbase', '--hbase-url',
        required=False,
        metavar='hbase_jmx_url',
        help='Hadoop hbase metrics URL. (default "http://indata-10-110-13-115.indata.com:16010/jmx")',
        default="http://indata-10-110-13-115.indata.com:16010/jmx"
    )
    parser.add_argument(
        '-rs', '--regionserver-url',
        required=False,
        metavar='regionserver_jmx_url',
        help='Hadoop regionserver metrics URL. (default "http://indata-10-110-13-114.indata.com:60030/jmx")',
        default="http://indata-10-110-13-114.indata.com:60030/jmx"
    )
    parser.add_argument(
        '-hive', '--hive-url',
        required=False,
        metavar='hive_jmx_url',
        help='Hadoop hive metrics URL. (default "http://indata-10-110-13-116.indata.com:10502/jmx")',
        default="http://indata-10-110-13-116.indata.com:10502/jmx"
    )
    parser.add_argument(
        '-ld', '--llapdaemon-url',
        required=False,
        metavar='llapdaemon_jmx_url',
        help='Hadoop llapdaemon metrics URL. (default "http://indata-10-110-13-116.indata.com:15002/jmx")',
        default="http://indata-10-110-13-116.indata.com:15002/jmx"
    )
    parser.add_argument(
        '-p','--path',
        metavar='metrics_path',
        required=False,
        help='Path under which to expose metrics. (default "/metrics")',
        default='/metrics'
    )
    parser.add_argument(
        '-host','-ip','--address','--addr',
        metavar='ip_or_hostname',
        required=False,
        type=str,
        help='Polling server on this address. (default "127.0.0.1")',
        default='127.0.0.1'
    )
    parser.add_argument(
        '-P', '--port',
        metavar='port',
        required=False,
        type=int,
        help='Listen to this port. (default "9131")',
        default=9131
    )
    return parser.parse_args()


def main():

    print parse_args()
    print get_file_list("namenode")
    print get_file_list("resourcemanager")
    print "=============================="
    url = "http://10.10.6.10:9035/alert/getservicesbyhost"
    print get_node_info(url)
    print "=============================="
    print get_file_list("common")
    pass

if __name__ == '__main__':
    main()