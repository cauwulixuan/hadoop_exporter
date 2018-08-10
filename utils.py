#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import requests
import argparse
import logging
import yaml
from config import Config

c = Config

def get_module_logger(mod_name):
    '''
    define a common logger template to record log.
    @param mod_name log module name.
    @return logger.
    '''
    logger = logging.getLogger(mod_name)
    logger.setLevel(logging.DEBUG)
    # 设置日志文件handler，并设置记录级别
    fh = logging.FileHandler("hadoop_exporter.log")
    fh.setLevel(logging.ERROR)

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
    try:
        response = requests.get(url, auth=("admin", "admin"), timeout=5)  # , params=params, auth=(self._user, self._password))
    except Exception as e:
        logger.error(e)
    else:    
        if response.status_code != requests.codes.ok:
            logger.error("Get {0} failed, response code is: {1}.".format(url, response.status_code))
            return []
        result = response.json()
        logger.debug(result)
        if result and "beans" in result:
            return result
        else:
            logger.error("No metrics get in the {0}.".format(url))
            return []

def read_json_file(path_name, file_name):
    '''
    read metric json files.
    '''
    path = os.path.dirname(os.path.realpath(__file__))
    metric_path = os.path.join(path, path_name)
    metric_name = "{0}.json".format(file_name)
    try:
        with open(os.path.join(metric_path, metric_name), 'r') as f:
            metrics = yaml.safe_load(f)
            return metrics
    except Exception as e:
        logger.error("read metrics json file failed, error msg is: %s" %e)
        sys.exit(1)


def get_url_list():

    url_list = []
    items = vars(Config).items()
    tmp = dict(items)
    for k,v in tmp.items():
        if "URL" in k:
            url_list.append(v)
    return url_list

def get_active_host(cluster, component):
    '''
    Using ambari API to get Active component.
    @param cluster: cluster name.
    @param component: component name, e.g. namenode
    @return a string of host name.
    '''
    host = "10.110.13.54"
    component_upper = component.upper()
    url = "http://{0}:8080/api/v1/clusters/{1}/host_components?HostRoles/component_name={2}&metrics/dfs/FSNamesystem/HAState=active".format(host, cluster, component_upper)
    try:
        response = requests.get(url, auth=("admin", "admin"), timeout=5)  # , params=params, auth=(self._user, self._password))
    except Exception as e:
        logger.error(e)
    else:    
        if response.status_code != requests.codes.ok:
            logger.error("Get {0} failed, response code is: {1}.".format(url, response.status_code))
            return None
        result = response.json()
        logger.debug(result)
        if result and "items" in result and result['items']:
            return result['items'][0]['HostRoles']['host_name']
        else:
            logger.error("No metrics get in the {0}.".format(url))
            return None


def get_file_list(file_path_name):
    '''
    This function is to get all .json file name in the specified file_path_name.
    @param file_path: The file path name, e.g. namenode, ugi, resourcemanager ...
    @return a list of file name.
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(path, file_path_name)
    try:
        files = os.listdir(json_path)
    except OSError:
        logger.error("No such file or directory: '%s'" %json_path)
        return []
    else:
        rlt = []
        for i in range(len(files)):
            rlt.append(files[i].split(".")[0])
        return rlt


def parse_args():

    parser = argparse.ArgumentParser(
        description = 'hadoop node exporter args, including url, metrics_path, address, port and cluster.'
    )
    parser.add_argument(
        '-c','--cluster',
        required = False,
        dest = 'cluster',
        metavar = 'cluster',
        help = 'Hadoop cluster labels.',
        default = 'cluster1'
    )
    parser.add_argument(
        '-hdfs', '--namenode-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop hdfs metrics URL. (default "http://ip:port/jmx")',
        default=c.HDFS_ACTIVE_URL
    )
    parser.add_argument(
        '-resourcemanager', '--resourcemanager-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop resourcemanager metrics URL. (default "127.0.0.1:8088/jmx")',
        default=c.YARN_ACTIVE_URL
    )
    parser.add_argument(
        '-dn', '--datanode-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop datanode metrics URL. (default "http://ip:port/jmx")',
        default=c.DATA_NODE1_URL
    )
    parser.add_argument(
        '-jn', '--journalnode-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop datanode metrics URL. (default "http://ip:port/jmx")',
        default=c.JOURNAL_NODE1_URL
    )
    parser.add_argument(
        '-mr', '--mapreduce2-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop datanode metrics URL. (default "http://ip:port/jmx")',
        default=c.MAPREDUCE2_URL
    )
    parser.add_argument(
        '-hbase', '--hbase-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop datanode metrics URL. (default "http://ip:port/jmx")',
        default=c.HBASE_ACTIVE_URL
    )
    parser.add_argument(
        '-hive', '--hive-url',
        choices = get_url_list(),
        required=False,
        help='Hadoop datanode metrics URL. (default "http://ip:port/jmx")',
        default=c.HIVE_URL
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
    '''
    config = Config()
    url = config.BASE_URL
    address = config.DEFAULT_ADDR
    port = config.DEFAULT_PORT
    parsejobs(url)
    logger.info("utils.py info msg.")
    '''
    # url = []
    # items = vars(Config).items()
    # tmp = dict(items)
    # keys = filter(lambda k:"URL" in k, tmp.keys())
    # print keys
    # for k in keys:
    #     url.append(tmp[k])
    # print [lambda k:tmp[k] for k in keys]
    # print [lambda v:v for k,v in tmp.items if "URL" in k]
    
    # print url
    # args = parse_args()
    # print args.address
    print parse_args()
    print get_file_list("namenode")
    print get_file_list("resourcemanager")
    print get_active_host("indata", "namenode")
    print get_file_list("common")
    pass

if __name__ == '__main__':
    main()