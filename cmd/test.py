#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
import re
import time
from sys import exit
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, HistogramMetricFamily, REGISTRY

from consul import Consul

from cmd import utils
from cmd.utils import get_module_logger
from cmd.hdfs_namenode import NameNodeMetricCollector
from cmd.hdfs_datanode import DataNodeMetricCollector
from cmd.hdfs_journalnode import JournalNodeMetricCollector
from cmd.yarn_resourcemanager import ResourceManagerMetricCollector
from cmd.yarn_nodemanager import NodeManagerMetricCollector
from cmd.mapreduce_jobhistoryserver import MapReduceMetricCollector
from cmd.hbase_master import HBaseMasterMetricCollector
from cmd.hbase_regionserver import HBaseRegionServerMetricCollector
from cmd.hive_server import HiveServerMetricCollector
from cmd.hive_llap import HiveLlapDaemonMetricCollector

logger = get_module_logger(__name__)


def dec(func):
    from functools import wraps
    @wraps(func)
    def wrapper(*args, **kwargs):
        t1 = time.time()
        func(*args, **kwargs)
        t2 = time.time()
        logger.error("===== %s runs ===> %s =====" %(func.__name__, (t2-t1)))
    return wrapper

def register_consul(address, port):
    c = Consul(host='10.10.6.11')
    c.agent.service.register('hadoop_exporter_dev',
                             service_id='hadoop_exporter_dev2',
                             address='indata-10-10-6-11.indata.com',
                             port=port,
                             tags=['hadoop'])
    start_http_server(port)
    # print("Polling %s. Serving at port: %s" % (args.address, port))
    print("Polling %s. Serving at port: %s" % (address, port))


def register_prometheus(rest_url):
    try:
        rest_url = rest_url
        namenode_flag, datanode_flag, journalnode_flag, resourcemanager_flag, nodemanager_flag, hbase_master_flag, hbase_regionserver_flag, historyserver_flag, hive_server_interactive_flag, hive_llap_flag = 1,1,1,1,1,1,1,1,1,1
        service_list = ['DATANODE']
        while True:
            print "=================START==================="
            if 'DATANODE' in service_list:
                if datanode_flag:
                    cluster = "xxx"
                    datanode_url = "http://indata-10-10-6-11.indata.com:1022/jmx"
                    print "datanode_url = {0}, start to register".format(datanode_url)
                    REGISTRY.register(DataNodeMetricCollector(cluster, datanode_url))
                    from prometheus_client import start_http_server,exposition
                    print '##########  _names_to_collectors ################'
                    print REGISTRY._names_to_collectors
                    print '\n\n############# _collector_to_names #################' 
                    print REGISTRY._collector_to_names
                    #REGISTRY.collect()
                    data= exposition.generate_latest(registry=REGISTRY)
                    print data
                    datanode_flag = 0
            else:
                if not datanode_flag:
                    print "datanode_url = {0}, start to deregister".format(datanode_url)
                    REGISTRY.unregister(DataNodeMetricCollector(cluster, datanode_url))
                    print "after unregister"
                    datanode_flag = 1
            print "=================END==================="
            time.sleep(10)
            service_list = []

    except KeyboardInterrupt:
        print "Interrupted"
        exit(0)
    except Exception as e:
        logger.info("Error in register proemtheus, mgs: {0}".format(e))
        pass

def main():
    try:
        args = utils.parse_args()
        address = args.address
        port = int(args.port)
        rest_url = args.services_api
        register_consul(address, port)
        register_prometheus(rest_url)
    except Exception as e:
        logger.info('Error happened, msg: %s'%e)
    else:
        pass

if __name__ == "__main__":
    main()
