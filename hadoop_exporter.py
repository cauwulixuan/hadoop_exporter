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
from cmd.utils import get_host_ip
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


def register_consul(address, port):
    start_http_server(port)
    # print("Polling %s. Serving at port: %s" % (args.address, port))
    print "Polling %s. Serving at port: %s" % (address, port)


def register_prometheus(rest_url):
    try:
        namenode_flag, datanode_flag, journalnode_flag, resourcemanager_flag, nodemanager_flag, hbase_master_flag, hbase_regionserver_flag, historyserver_flag, hive_server_interactive_flag, hive_llap_flag = 1,1,1,1,1,1,1,1,1,1
        while True:
            url = 'http://{0}/alert/getservicesbyhost'.format(rest_url)
            node_info = utils.get_node_info(url)
            if node_info:
                for cluster, info in node_info.items():
                    for k, v in info.items():
                        if 'NAMENODE' in k:
                            if namenode_flag:
                                namenode_url = v['jmx']
                                logger.info("namenode_url = {0}, start to register".format(namenode_url))
                                REGISTRY.register(NameNodeMetricCollector(cluster, namenode_url))
                                namenode_flag = 0
                                continue
                        if 'DATANODE' in k:
                            if datanode_flag:
                                datanode_url = v['jmx']
                                logger.info("datanode_url = {0}, start to register".format(datanode_url))
                                REGISTRY.register(DataNodeMetricCollector(cluster, datanode_url))
                                datanode_flag = 0
                                continue
                        if 'JOURNALNODE' in k:
                            if journalnode_flag:
                                journalnode_url = v['jmx']
                                logger.info("journalnode_url = {0}, start to register".format(journalnode_url))
                                REGISTRY.register(JournalNodeMetricCollector(cluster, journalnode_url))
                                journalnode_flag = 0
                                continue
                        if 'RESOURCEMANAGER' in k:
                            if resourcemanager_flag:
                                resourcemanager_url = v['jmx']
                                logger.info("resourcemanager_url = {0}, start to register".format(resourcemanager_url))
                                REGISTRY.register(ResourceManagerMetricCollector(cluster, resourcemanager_url))
                                resourcemanager_flag = 0
                                continue
                        if 'NODEMANAGER' in k:
                            if nodemanager_flag:
                                nodemanager_url = v['jmx']
                                logger.info("nodemanager_url = {0}, start to register".format(nodemanager_url))
                                REGISTRY.register(NodeManagerMetricCollector(cluster, nodemanager_url))
                                nodemanager_flag = 0
                                continue
                        if 'HBASE_MASTER' in k:
                            if hbase_master_flag:
                                hbase_master_url = v['jmx']
                                logger.info("hbase_master_url = {0}, start to register".format(hbase_master_url))
                                REGISTRY.register(HBaseMasterMetricCollector(cluster, hbase_master_url))
                                hbase_master_flag = 0
                                continue
                        if 'HBASE_REGIONSERVER' in k:
                            if hbase_regionserver_flag:
                                hbase_regionserver_url = v['jmx']
                                logger.info("hbase_regionserver_url = {0}, start to register".format(hbase_regionserver_url))
                                REGISTRY.register(HBaseRegionServerMetricCollector(cluster, hbase_regionserver_url))
                                hbase_regionserver_flag = 0
                                continue
                        if 'HISTORYSERVER' in k:
                            if historyserver_flag:
                                historyserver_url = v['jmx']
                                logger.info("historyserver_url = {0}, start to register".format(historyserver_url))
                                REGISTRY.register(MapReduceMetricCollector(cluster, historyserver_url))
                                historyserver_flag = 0
                                continue
                        if 'HIVE_SERVER_INTERACTIVE' in k:
                            if hive_server_interactive_flag:
                                hive_server_interactive_url = v['jmx']
                                logger.info("hive_server_interactive_url = {0}, start to register".format(hive_server_interactive_url))
                                REGISTRY.register(HiveServerMetricCollector(cluster, hive_server_interactive_url))
                                hive_server_interactive_flag = 0
                                continue
                        if 'HIVE_LLAP' in k:
                            if hive_llap_flag:
                                hive_llap_url = v['jmx']
                                logger.info("hive_llap_url = {0}, start to register".format(hive_llap_url))
                                REGISTRY.register(HiveLlapDaemonMetricCollector(cluster, hive_llap_url))
                                hive_llap_flag = 0
                                continue
                time.sleep(300)
            else:
                logger.error("No service running in THIS node")
                # if getserviceshost url return null dict, sleep 60s, retry to reconnect the api.
                time.sleep(300)

    except KeyboardInterrupt:
        print "Interrupted"
        exit(0)
    except Exception as e:
        logger.info("Error in register prometheus, msg: {0}".format(e))
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
