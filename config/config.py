#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import os

basedir = os.path.dirname(os.path.abspath(__file__))


class Config(object):
    DEBUG = True
    LOGGER_LEVEL = logging.DEBUG
    LOGGER_FILE = os.path.join(basedir, 'hadoop.log')
    DEFAULT_ADDR = '10.9.11.95'
    DEFAULT_PORT = 9089
    DEFAULT_PATH = '/metrics'

    # a REST API to get all components info.
    # http://10.110.13.163:8080/api/v1/clusters/<cluster_name>/services/HDFS/components/DATANODE
    # consul-template -template="test.tpl:test.conf" -once

    # egrep -i -A 1 "namenode.http-address" /etc/hadoop/conf/hdfs-site.xml
    HDFS_ACTIVE_URL = "http://10.110.13.165:50070/jmx"
    HDFS_STANDBY_URL = "http://10.110.13.164:50070/jmx"

    # egrep -i -A 1  "datanode.http.address" /etc/hadoop/conf/hdfs-site.xml
    # should know every datanode ip
    DATA_NODE1_URL = "http://10.110.13.165:1022/jmx"
    DATA_NODE2_URL = "http://10.110.13.164:1022/jmx"
    DATA_NODE3_URL = "http://10.110.13.163:1022/jmx"

    # egrep -i -A 1  "dfs.journalnode.http-address" /etc/hadoop/conf/hdfs-site.xml
    JOURNAL_NODE1_URL = "http://10.110.13.165:8480/jmx"
    JOURNAL_NODE2_URL = "http://10.110.13.164:8480/jmx"
    JOURNAL_NODE3_URL = "http://10.110.13.163:8480/jmx"

    # egrep -i "resourcemanager.resourcemanager.webapp.address" /etc/hadoop/conf/resourcemanager-site.xml
    YARN_ACTIVE_URL = "http://10.110.13.164:8088/jmx"
    YARN_STANDBY_URL = "http://10.110.13.165:8088/jmx"

    NodeManager_URL1 = "http://10.110.13.163:8042/jmx"
    NodeManager_URL2 = "http://10.110.13.164:8042/jmx"
    NodeManager_URL3 = "http://10.110.13.165:8042/jmx"


    # egrep -i "mapreduce.jobhistory.webapp.address" /etc/hadoop/conf/mapred-site.xml
    MAPREDUCE2_URL = "http://10.110.13.165:19888/jmx"

    # egrep -i "hbase.master.info.port" /usr/hdp/current/hbase-master/conf/hbase-site.xml
    HBASE_ACTIVE_URL = "http://10.110.13.164:16010/jmx"
    HBASE_STANDBY_URL = "http://10.110.13.165:16010/jmx"

    RegionServer_URL1 = "http://10.110.13.163:60030/jmx"
    RegionServer_URL2 = "http://10.110.13.164:60030/jmx"
    RegionServer_URL3 = "http://10.110.13.165:60030/jmx"
    


    # egrep -i "hive.server2.webui.port" /usr/hdp/2.6.1.0-129/hive2/conf/hive-site.xml
    HIVE_URL = "http://10.10.6.10:10502/jmx"
    