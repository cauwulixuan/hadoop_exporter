#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

def parse_args():

    parser = argparse.ArgumentParser(
        description = 'hadoop node exporter args, including url, metrics_path, address, port and cluster.'
    )
    parser.add_argument(
        '-c','--cluster',
        required = False,
        # dest = 'cluster',
        metavar = 'cluster_name',
        help = 'Hadoop cluster labels. (default "cluster_indata")',
        default = 'cluster_indata'
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
        help='Listen to this port. (default "9130")',
        default=9130
    )
    return parser.parse_args()

def main():
    args = parse_args()
    print args.cluster
    print "namenode_url = %s"%args.namenode_url
    print "datanode_url = %s"%args.datanode_url
    print "journalnode_url = %s"%args.journalnode_url
    print "resourcemanager_url = %s"%args.resourcemanager_url
    print "nodemanager_url = %s"%args.nodemanager_url
    print "mapreduce2_url = %s"%args.mapreduce2_url
    print "hbase_url = %s"%args.hbase_url
    print "regionserver_url = %s"%args.regionserver_url
    print "hive_url = %s"%args.hive_url
    print "llapdaemon_url = %s"%args.llapdaemon_url
    print "address = %s"%args.address
    print "port = %s"%args.port

if __name__ == '__main__':
    main()