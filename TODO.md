## TODO:
```
class MetricCol(Object)

class ComponentCollector(MetricCol):
    def __init__(xxxx):
        MetricCol.__init__(xxxx)

collector(cluster, url, component, service):
@param cluster: cluster name.
@param url: scrape all metrics from the specified url, each component has unique url.
@param component: component name, corresponding to the same directory to read the metrics in json type. On the other hand, the component name should use to check component status, active or standby via ambari REST API. 
@param service: service name, simply like the component param.
//@param name: the specified name, corresponding to the json file.
@return xxxxx.
```

## TODO:
1. scraped metrics, including setup labels and add values.
2. setup labels, 

每个`Collector`执行前, 优先获取`Common Metrics`, 将`Common Metrics`合并到每一个具体的`Collector`.
`Common Metrics`数据格式一致，获取方式一致。可以单独提取出来。
```
@_Wrapper
class SpecificCollector():
    pass
```

## TODO:
1.无需单独设定一个类进行解析，只需要返回对应的`json`数据即可。
2.带参数的包装器。
3.类包装器。

## TODO:
1. 解析yarn相关指标
2. 解析hbase相关指标
3. 解析MapReduce相关指标

## TODO:
1. 更新process_status_exporter, 添加大数据组件的组件状态信息
2. 更新前端页面, 适配大数据组件的监控展示
3. 皓轩接口发生变化, 需要获取url信息回传给REST API
4. hadoop_exporter相关dashboard制作
5. 已有监控系统的告警规则编写

## TODO:
1. 更新regionserver/nodemanager/hiveserver2/llapdaemon

## TODO:
1. 更新llapdaemon
2. 拆分单节点和多节点exporter，给多节点exporter打上hostname标签
3. 更新process_status_exporter
4. 更新dashboard

## TODO:
HBASE之后

手工安装python包
pip install --no-index --find-links http://10.110.13.93:81/httpfiles/pipfiles/ --trusted-host 10.110.13.93 prometheus_client


hdfs_namenode_process_up
hdfs_datanode_process_up
hdfs_journalnode_process_up
hbase_master_process_up
hbase_regionserver_process_up
hive_server_process_up
yarn_nodemanager_process_up
yarn_resourcemanager_process_up
mapreduce_historyserver_process_up


/run/hadoop/hdfs/hadoop-hdfs-datanode.pid
/run/hadoop/hdfs/hadoop-hdfs-journalnode.pid
/run/hadoop/hdfs/hadoop-hdfs-namenode.pid

/run/hbase/hbase-hbase-master.pid
/run/hbase/hbase-hbase-regionserver.pid

/run/hive/hive.pid
/run/hive/hive-server.pid

/run/hadoop-yarn/yarn/yarn-yarn-nodemanager.pid
/run/hadoop-yarn/yarn/yarn-yarn-resourcemanager.pid

/run/hadoop-mapreduce/mapred/mapred-mapred-historyserver.pid


jvm.RegionServer.JvmMetrics.MemNonHeapMaxM
jvm.RegionServer.JvmMetrics.MemNonHeapMaxM
jvm.RegionServer.JvmMetrics.MemNonHeapUsedM
jvm.RegionServer.JvmMetrics.MemNonHeapCommittedM



## TODO
1. 修改 `prometheus.yaml`
```
[root@indata-10-10-6-10 hadoop_exporter]# cat /etc/prometheus.yml
# my global config
global:
  scrape_interval:     15s # Set the scrape interval to every 15 seconds. Default is every 1 minute.
  evaluation_interval: 15s # Evaluate rules every 15 seconds. The default is every 1 minute.
  # scrape_timeout is set to the global default (10s).

  # Attach these labels to any time series or alerts when communicating with
  # external systems (federation, remote storage, Alertmanager).
  external_labels:
      monitor: 'indata-monitor'

# Load rules once and periodically evaluate them according to the global 'evaluation_interval'.
rule_files:
  # - "first.rules"
  # - "second.rules"

# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'prometheus'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.
    scrape_interval: 5s
    static_configs:
      - targets: ['10.10.6.10:9500']

  - job_name: 'consul_node'

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.
    consul_sd_configs:
      - server: '127.0.0.1:8500'

    relabel_configs:
      - source_labels: [__meta_consul_tags]
        regex: .*,monitor,.*
        action: keep
      - source_labels: [__meta_consul_service]
        target_label: services
      - source_labels: [__meta_consul_service_address]
        target_label: hostname
      - source_labels: [__address__]
        regex: (.*)-([0-9]*)-([0-9]*)-([0-9]*)-([0-9]*)(.*):(.*)
        replacement: ${2}.${3}.${4}.${5}:${7}
        target_label: __address__

  - job_name: 'consul_hadoop'
    scrape_interval: 30s
    consul_sd_configs:
      - server: '127.0.0.1:8500'

    relabel_configs:
      - source_labels: [__meta_consul_tags]
        regex: .*,hadoop.*,.*
        action: keep
      - source_labels: [__meta_consul_service]
        target_label: services
      - source_labels: [__meta_consul_service_address]
        target_label: hostname
      - source_labels: [__address__]
        regex: (.*)-([0-9]*)-([0-9]*)-([0-9]*)-([0-9]*)(.*):(.*)
        replacement: ${2}.${3}.${4}.${5}:${7}
        target_label: __address__
```
2. 更新 `process_status_exporter`

3. 注册到`consul`时, 把 `ip`替换成`hostname`

4. 部署`alertmanager`, 对应的更新`prometheus.yaml`

5. 更新所有`dashboards`

6. 设置`Home`页面为自定义页面, 需要更新`/usr/local/grafana/public/dashboards/home.json`文件, 重启`grafana-server`生效

7. 添加`grafana`插件：
```
chmod 755 /usr/local/bin/grafana-cli
ln -s /usr/local/grafana/bin/grafana-cli /usr/local/bin/grafana-cli
grafana-cli plugins install vonage-status-panel
grafana-cli plugins install grafana-piechart-panel
```

8. browser2api.py脚本中:
  - genID为null, 需修改, 现为实际id
  - overwrite为true, 需修改, 现为"true"