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
1.解析yarn相关指标
2.解析hbase相关指标
3.解析MapReduce相关指标