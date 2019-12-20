### 实验3-3 spark编程

### 0. scala安装

ubuntu: apt-get install scala
为了后面配置scala的环境变量，这里先找出scala的位置
通过which scala 可以查看到java的执行路径（不同于安装路径）

/usr/bin/scala

执行 ls -lrt /usr/bin/scala 

执行 ls -lrt /etc/alternatives/scala，其显示的/usr/share/scala-2.11就是我们需要查询的scala安装目录

![1575354077832](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354077832.png)

### 1. spark安装

版本&环境: Ubuntu 18 + java 1.8 +hadoop-3.2.1 ，使用集群模式，两个节点h0和h1，h0作为master，仅有h1作为worker

spark使用hadoop的resourcemanager分配资源

下载，解压，配置相关文件见https://www.jianshu.com/p/a4a0e7e4e4b7

其中spark-env.sh配置见下图

![1575354266133](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354266133.png)

start-all.sh

h0![1575354364334](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354364334.png)

h1![1575354390378](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354390378.png)

运行样例程序

![1575354415279](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354415279.png)

webUI

![1575354438365](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\1575354438365.png)

### 2. spark编程实践

#### 2.1 pyspark配置及使用

实验环境：阿里云服务器，docker spark集群

- 安装anaconda，并配置jupyter见教程https://www.jianshu.com/p/670486953d9e（因为是在docker中配置，还需要在创建容器的时候配置端口映射，在阿里云安全组中开放对应的端口，关闭防火墙）

- 配置jupyter的自动补全https://www.jianshu.com/p/0ab80f63af8a

- 运行jupyter，并在本机浏览器访问：xx.xx.xx.x:端口号 （注意使用chrome浏览器，默认edge不行)

- 配置pysparkhttps://blog.csdn.net/dxyna/article/details/79772343

![image-20191207193127962](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191207193127962.png)

下面就可以愉快的写代码啦

#### 2.2 编程实践

- 统计各省销售最好的产品类别前十（每个省的数据中，第一个元素为商品类别，第二个是统计购买量）

![image-20191208165339543](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191208165339543.png)

```python
data=sc.textFile('file:///home/Hadoop/share-files/million_user_log.csv')#transform 类操作
#Spark每次作行动操作时，都是从最初的转化操作开始计算；如果不想从头开始计算，想保存中间结果表，就应该把数据载入缓存
data.cache()#相当于 .persist(MEMORY_ONLY) 将数据缓存存内存中
print(data.count())#count()为action类操作立即执行
print(data.first())
data=data.map(lambda x: list(x.split(',')))
data=data.filter(lambda x: x[7]=='2')
print(data.first())
data=data.map(lambda x: (x[10],x[2]))
print(data.first())
data=data.groupByKey()# 返回[(key,pyspark.resultiteratable),(,)]
data=data.mapValues(list)
#print(data.first())
def cat_10 (x):
    dct={}
    for key in x:
        dct[key]=dct.get(key,0)+1
    lst=sorted(dct.items(),key=lambda y:y[1],reverse=True)
    return lst[:10]
data=data.mapValues(cat_10)
print(data.first())
data.saveAsTextFile('file:///home/Hadoop/share-files/spark_local_output/out2')
#这里结果保存在本地，也可以选择saveAsHadoopData等保存在hdfs上
```

- 统计各省的双十一前十热门销售产品（购买最多前10的产品）-- 和MapReduce作业对比结果

![image-20191208165534032](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191208165534032.png)

```
代码同上一题，只需把(x[10],x[2]) 替换为(x[10],x[1])即可
```

与mapreduce对比

![image-20191208165659664](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191208165659664.png)

​	可以发现结果相同，但是用python 编写的spark程序简单非常非常多

- 查询双11那天浏览次数前十的品牌 -- 和Hive作业对比结果

  1. 使用RDD编程

     ```python
     data=sc.textFile('file:///home/Hadoop/share-files/million_user_log.csv')
     data.cache()
   data=data.map(lambda x: list(x.split(',')))
     data=data.filter(lambda x: x[7]=='0')
   data=data.map(lambda x:x[4])
     lst=sorted(data.countByValue().items(), key= lambda x: x[1],reverse=True)
     print(lst[:10])
     ```
  
     结果![image-20191209002131806](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191209002131806.png)
  
     之前使用Hive的结果：
  
     ![image-20191209002151717](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191209002151717.png)

  2. 此外还可以用dataframe进行查询（pyspark不支持Dataset，因为python本身不是一种类型安全的语言）

     ```python
   #使用Dataframe+sparkSQL
     from pyspark.sql.types import *
   #如果使用默认反射推断会将全部数据推断为String，并且淘宝数据没有header，这里用编程指定类型
     schema = StructType([
      StructField("user_id", StringType()),
      StructField("item_id",StringType()),
      StructField("cat_id", StringType()),
      StructField("merchant_id", StringType()),
      StructField("brand_id", StringType()),
      StructField("month", StringType()),
      StructField("day", StringType()),
      StructField("action", StringType()),
      StructField("age_range", StringType()),
      StructField("gender", StringType()),
      StructField("province", StringType())])
     
     data1=spark.read.csv('file:///home/Hadoop/share-files/million_user_log.csv',header=False,schema=schema)
     data1.createOrReplaceTempView('taobao')
     data1.show(5)
     
     ```
  
     ![image-20191211112449500](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191211112449500.png)
  
     ```python
     data2=spark.sql('select brand_id, count ( action ) actions from taobao where action = 0 group by brand_id order by actions desc limit 10')
     data2.show()
     ```
  
     ![image-20191211112510922](C:\Users\DELL\Desktop\金融大数据\实验3\实验3-3 spark实践.assets\image-20191211112510922.png)
  
     参考教程：https://www.jianshu.com/p/cb0fec7a4f6d
  
  3. 此外还可以是加载hiveContext然后运行SQL语句进行查询
  
     ```python
     from pyspark.sql import HiveContext
     hive_context=HiveContext(sc)
     hive_context.sql('select brand_id, count ( action ) actions from tUser where action = 0 group by brand_id order by actions desc limit 10').show()
     ```
  
     
  
  
  
  