[TOC]

具体内容已在专栏展示：

https://blog.csdn.net/qq_15604349/category_11734572.html

源码地址：

https://github.com/lvjianyang/flink

https://gitee.com/jian_yang_lv/flink

# 实战练习

## 项目准备

### Maven 快速入门

**创建项目**

唯一的要求是安装**Maven 3.0.4**（或更高版本）和**Java 8.x。**

使用以下命令之一**创建项目**：

```bash
$ mvn archetype:generate -DarchetypeGroupId=org.apache.flink -DarchetypeArtifactId=flink-quickstart-java -DarchetypeVersion=1.14.4
```

**构建项目**

如果您想**构建/打包您的项目**，请转到您的项目目录并运行“ `mvn clean package`”命令。您将**找到一个 JAR 文件**，其中包含您的应用程序，以及您可能已作为依赖项添加到应用程序的连接器和库：`target/<artifact-id>-<version>.jar`.

### 本地模式安装

Flink 旨在以闪电般的速度处理连续的数据流。这个简短的指南将向您展示如何下载、安装和运行最新的 Flink 稳定版本。您还将运行一个示例 Flink 作业并在 Web UI 中查看它。

- 下载 

为了运行Flink，只需提前安装好 **Java 8 或者 Java 11**。你可以通过以下命令来检查 Java 是否已经安装正确。

```
$ java -version
$ curl -O https://mirrors.cloud.tencent.com/apache/flink/flink-1.14.4//flink-1.14.4-bin-scala_2.12.tgz
$ tar -zxzf flink-*.tgz
```

制作镜像

```
$ vi Dockerfile
FROM daocloud.io/library/centos:7
MAINTAINER ljy
RUN mkdir /usr/local/jdk
WORKDIR /usr/local/jdk
ADD jdk-8u211-linux-x64.tar.gz  /usr/local/jdk

ENV JAVA_HOME /usr/local/jdk/jdk1.8.0_211
ENV JRE_HOME /usr/local/jdk/jdk1.8.0_211/jre
ENV PATH $JAVA_HOME/bin:$PATH


ADD flink-1.14.4-bin-scala_2.12.tgz /opt
WORKDIR /opt/flink-1.14.4
EXPOSE 8081 
 
$ docker build -f Dockerfile -t flink .
$ docker run -itd -p 8081:8081 --name flink flink 
$ docker exec -it flink /bin/bash
```



- 启动和停止本地集群

要启动本地集群，请运行 Flink 附带的 bash 脚本：

```bash
$ ./bin/start-cluster.sh
```

Flink 现在作为后台进程运行。您可以使用以下命令检查其状态：

```bash
$ ps aux | grep flink
```

您应该能够导航到[localhost:8081](http://localhost:8081/)的 Web UI以查看 Flink 仪表板并看到集群已启动并正在运行。

要快速停止集群和所有正在运行的组件，您可以使用提供的脚本：

```bash
$ ./bin/stop-cluster.sh
```

- 提交 Flink 作业

Flink 提供了一个 CLI 工具**bin/flink**，它可以运行打包为 Java ARchives (JAR) 的程序并控制它们的执行。提交作业是指将作业的 JAR 文件和相关依赖上传到正在运行的 Flink 集群并执行。

Flink 版本附带示例作业，您可以在**示例/**文件夹中找到这些示例作业。

要将示例字数统计作业部署到正在运行的集群，请发出以下命令：

```bash
$ ./bin/flink run examples/streaming/WordCount.jar
```

您可以通过查看日志来验证输出：

```bash
$ tail log/flink-*-taskexecutor-*.out
```

样本输出：

```bash
  (nymph,1)
  (in,3)
  (thy,1)
  (orisons,1)
  (be,4)
  (all,2)
  (my,1)
  (sins,1)
  (remember,1)
  (d,4)
```

此外，您可以查看 Flink 的[Web UI](http://localhost:8081/)来监控集群的状态和正在运行的作业。

任务管理器可以查看日志。

## 配置

### 中文乱码

web页面task managers logs和stdout中文显示均乱码，/conf/flink-conf.yaml添加一行。

```
env.java.opts: "-Dfile.encoding=UTF-8"
```



## 快速开始

### socket 源流处理

如下是一个完整的、可运行的程序示例，它是基于流窗口的单词统计应用程序，计算 5 秒窗口内来自 Web 套接字的单词数。你可以复制并粘贴代码以在本地运行。

```java
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
```

修改pom

```
<mainClass>quick.WindowWordCount</mainClass>
```

要运行示例程序，首先从终端使用 netcat 启动输入流：

```bash
nc -lk 9999
```

然后，将打包应用程序提交，Flink 的[Web UI](http://localhost:8081/)来提交作业监控集群的状态和正在运行的作业。

只需输入一些单词，然后按回车键即可传入新单词。这些将作为单词统计程序的输入。如果想查看大于 1 的计数，在 5 秒内重复输入相同的单词即可（如果无法快速输入，则可以将窗口大小从 5 秒增加 ☺）。

```
$ tail log/flink-*-taskexecutor-*.out
```

### 文件流处理

```
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCount {


    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text;
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = env.fromElements("Flink Spark Storm","Flink Flink Flink");
        }

        DataStream<Tuple2<String, Integer>> counts = text
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);                                                     //1


        if (params.has("output")) {
            counts.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        env.execute("Streaming WordCount");
    }


    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {

            String[] tokens = value.toLowerCase().split("\\W+");


            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

}

```

修改pom

```
<version>1.0.1</version>
<mainClass>quick.WordCount</mainClass>
```

然后，将打包应用程序提交，Flink 的[Web UI](http://localhost:8081/)来提交作业监控集群的状态和正在运行的作业。

**批处理**

```
 bin/flink run -Dexecution.runtime-mode=BATCH examples/streaming/WordCount.jar
```

然后，将打包应用程序提交，Flink 的[Web UI](http://localhost:8081/)来提交作业监控集群的状态和正在运行的作业。



## 算子

### 概述

用户通过算子能将一个或多个 DataStream 转换成新的 DataStream，在应用程序中可以将多个数据转换算子合并成一个复杂的数据流拓扑。

这部分内容将描述 Flink DataStream API 中基本的数据转换API，数据转换后各种数据分区方式，以及算子的链接策略。



## 连接器

要在应用程序中使用这些连接器之一，通常需要额外的第三方组件，例如用于数据存储或消息队列的服务器。另请注意，虽然本节中列出的流连接器是 Flink 项目的一部分并且包含在源代码版本中，但它们不包含在二进制发行版中。



### kafka 连接器

该文档描述的是基于新数据源 API的 Kafka Source。

**依赖** 

Apache Flink 集成了通用的 Kafka 连接器，它会尽力与 Kafka client 的最新版本保持同步。该连接器使用的 Kafka client 版本可能会在 Flink 版本之间发生变化。 当前 Kafka client 向后兼容 0.10.0 或更高版本的 Kafka broker。 

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka_2.11</artifactId>
    <version>1.14.4</version>
</dependency>
```

Flink 目前的流连接器还不是二进制发行版的一部分。

#### Kafka Source

**使用方法** 

Kafka Source 提供了构建类来创建 `KafkaSource` 的实例。以下代码片段展示了如何构建 `KafkaSource` 来消费 “input-topic” 最早位点的数据， 使用消费组 “my-group”，并且将 Kafka 消息体反序列化为字符串：

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(brokers)
    .setTopics("input-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
```

以下属性在构建 KafkaSource 时是必须指定的：

- Bootstrap server，通过 `setBootstrapServers(String)` 方法配置
- 消费者组 ID，通过 `setGroupId(String)` 配置
- 要订阅的 Topic / Partition
- 用于解析 Kafka 消息的反序列化器（Deserializer）



#### Kafka Sink

`KafkaSink` 可将数据流写入一个或多个 Kafka topic。

**使用方法** 

Kafka sink 提供了构建类来创建 `KafkaSink` 的实例。以下代码片段展示了如何将字符串数据按照至少一次（at lease once）的语义保证写入 Kafka topic：

```java
DataStream<String> stream = ...;
        
KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("topic-name")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
        
stream.sinkTo(sink);
```

以下属性在构建 KafkaSink 时是必须指定的：

- Bootstrap servers, `setBootstrapServers(String)`
- 消息序列化器（Serializer）, `setRecordSerializer(KafkaRecordSerializationSchema)`
- 如果使用`DeliveryGuarantee.EXACTLY_ONCE` 的语义保证，则需要使用 `setTransactionalIdPrefix(String)`





### JDBC Connector 

#### jdbc sink

该连接器可以向 JDBC 数据库写入数据。

添加下面的依赖以便使用该连接器（同时添加 JDBC 驱动）：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-jdbc_2.11</artifactId>
    <version>1.14.4</version>
</dependency>

<dependency>
	<groupId>mysql</groupId>
	<artifactId>mysql-connector-java</artifactId>
	<version>8.0.19</version>
</dependency>
```



已创建的 JDBC Sink 能够保证至少一次的语义。 更有效的精确执行一次可以通过 upsert 语句或幂等更新实现。

用法示例：

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env
        .fromElements(...)
        .addSink(JdbcSink.sink(
                "insert into books (id, title, author, price, qty) values (?,?,?,?,?)",
                (ps, t) -> {
                    ps.setInt(1, t.id);
                    ps.setString(2, t.title);
                    ps.setString(3, t.author);
                    ps.setDouble(4, t.price);
                    ps.setInt(5, t.qty);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(getDbMetadata().getUrl())
                        .withDriverName(getDbMetadata().getDriverClass())
                        .build()));
env.execute();
```





### Elasticsearch 连接器 

此连接器提供可以向 Elasticsearch 索引请求文档操作的 sinks。 要使用此连接器，请根据 Elasticsearch 的安装版本将以下依赖之一添加到你的项目中：

```
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-connector-elasticsearch7_2.11</artifactId>
	<version>1.14.4</version>
</dependency>
```



请注意，流连接器目前不是二进制发行版的一部分。

#### 安装 Elasticsearch 

本文采用 docker 方式安装。

#### Elasticsearch Sink 
