# spark-kafka-thrift

A prototype Spark streaming (micro-batch) app that consumes event data from Kafka and then performs some processing (i.e. schema validation or translation if necessary).  Finally, the app can expose a thrift server JDBC endpoint allowing a user or process to query via SQL the Kafka event data. 

Possible Use-cases:
[x] live scoring machine learning framework in Spark that listens for prediction requests on Kafka, computes the prediction within Spark, and posts the result
[x] near real-time BI reporting via the Thrift JDBC SQL endpoint 

### Kafka Stream

```scala
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.spark.streaming.kafka.{OffsetRange, HasOffsetRanges, KafkaUtils}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import kafka.utils.{ZkUtils, ZKStringSerializer}

import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

val logger = LoggerFactory.getLogger(getClass)
val mapper = new ObjectMapper().registerModule(DefaultScalaModule)


object ZkClientBuilder {

   private lazy val DEFAULT_SERIALIZER: ZkSerializer = ZKStringSerializer

   /**
     * Create a new [[ZkClient]] object
     *
     * @param connectionString  Zookeeper endpoint
     * @param connectionTimeOut [[ZkClient]] connectionTimeOut in milliseconds
     * @param sessionTimeOut [[ZkClient]] sessionTimeOut in milliseconds
     * @param zkSerializer [[ZkClient]] serializer
     * @return [[ZkClient]] object
     */
   def apply(connectionString: String,
             connectionTimeOut: Int,
             sessionTimeOut: Int,
             zkSerializer: ZkSerializer = DEFAULT_SERIALIZER): ZkClient = {
     new ZkClient(connectionString, sessionTimeOut, connectionTimeOut, zkSerializer)
   }
 }


def isKeyPresent(zkClient: ZkClient, zkPath: String): Boolean = {
     try {
       ZkUtils.pathExists(zkClient, zkPath)
     } catch {
       case e: Throwable =>
         logger.info("No key found, exception was thrown", e)
         false
     }
   }


def readOffsets(zkClient: ZkClient, zkPath: String, topic: String): Option[Map[TopicAndPartition, Long]] = {
     if(isKeyPresent(zkClient, zkPath)) {
       val storedOffsets: (String, Stat) = ZkUtils.readData(zkClient, zkPath)

       Some(storedOffsets._1.split(',').map(partitionAndOffset => {
         val x = partitionAndOffset.split(':')
         val partition = x(0).toInt
         val offset = x(1).toLong
         (new TopicAndPartition(topic, partition), offset)
       }).toMap)
     } else {
       None
     }
   }


def saveOffsets(zkClient: ZkClient, zkPath: String, rdd: RDD[_]): Unit = {
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.info(s"Offset ranges are:\n\t$offsetsRangesStr")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
  }


def kafkaStream(ssc: StreamingContext, zkEndpoint: String, kafkaParams: Map[String, String], topic: String):
            InputDStream[(String, Array[Byte])] = {
          val zkClient = ZkClientBuilder(zkEndpoint, connectionTimeOut = 10000, sessionTimeOut = 10000)
          val zkPath = "/" + ssc.sparkContext.appName + "/" + "leotest"
          val storedOffsets: Option[Map[TopicAndPartition, Long]] = readOffsets(zkClient, zkPath, topic)
          val kafkaStream = storedOffsets match {
            case None =>
              // start from the latest offsets
              KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, Set(topic))
            case Some(fromOffsets) =>
              // start from previously saved offsets
              val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.key, mmd.message)
              KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](
                ssc, kafkaParams, fromOffsets, messageHandler)
          }
          kafkaStream.foreachRDD(rdd => saveOffsets(zkClient, zkPath, rdd))
          kafkaStream
        }
```

### Spark Session Singleton

```
import org.apache.spark.{rdd, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.thriftserver._


object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}
```

### Configurable Parameters

```scala
val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
val thriftPort = "my-thrift-port"
val broker = "my-broker:my-broker-port"
val topics = "my-topic"
val kafkaParams = Map[String, String]("metadata.broker.list" -> broker)
val zkEndpoint = "my-zookeeper:my-zookeeper-port"
```

### Execution - Thrift Server SQL API

```scala
val sparkConf = new SparkConf().setAppName("My App")
                               .set("spark.sql.hive.thriftServer.singleSession","true")
                               .set("hive.server2.thrift.port", thriftPort)
                               .set("spark.sql.warehouse.dir", warehouseLocation)
                               
val spark = SparkSessionSingleton.getInstance(sparkConf)
import spark.implicits._

val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
import sqlContext.implicits._

val ssc = new StreamingContext(spark.sparkContext,Seconds(2))

HiveThriftServer2.startWithContext(sqlContext)

val messages = kafkaStream(ssc, zkEndpoint, kafkaParams, topics).map(x => x._2)

messages.foreachRDD( rdd => {
  if (!rdd.isEmpty) { 
     val spark = SparkSessionSingleton.getInstance(sparkConf)
     import spark.implicits._
     rdd.toDF().cache().createOrReplaceTempView("view_df_temp")
     }
})

ssc.awaitTermination()
ssc.start()
ssc.stop()
```

### Execution - stdout

```scala
messages.foreachRDD( rdd => {
  if (!rdd.isEmpty) { 
     val spark = SparkSessionSingleton.getInstance(sparkConf)
     import spark.implicits._
      rdd.take(1).foreach(x =>
         print(x)
        )
     }
})
```

### Execution - mapping message formats

* requires mapper/schema util (i.e. janus)
```scala
import AvroUtil
import SchemaBaseURL

val messagesParsed = messages.map(x => AvroUtil.transform(x, SchemaBaseURL.PROD.getUrl))
```

### Dependencies - sbt

```
scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.0.1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.5"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.5"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % "2.6.5"
libraryDependencies += "com.fasterxml.jackson.datatype" % "jackson-datatype-guava" % "2.6.5"
libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.6.5"  
```

* also see [baryon](https://github.com/groupon/baryon)
