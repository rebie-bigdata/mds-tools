import java.util.UUID

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object MDSBuilder extends App {
  System.setProperty("user.name", "hadoop")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  // 入参
  val hdfsWorkspace = ""
  val idType = ""
  val dimensions: Array[String] = Array[String](
  )
  val dataField: String = ""
  val sql = ""


  // 准备工作
  val batch = System.currentTimeMillis() + "_" + UUID.randomUUID().toString
  val cubes = MagicToolsBox.Balabala.createToolsItem(idType, dimensions)

  // spark初始化
  val classes = Array[Class[_]](classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[Put], classOf[ImmutableBytesWritable.Comparator])
  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.referenceTracking", "false")
    .registerKryoClasses(classes)

  val spark = SparkSession.builder()
    .enableHiveSupport()
    .config(sparkConf)
    .getOrCreate()
  val basic = spark.sql(sql).where(s"$dataField is not null")
    .select(dataField, dimensions: _*).distinct().persist(StorageLevel.MEMORY_AND_DISK_SER)
  cubes.par.foreach(MagicToolsBox.Balabala.buildFunctionCreator(hdfsWorkspace, dataField, basic, batch))


}



