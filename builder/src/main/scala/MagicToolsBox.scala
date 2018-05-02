import java.util.UUID

import com.rebiekong.bdt.mds.commons.HbaseUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{CompareFilter, Filter, FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.convert.wrapAll._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object MagicToolsBox {

  object Hbase {


    def getDataTableName(idType: String, dimensions: Array[String]): String = {
      // 在hbase中读取相应的信息
      val mDimensions = dimensions.sorted
      val hbaseConf = HBaseConfiguration.create
      val conn = ConnectionFactory.createConnection(hbaseConf)
      val metaTableName = TableName.valueOf("DETAIL_SEARCH".getBytes, "META_DATA".getBytes)
      val metaTable = conn.getTable(metaTableName)
      val scanner = metaTable.getScanner(
        new Scan().setFilter(HbaseUtils.getIdInfo(idType,dimensions.toList))
      )
      val meta = scanner.next()
      val tName = if (meta == null) {
        val rName = idType.toUpperCase() + "." + UUID.randomUUID().toString
        metaTable.put(
          new Put(UUID.randomUUID().toString.getBytes())
            .addColumn("META".getBytes(), "type".getBytes(), idType.getBytes())
            .addColumn("META".getBytes(), "dimension".getBytes(), String.join("|", mDimensions.toList).getBytes())
            .addColumn("META".getBytes(), "TABLE_NAME".getBytes(), rName.getBytes())
        )
        val tableDescriptor = new HTableDescriptor(new TableName("DETAIL_SEARCH",rName))
        import org.apache.hadoop.hbase.HColumnDescriptor
        tableDescriptor.addFamily(new HColumnDescriptor("DT"))

        conn.getAdmin.createTable(tableDescriptor)
        rName
      } else {
        new String(meta.getValue("META".getBytes(), "TABLE_NAME".getBytes()))
      }
      tName
    }


    def getFilter(idType: String, dimensions: Array[String]): Filter = {
      new FilterList(
        FilterList.Operator.MUST_PASS_ALL,
        new SingleColumnValueFilter(
          "META".getBytes(),
          "type".getBytes(),
          CompareFilter.CompareOp.EQUAL, idType.getBytes()
        ),
        new SingleColumnValueFilter(
          "META".getBytes(),
          "dimension".getBytes(),
          CompareFilter.CompareOp.EQUAL,
          String.join("|", dimensions.sorted.toList).getBytes()
        )
      )
    }
  }


  object CubeBuilder {
    def groupFields(fields: Array[String]): Seq[Seq[String]] = {
      val buffer = ArrayBuffer.empty[Seq[String]]
      for (i <- 1 to fields.length) {
        buffer.append(groupFields(fields, i): _*)
      }
      buffer
    }

    def groupFields(fields: Array[String], size: Int): Seq[Seq[String]] = {
      if (size == 1) {
        fields.map(s => List[String](s))
      } else {
        groupFields(fields, size - 1).flatten(item => {
          fields.map(s => {
            val tmp = item.toBuffer
            tmp.append(s)
            tmp.toList
          })
        }).filter(l => l.distinct.size == l.size).map(_.sorted).distinct
      }
    }
  }

  object StringTools {
    def md5(input: String): String = {
      DigestUtils.md5Hex(input)
    }
  }

  object Balabala {
    def createToolsItem(idType:String,dimensions:Array[String]): Seq[(Seq[String], String)] ={
      MagicToolsBox.CubeBuilder.groupFields(dimensions)
// 在此处可以进行剪枝
// 1. 必须存在某一个字段
//        .filter(_.contains("C"))
// 2. 保证A/B两字段间关系
//        .filter(i => {
//          i.contains("A") || (!i.contains("A") && !i.contains("B"))
//        })
        .sortBy(_.size).reverse.map(i => (i, {
        MagicToolsBox.Hbase.getDataTableName(idType, i.toArray)
      }))
    }

    def buildFunctionCreator(hdfsWorkspace: String, dataField: String, basic: DataFrame, batch: String): ((Seq[String], String)) => Unit = {
      (i: (Seq[String], String)) => {
        val hdfsOutputDir = hdfsWorkspace + "/" + i._2 + "." + System.currentTimeMillis()
        val p = basic.select(dataField, i._1: _*).distinct().repartition(i._1.map(c => new Column(c)): _*).rdd
          .groupBy(row => {
            MagicToolsBox.StringTools.md5(
              String.join("|", i._1.sorted.map(fieldName => row.getString(row.fieldIndex(fieldName))))
            )
          })
          .flatMap(item => {
            var data = item._2.map(row => row.getString(row.fieldIndex(dataField))).toList.sorted
            val dataBuffer = ArrayBuffer.empty[List[String]]
            while (data.size > 100) {
              val (a, b) = data.splitAt(100)
              dataBuffer.append(a)
              data = b
            }
            dataBuffer.append(data)
            val result = ListBuffer.empty[(ImmutableBytesWritable, KeyValue)]
            var ct = 0
            dataBuffer.foreach(i => {
              ct = ct + 1
              result.append((
                new ImmutableBytesWritable(item._1.getBytes()),
                new KeyValue(
                  item._1.getBytes(),
                  "DT".getBytes(),
                  (batch + "_" + String.format("%03d", new Integer(ct))).getBytes(),
                  String.join(",", i).getBytes()
                ))
              )
            })
            result
          }).sortByKey()

        val hbaseConf = HBaseConfiguration.create
        hbaseConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","1024")
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "DETAIL_SEARCH:" + i._2)
        val job = Job.getInstance
        job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setMapOutputValueClass(classOf[KeyValue])

        val conn = ConnectionFactory.createConnection(hbaseConf)
        val hbTableName = TableName.valueOf("DETAIL_SEARCH".getBytes, i._2.getBytes)
        val regionLocator = new HRegionLocator(hbTableName, conn.asInstanceOf[ClusterConnection])
        val realTable = conn.getTable(hbTableName)

        HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator)

        p.saveAsNewAPIHadoopFile(hdfsOutputDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)

        // bulk load start
        val loader = new LoadIncrementalHFiles(hbaseConf)
        val admin = conn.getAdmin
        loader.doBulkLoad(new Path(hdfsOutputDir), admin, realTable, regionLocator)
      }
    }
  }

}
