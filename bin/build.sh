#!/usr/bin/env bash
HBASE_LIB=`find ${HBASE_HOME}/lib -name  '*.jar' | xargs | sed  "s/ /,/g"`
export LD_LIBRARY_PATH=${HADOOP_COMMON_LIB_NATIVE_DIR}:${LD_LIBRARY_PATH};
export SPARK_HOME=/usr/local/spark-2.3.0-bin-hadoop2.7
export HADOOP_CONF_DIR=/works/hadoop_conf_dir
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode cluster \
--executor-memory 4g \
--conf spark.dynamicAllocation.minExecutors=0 \
--conf spark.dynamicAllocation.maxExecutors=15 \
--conf spark.dynamicAllocation.initialExecutors=1 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.shuffle.service.enabled=true \
--driver-class-path /hadoop/jars/mysql-connector-java-5.1.38.jar \
--jars /hadoop/jars/mysql-connector-java-5.1.38.jar,${HBASE_LIB} \
--files ${HIVE_HOME}/conf/hive-site.xml,${HBASE_HOME}/conf/hbase-site.xml \
--class MDSBuilder \
builder-1.0-SNAPSHOT.jar