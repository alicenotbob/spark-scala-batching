docker cp target/scala-2.11/spark-batching_2.11-0.1.jar sandbox-hdp:/root/
docker cp envvars.sh sandbox-hdp:/root/
docker exec sandbox-hdp bash -c '. /root/envvars.sh && spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1,com.databricks:spark-avro_2.11:4.0.0 \
--master yarn \
--deploy-mode client \
--driver-memory 8g \
--class com.epam.bigdata.SparkBatching \
/root/spark-batching_2.11-0.1.jar'