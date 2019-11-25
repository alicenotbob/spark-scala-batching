# specify list of kafka brokers 
export KAFKA_BOOTSTRAP_SERVER="sandbox-hdp.hortonworks.com:6667"

# specify name of topic where hotels/wheather data placed
export KAFKA_TOPIC="hotels_forecast"

# specify index of the last record in $KAFKA_TOPIC topic
export LAST_INDEX="63813003"

# specify root for HDFS
export HDFS_ROOT="hdfs://sandbox-hdp.hortonworks.com:8020/"

# specify path to Expedia dataset, stored in HDFS referenced by the HDFS_ROOT env var
# please note that you should omit root reference '/' in path (e.g. "root/data/expedia" but not "/root/data/expedia")
export EXPEDIA_PATH="user/uladzislau/expedia/"

# specify path to the folder where result set will be placed
export RESULT_PATH=$HDFS_ROOT"user/uladzislau/spark_batching_valid_expedia/"
