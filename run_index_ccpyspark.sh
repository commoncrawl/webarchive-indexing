#!/bin/bash

if [ $# -lt 2 ]; then
    cat <<"EOF"
$0 <year-week-of-crawl> <path-to-warc-file-list> [<split_file>]

Create a Common Crawl CDX ZipNum index for a monthly crawl. All steps are run on Spark.

  <year-week-of-crawl>   Year and week of the monthly crawl to be indexed, e.g. 2016-44
                         used to determine the final location of the index
                             s3://commoncrawl/cc-index/collections/CC-MAIN-2016-44/...
                         Also locations for temporary files include the crawl name.

  <path-to-warc-file-list>  List of WARC file objects to be indexed, e.g, the WARC/WAT/WET list
                               s3://commoncrawl/crawl-data/CC-MAIN-2016-44/warc.paths
                         or any subset or union of multiple WARC listings (incl. robots.txt WARCs).
                         Paths in the list must be keys/objects in the Common Crawl bucket
                         or another bucket configured in this script (WARC_PREFIX).
                         The path to the list must be an absolute URL on HDFS or S3A.

                         The "index warcs" step is skipped if an empty string is passed as argument.
                         Since 2018 the per-WARC CDX files are written directly by the Fetcher
                         and include index fields combined from the WARC response and metadata record.
                         The latter holds the detected language and charset.

  <split_file>           Optional split file to be reused from previous crawl with similar distribution of URLs.
                         If not given, splits are calculated and saved on the default split file path.

EOF
    exit 1
fi


YEARWEEK="$1"
WARC_MANIFEST="$2"
REUSE_SPLIT_FILE="$3"

CRAWL="CC-MAIN-$YEARWEEK"

echo "Generating cc-index for $CRAWL"
echo
echo WARC_MANIFEST="$WARC_MANIFEST"
echo

# Path prefix of WARC/WAT/WET files listed in WARC_MANIFEST
WARC_PREFIX="s3://commoncrawl/"

# AWS S3 bucket to hold CDX files
WARC_CDX_BUCKET="commoncrawl-index-temp"
WARC_CDX_PREFIX="s3://$WARC_CDX_BUCKET/"

# Location of the CDX status table
SPARK_SQL_WAREHOUSE="s3a://$WARC_CDX_BUCKET/$CRAWL"
CDX_STATUS_TABLE="cdx_status"


# glob pattern to match all CDX files generated in step 1 (indexwarcs_cc_pyspark.py)
# or available otherwise. The URI scheme must be supported by Hadoop / HDFS.
WARC_CDX="s3a://$WARC_CDX_BUCKET/$CRAWL/cdx/segments/*/*/*.cdx.gz"


### ZipNum definitions
ZIPNUM_N_LINES=3000
ZIPNUM_N_PARTITIONS=300

# SPLIT_FILE could be reused from previous crawl with similar distribution of URLs, see REUSE_SPLIT_FILE
SPLIT_FILE="s3a://$WARC_CDX_BUCKET/$CRAWL/partition_boundaries.json"
# if explicitely configured
if [ -n "$REUSE_SPLIT_FILE" ]; then
    echo "Reusing SPLIT_FILE $REUSE_SPLIT_FILE"
    SPLIT_FILE="$REUSE_SPLIT_FILE"
fi

# temporary output path of part-n files of the zipnum job, concatenated into the cluster.idx
ZIPNUM_TEMP_DIR="s3://$WARC_CDX_BUCKET/$CRAWL/indexes/"

# final path to ZipNum index files
ZIPNUM_CLUSTER_DIR="s3://commoncrawl/cc-index/collections/$CRAWL/indexes/"


# configure S3 buffer directory
# - must exist on task/compute nodes for buffering data
# - should provide several GBs of free space to hold temporarily
#   the downloaded data (WARC, WAT, WET files to be indexed),
#   only relevant for the indexwarcs_cc_pyspark job.
if [ -n "$S3_LOCAL_TEMP_DIR" ]; then
	S3_LOCAL_TEMP_DIR="--local_temp_dir=$S3_LOCAL_TEMP_DIR"
else
	S3_LOCAL_TEMP_DIR=""
fi



### PySpark definitions
export PYSPARK_PYTHON="python"  # or "python3"

# Python dependencies (for simplicity, include all Python files: cc-pyspark/*.py)
PYFILES=sparkcc.py

### Spark configuration

SPARK_ON_YARN="--master yarn"
SPARK_HADOOP_OPTS=""
SPARK_EXTRA_OPTS=""

# defines SPARK_HOME, SPARK_HADOOP_OPTS and HADOOP_CONF_DIR
. spark_env.sh

NUM_EXECUTORS=${NUM_EXECUTORS:-1}
EXECUTOR_CORES=${EXECUTOR_CORES:-2}
# input partitions for the WARC-to-CDX stop
NUM_WARC_INPUT_PARTITIONS=${NUM_WARC_INPUT_PARTITIONS:-10}

export LC_ALL=C

set -e
set -x


if [ -n "$WARC_MANIFEST" ]; then
    # Index WARC files in the manifest, write one CDX file per WARC
    EXECUTOR_MEM=${EXECUTOR_MEM:-2g}
    if [[ $NUM_WARC_INPUT_PARTITIONS -lt $((NUM_EXECUTORS*EXECUTOR_CORES)) ]]; then
        echo "The number of input partitions is too low to utilize all executor cores"
        exit 1
    fi
    $SPARK_HOME/bin/spark-submit \
        $SPARK_ON_YARN \
        $SPARK_HADOOP_OPTS \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.task.maxFailures=5 \
        --conf spark.executor.memory=$EXECUTOR_MEM \
        --conf spark.driver.memory=3g \
        --conf spark.core.connection.ack.wait.timeout=600s \
        --conf spark.network.timeout=300s \
        --conf spark.shuffle.io.maxRetries=50 \
        --conf spark.shuffle.io.retryWait=600s \
        --conf spark.locality.wait=1s \
        --conf spark.executorEnv.LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native \
        --num-executors $NUM_EXECUTORS \
        --executor-cores $EXECUTOR_CORES \
        --executor-memory $EXECUTOR_MEM \
        --conf spark.sql.warehouse.dir="$SPARK_SQL_WAREHOUSE" \
        --py-files $PYFILES \
        indexwarcs_cc_pyspark.py \
        --input_base_url="$WARC_PREFIX" \
        --output_base_url="$WARC_CDX_PREFIX" \
        $S3_LOCAL_TEMP_DIR \
        --num_input_partitions=$NUM_WARC_INPUT_PARTITIONS \
        --num_output_partitions=1 \
        "$WARC_MANIFEST" "$CDX_STATUS_TABLE"
fi


### Create ZipNum index
EXECUTOR_MEM=${EXECUTOR_MEM:-3g}

$SPARK_HOME/bin/spark-submit \
    $SPARK_ON_YARN \
    $SPARK_HADOOP_OPTS \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.task.maxFailures=5 \
    --conf spark.executor.memory=$EXECUTOR_MEM \
    --conf spark.driver.memory=3g \
    --conf spark.core.connection.ack.wait.timeout=600s \
    --conf spark.network.timeout=300s \
    --conf spark.shuffle.io.maxRetries=50 \
    --conf spark.shuffle.io.retryWait=600s \
    --conf spark.locality.wait=1s \
    --conf spark.io.compression.codec=zstd \
    --conf spark.checkpoint.compress=true \
    --conf spark.executorEnv.LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native \
    --num-executors $NUM_EXECUTORS \
    --executor-cores $EXECUTOR_CORES \
    --executor-memory $EXECUTOR_MEM \
    --py-files $PYFILES \
    zipnumcluster_cc_pyspark.py \
    $S3_LOCAL_TEMP_DIR \
    --input_base_url="" \
    --output_base_url="$ZIPNUM_CLUSTER_DIR" \
    --temporary_output_base_url="$ZIPNUM_TEMP_DIR" \
    --partition_boundaries_file="$SPLIT_FILE" \
    --num_lines=$ZIPNUM_N_LINES \
    --num_output_partitions=$ZIPNUM_N_PARTITIONS \
    "$WARC_CDX" ""


