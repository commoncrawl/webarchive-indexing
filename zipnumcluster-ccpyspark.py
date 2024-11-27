import logging
from sparkcc import CCFileProcessorSparkJob
import os
from pyspark.sql.functions import row_number, concat, lit, col, min as min_, max as max_
import gzip
from typing import Iterator, Tuple, List
from pyspark.sql.types import StringType, LongType, StructType, StructField
import zlib
from pyspark.sql.window import Window
import random
import pickle
from pyspark import StorageLevel

LOG = logging.getLogger('IndexWARCJob')

# TODO: WE USE CCFileProcessorSparkJob here only for write_output_file, we should probably move write_output_file to CCSparkJob instead.
# It's OK for this one, because we override the entire run_job method, but it's not ideal, because we're not really doing "file-wise" processing here...

class ZipNumClusterCdx(CCFileProcessorSparkJob):
    name = 'ZipNumClusterCdx'

    def add_arguments(self, parser):
        super(CCFileProcessorSparkJob,self).add_arguments(parser)
        parser.add_argument("--output_base_url", required=False,
                            default='my_cdx_bucket',
                            help="destination for output")
        parser.add_argument("--partition_boundries_file", required=False,
                            help="Full path to a file containing partition boundaries. if specified, and does not exist, will be created, otherwise, will be used.")
        parser.add_argument("--num_lines", type=int, required=False,
                            default=3000,
                            help="number of lines to compress in each chunk")
        parser.add_argument("--num_output_partitions", type=int, required=False,
                            default=300,
                            help="number of partitions/shards")

    def get_partition_id(self, key: str, boundaries: List[str]) -> int:
        """Determine partition based on range boundaries"""
        for i, boundary in enumerate(boundaries):
            if key < boundary:
                return i
        return len(boundaries)

    def process_partition(self, partition_id: int, partition_iter: Iterator[Tuple[str, Tuple[str, str]]]) -> Iterator[Tuple[str, str, str, str, int, int, int]]:
        """Process partition with chunked compression and chunk boundary tracking"""
        output_filename = f"cdx-{partition_id:05d}.gz"
        index_entries = []
        current_offset = 0
        chunk_size = self.args.num_lines
        
        current_chunk = []
        chunk_min_surt = None
        chunk_max_surt = None
        
        with open(output_filename, 'wb') as f:
            for surt_key, (timestamp, json_data) in partition_iter:
                line = f"{surt_key} {timestamp} {json_data}\n"
                if chunk_min_surt is None:
                    chunk_min_surt = surt_key
                chunk_max_surt = surt_key  # Will end up as max since data is sorted
                current_chunk.append(line)
                
                if len(current_chunk) >= chunk_size:
                    # Compress and write chunk
                    chunk_data = ''.join(current_chunk).encode('utf-8')
                    z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
                    compressed = z.compress(chunk_data) + z.flush()
                    chunk_length = len(compressed)
                    f.write(compressed)
                    
                    for sk, ts in chunk_records:
                        index_entries.append((sk, ts, partition_id, current_offset, chunk_length))
            
            # Handle final chunk
            if current_chunk:
                chunk_data = ''.join(current_chunk).encode('utf-8')
                z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
                compressed = z.compress(chunk_data) + z.flush()
                chunk_length = len(compressed)
                f.write(compressed)
                
                index_entries.append((
                    chunk_min_surt,
                    chunk_max_surt,
                    output_filename,
                    partition_id,
                    current_offset,
                    chunk_length,
                    len(current_chunk)
                ))
        
        with open(output_filename, 'rb') as fd:
            self.write_output_file(output_filename, fd, self.args.output_base_url)
        
        os.unlink(output_filename)

        return index_entries

    def parse_line(_, line):
        try:
            parts = line.split(' ', 2)
            if len(parts) != 3:
                return None
            surt_key, timestamp, json_str = parts
            return (surt_key, (timestamp, json_str))
        except:
            return None
    
    def run_job(self, session):
        os.makedirs(self.args.output_base_url, exist_ok=True)
        input = self.args.input_base_url + self.args.input
        num_partitions = self.args.num_output_partitions
        boundries_file_uri = self.args.partition_boundries_file


        rdd = session.sparkContext.textFile(input).map(self.parse_line).filter(lambda x: x is not None)

        # Cache the RDD with MEMORY_AND_DISK storage level
        rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK)

        boundaries = None
        if boundries_file_uri and self.check_for_output_file(boundries_file_uri):
            with self.fetch_file(boundries_file_uri) as f:
                boundaries = pickle.load(f)
        else:
            samples = rdd.keys().sample(False, 0.1).collect()
            samples.sort()
            step = len(samples) // num_partitions
            boundaries = samples[::step][:num_partitions-1]
            
            temp_file_name = 'temp_range_boundaries.pkl'
            with open(temp_file_name, 'wb') as f:
                pickle.dump(boundaries, f)
            
            with open(temp_file_name, 'rb') as f:
                self.write_output_file(boundries_file_uri, f)

            os.unlink(temp_file_name)
        
        # Process with range partitioning
        rdd = rdd.keyBy(lambda x: x[0]) \
            .partitionBy(num_partitions, lambda k: self.get_partition_id(k, boundaries)) \
            .sortByKey() \
            .values()
        
        rdd = rdd.mapPartitionsWithIndex(self.process_partition)

        # Update schema for new index format
        index_schema = StructType([
            StructField("min_surt", StringType(), False),
            StructField("max_surt", StringType(), False),
            StructField("filename", StringType(), False),
            StructField("partition_id", LongType(), False),
            StructField("offset", LongType(), False),
            StructField("length", LongType(), False)
        ])
        
        w = Window.orderBy("surt_key")
        # Create index with correct filename formatting
        index_df = session.createDataFrame(rdd, index_schema)\
            .withColumn("sequence_number", row_number().over(w))

        index_df = index_df\
            .withColumn("output_filename", concat(lit("cdx"), col("partition_id").cast(StringType()), lit(".gz")))\
            .select("surt_key", "timestamp", "output_filename", "offset", "length", "sequence_number")

        # Create secondary index for partition boundaries
        partition_bounds = index_df.groupBy("output_filename") \
            .agg({"surt_key": "min", "surt_key": "max"}) \
            .sort("output_filename")
        
        partition_bounds.coalesce(1).write \
        .option("sep", "\t").csv(
            f"{self.args.output_base_url}/secondary_index.idx",
            header=False,
            mode="overwrite"
        )

if __name__ == "__main__":
    job = ZipNumClusterCdx()
    job.run()