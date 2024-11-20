import logging
from sparkcc import CCSparkJob
import os
from pyspark.sql.functions import row_number, concat, lit, col
import gzip
from typing import Iterator, Tuple, List
from pyspark.sql.types import StringType, LongType, StructType, StructField
import zlib
from pyspark.sql.window import Window
import random

LOG = logging.getLogger('IndexWARCJob')

class ZipNumClusterCdx(CCSparkJob):
    name = 'ZipNumClusterCdx'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument("--output_base_url", required=False,
                            default='my_cdx_bucket',
                            help="destination for output")
        parser.add_argument("--num_lines", type=int, required=False,
                            default=3000,
                            help="number of lines to compress in each chunk")
        parser.add_argument("--num_output_partitions", type=int, required=False,
                            default=300,
                            help="number of partitions/shards")
    
    def get_partition_boundaries(self, session, input_path: str, num_partitions: int) -> List[str]:
        """Use reservoir sampling to determine partition boundaries"""
        def reservoir_sample(iterator: Iterator[str], k: int) -> List[str]:
            sample = []
            for i, item in enumerate(iterator):
                if i < k:
                    sample.append(item)
                else:
                    j = random.randint(0, i)
                    if j < k:
                        sample[j] = item
            return sample

        # Collect samples and sort them
        samples = session.sparkContext.textFile(input_path) \
            .map(lambda line: line.split(" ", 1)[0]) \
            .mapPartitions(lambda x: reservoir_sample(x, 100)) \
            .collect()
        
        samples.sort()
        
        # Select evenly spaced samples as boundaries
        step = len(samples) // (num_partitions - 1)
        return [samples[i] for i in range(0, len(samples), step)][:num_partitions-1]

    def get_partition_id(self, key: str, boundaries: List[str]) -> int:
        """Determine partition based on range boundaries"""
        for i, boundary in enumerate(boundaries):
            if key < boundary:
                return i
        return len(boundaries)

    def process_partition(self, partition_id: int, partition_iter: Iterator[Tuple[str, Tuple[str, str, str]]]) -> Iterator[Tuple[str, str, int, int, int]]:
        """Process partition with chunked compression and first-entry-only indexing"""
        z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
        output_filename = f"cdx{partition_id}.gz"
        output_file = f"{self.args.output_base_url}/{output_filename}"
        index_entries = []
        current_offset = 0
        chunk_size = self.args.num_lines
        
        # Sort partition contents
        partition_data = sorted(partition_iter, key=lambda x: x[0])
        
        current_chunk = []
        first_record = None
        
        with open(output_file, 'wb') as f:
            for _, (surt_key, timestamp, json_data) in partition_data:
                line = f"{surt_key} {timestamp} {json_data}\n"
                if not first_record:
                    first_record = (surt_key, timestamp)
                current_chunk.append(line)
                
                if len(current_chunk) >= chunk_size:
                    # Compress and write chunk
                    chunk_data = ''.join(current_chunk).encode('utf-8')
                    z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
                    compressed = z.compress(chunk_data) + z.flush()
                    chunk_length = len(compressed)
                    f.write(compressed)
                    
                    # Only index the first entry of the chunk
                    if first_record:
                        index_entries.append((
                            first_record[0],  # surt_key
                            first_record[1],  # timestamp
                            partition_id,
                            current_offset,
                            chunk_length
                        ))
                    
                    current_offset += chunk_length
                    current_chunk = []
                    first_record = None
            
            # Handle final chunk
            if current_chunk:
                chunk_data = ''.join(current_chunk).encode('utf-8')
                z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
                compressed = z.compress(chunk_data) + z.flush()
                chunk_length = len(compressed)
                f.write(compressed)
                
                if first_record:
                    index_entries.append((
                        first_record[0],
                        first_record[1],
                        partition_id,
                        current_offset,
                        chunk_length
                    ))
        
        return index_entries

    def run_job(self, session):
        os.makedirs(self.args.output_base_url, exist_ok=True)
        input = self.args.input_base_url + self.args.input
        num_partitions = self.args.num_output_partitions

        # Get partition boundaries using reservoir sampling
        boundaries = self.get_partition_boundaries(session, input, num_partitions)

        # Process with range partitioning
        rdd = session.sparkContext.textFile(input) \
            .map(lambda line: tuple(line.strip().split(" ", 2))) \
            .keyBy(lambda x: x[0]) \
            .partitionBy(num_partitions, 
                        partitionFunc=lambda key: self.get_partition_id(key, boundaries)) \
            .mapPartitionsWithIndex(self.process_partition)

        # Create index
        index_schema = StructType([
            StructField("surt_key", StringType(), False),
            StructField("timestamp", StringType(), False),
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
        
        # Save main index, sorted by surt_key for binary search
        index_df.sort("surt_key").coalesce(1).write \
        .option("sep", "\t").csv(
            f"{self.args.output_base_url}/index.idx", 
            header=False,
            mode="overwrite"
        )

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