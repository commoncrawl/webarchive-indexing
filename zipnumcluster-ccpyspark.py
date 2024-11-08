import logging
from sparkcc import CCSparkJob
import os
from pyspark.sql.functions import row_number, concat, lit, col
import gzip
from typing import Iterator, Tuple, List
from pyspark.sql.types import StringType, LongType, StructType, StructField
import zlib
from pyspark.sql.window import Window

LOG = logging.getLogger('IndexWARCJob')

# note: this is LESS strict about partitioning than the original
# based on my read of the zipnum clustering code, this shoudl be just fine
# but so far, it's untested. I plan to test it with the index server we use (locally)

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
    
    def get_partition_id(self, surt_key: str, num_partitions: int) -> int:
        """
        Determine partition based on SURT key structure.
        Handles special cases like common TLD prefixes.
        """
        # Split SURT key into components
        parts = surt_key.split(',')
        
        # Handle special cases for domain-based SURT keys
        if len(parts) > 1:
            # Skip common TLDs for better distribution
            if parts[0] in {'com', 'org', 'net', 'edu', 'gov'}:
                key_for_hash = parts[1]
            else:
                key_for_hash = parts[0]
        else:
            # Handle non-domain SURT keys (like IP addresses)
            key_for_hash = parts[0]
        
        # Take first 3 meaningful characters for distribution
        prefix = key_for_hash[:3].ljust(3)
        
        # Create a number from the characters that preserves ordering
        # This ensures similar prefixes go to nearby partitions
        value = (ord(prefix[0]) << 16) + (ord(prefix[1]) << 8) + ord(prefix[2])
        
        return value % num_partitions
    
    def run_job(self, session):
        os.makedirs(self.args.output_base_url, exist_ok=True)
        input = self.args.input_base_url + self.args.input
        num_partitions = self.args.num_output_partitions

        def process_partition(partition_id: int, partition_iter: Iterator[Tuple[str, Tuple[str, str, str]]]) -> Iterator[Tuple[str, int, int, int]]:
            """Process partition with chunked compression"""
            z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
            output_filename = f"cdx{partition_id}.gz"
            output_file = f"{self.args.output_base_url}/{output_filename}"
            index_entries = []
            current_offset = 0
            chunk_size = self.args.num_lines
            
            # Sort partition contents
            partition_data = sorted(partition_iter, key=lambda x: x[0])
            
            current_chunk = []
            chunk_records = []  # Store full record info
            
            with open(output_file, 'wb') as f:
                for _, (surt_key, timestamp, json_data) in partition_data:
                    line = f"{surt_key} {timestamp} {json_data}\n"
                    current_chunk.append(line)
                    chunk_records.append((surt_key, timestamp))  # Store both surt_key and timestamp
                    
                    if len(current_chunk) >= chunk_size:
                        # Compress and write chunk
                        chunk_data = ''.join(current_chunk).encode('utf-8')
                        compressed = z.compress(chunk_data)
                        chunk_length = len(compressed)
                        f.write(compressed)
                        
                        # Create single index entry per record
                        for sk, ts in chunk_records:
                            index_entries.append((sk, ts, partition_id, current_offset, chunk_length))
                        
                        current_offset += chunk_length
                        current_chunk = []
                        chunk_records = []
                    
                # Handle final chunk
                if current_chunk:
                    chunk_data = ''.join(current_chunk).encode('utf-8')
                    compressed = z.compress(chunk_data) + z.flush()
                    chunk_length = len(compressed)
                    f.write(compressed)
                    
                    for sk, ts in chunk_records:
                        index_entries.append((sk, ts, partition_id, current_offset, chunk_length))
            
            return index_entries

        # Single pass processing with fixed-width partitioning
        rdd = session.sparkContext.textFile(input) \
            .map(lambda line: tuple(line.strip().split(" ", 2))) \
            .keyBy(lambda x: x[0]) \
            .partitionBy(num_partitions, 
                        partitionFunc=lambda key: self.get_partition_id(key, num_partitions)) \
            .mapPartitionsWithIndex(process_partition)

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