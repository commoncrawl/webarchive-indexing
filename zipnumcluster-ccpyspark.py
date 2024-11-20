import logging
from sparkcc import CCSparkJob
import os
from pyspark.sql.functions import row_number, concat, lit, col, min as min_, max as max_
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
        if num_partitions > 1:
            if len(samples) < num_partitions:
                # If we have fewer samples than requested partitions, use all samples
                return samples[:-1]  # exclude last sample to ensure num_partitions-1 boundaries
            step = max(1, len(samples) // (num_partitions - 1))
            return [samples[i] for i in range(0, len(samples), step)][:num_partitions-1]
        else:
            return samples

    def get_partition_id(self, key: str, boundaries: List[str]) -> int:
        """Determine partition based on range boundaries"""
        for i, boundary in enumerate(boundaries):
            if key < boundary:
                return i
        return len(boundaries)

    def process_partition(self, partition_id: int, partition_iter: Iterator[Tuple[str, Tuple[str, str, str]]]) -> Iterator[Tuple[str, str, str, str, int, int, int]]:
        """Process partition with chunked compression and chunk boundary tracking"""
        output_filename = f"cdx-{partition_id:05d}.gz"
        output_file = f"{self.args.output_base_url}/{output_filename}"
        index_entries = []
        current_offset = 0
        chunk_size = self.args.num_lines
        
        # Sort partition contents
        partition_data = sorted(partition_iter, key=lambda x: x[0])
        
        current_chunk = []
        chunk_min_surt = None
        chunk_max_surt = None
        
        with open(output_file, 'wb') as f:
            for _, (surt_key, timestamp, json_data) in partition_data:
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
                    
                    # Index entry with chunk boundaries
                    index_entries.append((
                        chunk_min_surt,  # min surt
                        chunk_max_surt,  # max surt
                        output_filename,  # filename
                        partition_id,
                        current_offset,
                        chunk_length,
                        len(current_chunk)  # number of records in chunk
                    ))
                    
                    current_offset += chunk_length
                    current_chunk = []
                    chunk_min_surt = None
            
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

        # Update schema for new index format
        index_schema = StructType([
            StructField("min_surt", StringType(), False),
            StructField("max_surt", StringType(), False),
            StructField("filename", StringType(), False),
            StructField("partition_id", LongType(), False),
            StructField("offset", LongType(), False),
            StructField("length", LongType(), False),
            StructField("num_records", LongType(), False)
        ])
        
        index_df = session.createDataFrame(rdd, index_schema).orderBy("min_surt")
        
        # Write chunk-level index
        chunk_index_path = f"{self.args.output_base_url}/cluster.idx"
        with open(chunk_index_path, 'w') as f:
            seq = 1
            for row in index_df.collect():
                # Write min entry
                f.write(f"{row['min_surt']}\t{row['filename']}\t{row['offset']}\t{row['length']}\t{seq}\n")
                # Write max entry (was just for testing, we don't really need this in final index I don't think...)
                # f.write(f"{row['max_surt']}\t{row['filename']}\t{row['offset']}\t{row['length']}\t{row['sequence_number']}\n")
                seq += 1

if __name__ == "__main__":
    job = ZipNumClusterCdx()
    job.run()