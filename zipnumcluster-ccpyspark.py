import os
import zlib
import json
import logging
from typing import Iterator, Tuple, List
from sparkcc import CCFileProcessorSparkJob
from pyspark import StorageLevel
from pyspark.sql.functions import row_number, concat, lit, col, min as min_, max as max_
from pyspark.sql.types import StringType, LongType, StructType, StructField
from pyspark.sql.window import Window

LOG = logging.getLogger('IndexWARCJob')

# note: this is LESS strict about partitioning than the original
# based on my read of the zipnum clustering code, this shoudl be just fine
# but so far, it's untested. I plan to test it with the index server we use (locally)

# some of these functions need to be serialized by spark, so, keep them outside of the class
# so we don't have issues with EMR serialization
def parse_line(line):
    try:
        parts = line.split(' ', 2)
        if len(parts) != 3:
            return None
        surt_key, timestamp, json_str = parts
        return (surt_key, (timestamp, json_str))
    except:
        return None

def get_partition_id(key: str, boundaries_data) -> int:
    """Determine partition based on range boundaries"""
    for i, boundary in enumerate(boundaries_data):
        if key < boundary:
            return i
    return len(boundaries_data)
    
class ZipNumClusterCdx(CCFileProcessorSparkJob):
    name = 'ZipNumClusterCdx'

    def add_arguments(self, parser):
        super(CCFileProcessorSparkJob,self).add_arguments(parser)
        parser.add_argument("--output_base_url", required=False,
                            default='my_cdx_bucket',
                            help="destination for output")
        parser.add_argument("--partition_boundries_file", required=False,
                            help="Full path to a json file containing partition boundaries. if specified, and does not exist, will be created, otherwise, will be used.")
        parser.add_argument("--num_lines", type=int, required=False,
                            default=3000,
                            help="number of lines to compress in each chunk")
        parser.add_argument("--num_output_partitions", type=int, required=False,
                            default=300,
                            help="number of partitions/shards")



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
                current_chunk = []
        
        with open(output_filename, 'rb') as fd:
            self.write_output_file(output_filename, fd, self.args.output_base_url)
        
        os.unlink(output_filename)

        return index_entries
    
    def run_job(self, session):
        os.makedirs(self.args.output_base_url, exist_ok=True)
        input = self.args.input_base_url + self.args.input
        num_partitions = self.args.num_output_partitions
        boundries_file_uri = self.args.partition_boundries_file


        rdd = session.sparkContext.textFile(input).map(parse_line).filter(lambda x: x is not None)

        # Cache the RDD with MEMORY_AND_DISK storage level
        #rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK)
        #rdd = rdd.cache()

        boundaries = None
        ##logging.info(f"Boundaries file: {boundries_file_uri}")
        if boundries_file_uri and self.check_for_output_file(boundries_file_uri):
            ##logging.info(f"Boundaries file found, using it: {boundries_file_uri}")
            with self.fetch_file(boundries_file_uri) as f:
                boundaries = json.load(f)
        else:
            ##logging.info(f"NO Boundaries file found, creating it: {boundries_file_uri}")
            samples = rdd.keys().sample(False, 0.1).collect()
            samples.sort()
            step = len(samples) // num_partitions
            boundaries = samples[::step][:num_partitions-1]
            
            temp_file_name = 'temp_range_boundaries.json'
            with open(temp_file_name, 'w') as f:
                json.dump(boundaries, f)
            
            with open(temp_file_name, 'rb') as f:
                self.write_output_file(boundries_file_uri, f)

            os.unlink(temp_file_name)
        
        
        
        # Process with range partitioning
        rdd = rdd.repartitionAndSortWithinPartitions(
            numPartitions=num_partitions,
            partitionFunc=lambda k: get_partition_id(k,boundaries),
            keyfunc=lambda x: x[0]) \
        .mapPartitionsWithIndex(self.process_partition) \
        .values()
        
        # EMR has issues with this...
        # rdd = rdd.persist(StorageLevel.MEMORY_AND_DISK)
        
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
        chunk_index_path = f"cluster.idx"
        with open(chunk_index_path, 'w') as f:
            seq = 1
            for row in index_df.collect():
                # Write min entry
                f.write(f"{row['min_surt']}\t{row['filename']}\t{row['offset']}\t{row['length']}\t{seq}\n")
                # Write max entry (was just for testing, we don't really need this in final index I don't think...)
                # f.write(f"{row['max_surt']}\t{row['filename']}\t{row['offset']}\t{row['length']}\t{row['sequence_number']}\n")
                seq += 1

        with open(chunk_index_path, 'rb') as fd:
            self.write_output_file(chunk_index_path, fd, self.args.output_base_url)
        
        os.unlink(chunk_index_path)
        

if __name__ == "__main__":
    job = ZipNumClusterCdx()
    job.run()