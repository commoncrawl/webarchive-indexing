import argparse
import json
import logging
import os
import re
import zlib

from typing import Iterator, Tuple

import boto3
import botocore


from sparkcc import CCFileProcessorSparkJob


class ZipNumClusterCdx(CCFileProcessorSparkJob):
    """Spark job to create a ZipNum Sharded CDX index, see
    <https://github.com/webrecorder/pywb/wiki/CDX-Index-Format#zipnum-sharded-cdx>.
    The index is sharded over multiple partitions (default = 300). Each partition file
    is compressed using gzip, but in chunks of 3000 lines (a configurable number).
    Every chunk can be read separately, a jump index allows to find the right chunk
    for a given key in a binary search.
    """

    name = 'ZipNumClusterCdx'

    input_descr = """Glob pattern of input CDX files, e.g., file:///path/*/*.cdx.gz
(HDFS-compatible filesystems only: hdfs://, s3a://, file://)."""
    output_descr = "Ignored but required (can be empty, no output table is produced)."

    DATA_URL_PATTERN = re.compile('^(s3|https?|file|hdfs|s3a|s3n):(?://([^/]*))?/(.*)')


    def add_arguments(self, parser):
        super(CCFileProcessorSparkJob,self).add_arguments(parser)
        parser.add_argument("--output_base_url", required=True,
                            help="Output destination.")
        parser.add_argument("--partition_boundaries_file", required=True,
                            help="Full path to a JSON file containing partition boundaries. "
                            "If specified, and does not exist, will be created, otherwise, "
                            "it will be used.")
        parser.add_argument("--temporary_output_base_url", required=True,
                            help="Temporary output location for per-shard cluster indexes.")
        parser.add_argument("--num_lines", type=int, required=False,
                            default=3000,
                            help="Number of lines to compress in each chunk")
        parser.add_argument("--num_output_partitions", type=int, required=False,
                            default=300,
                            help="Number of partitions/shards")
        # suppress help for ignored arguments
        parser.add_argument("--output_format", help=argparse.SUPPRESS)
        parser.add_argument("--output_compression", help=argparse.SUPPRESS)
        parser.add_argument("--output_option", help=argparse.SUPPRESS)

    @staticmethod
    def parse_line(line):
        try:
            parts = line.split(' ', 2)
            if len(parts) != 3:
                return None
            surt_key, timestamp, json_str = parts
            return ((surt_key, timestamp), json_str)
        except:
            return None

    @staticmethod
    def get_partition_id(key: str, boundaries_data) -> int:
        """Determine partition based on range boundaries"""
        if not boundaries_data:
            return 0

        # Binary search to find the right partition
        left = 0
        right = len(boundaries_data)

        while left < right:
            mid = (left + right) // 2
            if mid == len(boundaries_data):
                return mid
            if key <= boundaries_data[mid]:
                right = mid
            else:
                left = mid + 1

        return left

    @staticmethod
    def write_output_file(uri, fd, base_uri=None):
        """
        Write data from stream fd to output file location defined per URI.
        A static variant of CCFileProcessorSparkJob.write_output_file(...)
        """
        uri_match = ZipNumClusterCdx.DATA_URL_PATTERN.match(uri)
        if not uri_match and base_uri:
            # relative input URI (path) and base URI defined
            uri = base_uri + uri
            uri_match = ZipNumClusterCdx.DATA_URL_PATTERN.match(uri)
        if uri_match:
            (scheme, netloc, path) = uri_match.groups()
        else:
            # keep local file paths as is
            path = uri
            scheme = 'file'
            netloc = None

        if scheme in {'s3', 's3a', 's3n'}:
            bucketname = netloc
            output_path = path
            try:
                client = boto3.client('s3')
                client.upload_fileobj(fd, bucketname, path)
            except botocore.client.ClientError as exception:
                logging.error(
                    'Failed to write to S3 {}: {}'.format(output_path, exception))

        elif scheme in {'http', 'https'}:
            raise ValueError('HTTP/HTTPS output not supported')

        elif scheme == 'hdfs':
            raise NotImplementedError('HDFS output not implemented')

        else:
            logging.info('Writing local file {}'.format(uri))
            if scheme == 'file':
                # must be an absolute path
                uri = os.path.join('/', path)
            else:
                base_dir = os.path.abspath(os.path.dirname(__file__))
                uri = os.path.join(base_dir, uri)
            os.makedirs(os.path.dirname(uri), exist_ok=True)
            with open(uri, 'wb') as f:
                f.write(fd.read())

    @staticmethod
    def write_partition_with_global_seq(idx: int, partition_iter: list,
                                        records_per_partition: int, output_base_url: str):
        partition_idx_file = f"idx-{idx:05d}.idx"

        # Calculate starting sequence number for this partition
        start_seq = (idx * records_per_partition) + 1 if records_per_partition else 1

        with open(partition_idx_file, 'w', encoding="utf-8") as f:
            seq = start_seq
            for record in partition_iter:
                min_surt, _, min_surt_timestamp, filename, _, offset, length, _ = record
                f.write(f"{min_surt} {min_surt_timestamp}\t{filename}\t{offset}\t{length}\t{seq}\n")
                seq += 1

        with open(partition_idx_file, 'rb') as fd:
            ZipNumClusterCdx.write_output_file(partition_idx_file, fd, output_base_url)

        os.unlink(partition_idx_file)

        return [(partition_idx_file, True)]

    @staticmethod
    def process_partition(partition_id: int, partition_iter: Iterator[Tuple[str, Tuple[str, str]]],
                          num_lines: int, output_base_url: str, temporary_output_base_url: str) \
                          -> Iterator[Tuple[str, str, str, str, int, int, int, int]]:
        """Process partition with chunked compression and chunk boundary tracking"""
        output_filename = f"cdx-{partition_id:05d}.gz"
        index_entries = []
        current_offset = 0
        chunk_size = num_lines

        current_chunk = []
        chunk_min_surt = None
        chunk_max_surt = None
        chunk_min_timestamp = None

        with open(output_filename, 'wb') as f:
            for (surt_key, timestamp), json_data in partition_iter:
                line = f"{surt_key} {timestamp} {json_data}\n"
                if chunk_min_surt is None:
                    chunk_min_surt = surt_key
                    chunk_min_timestamp = timestamp
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
                        str(chunk_min_surt),   # min surt
                        str(chunk_max_surt),   # max surt
                        str(chunk_min_timestamp), # capture time
                        str(output_filename),  # filename
                        int(partition_id),     # explicit integer conversion
                        int(current_offset),   # explicit integer conversion
                        int(chunk_length),     # explicit integer conversion
                        int(len(current_chunk))  # number of records in chunk
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
                    str(chunk_min_surt),  # min surt
                    str(chunk_max_surt),  # max surt
                    str(chunk_min_timestamp), # capture time
                    str(output_filename),
                    int(partition_id),
                    int(current_offset),
                    int(chunk_length),
                    int(len(current_chunk))
                ))
                current_chunk = []

        with open(output_filename, 'rb') as fd:
            ZipNumClusterCdx.write_output_file(output_filename, fd, output_base_url)

        os.unlink(output_filename)

        final_files = ZipNumClusterCdx.write_partition_with_global_seq(
            partition_id, index_entries, num_lines, temporary_output_base_url)

        return final_files

    def run_job(self, session):
        input_url = self.args.input_base_url + self.args.input
        num_partitions = self.args.num_output_partitions
        boundaries_file_uri = self.args.partition_boundaries_file
        num_lines = self.args.num_lines
        output_base_url = self.args.output_base_url
        temporary_output_base_url = self.args.temporary_output_base_url

        rdd = session.sparkContext.textFile(input_url).map(
            self.parse_line).filter(lambda x: x is not None)

        boundaries = None
        self.get_logger(session).info(f"Boundaries file: {boundaries_file_uri}")
        if boundaries_file_uri and self.check_for_output_file(boundaries_file_uri):
            self.get_logger(session).info(f"Boundaries file found, using it: {boundaries_file_uri}")
            with self.fetch_file(boundaries_file_uri) as f:
                boundaries = list(map(lambda l: tuple(l), json.load(f)))

        else:
            # The percentage needs to be pretty small, since the collect
            # brings data back to the driver...
            # 1/2 percent should be fine
            samples = rdd.keys().sample(False, 0.005).collect()
            samples.sort()

            # Ensure more even distribution by using quantiles
            total_samples = len(samples)
            boundaries = []
            for i in range(1, num_partitions):
                idx = (i * total_samples) // num_partitions
                if idx < len(samples):
                    boundaries.append(samples[idx])

            temp_file_name = 'temp_range_boundaries.json'
            with open(temp_file_name, 'w', encoding="utf-8") as f:
                json.dump(boundaries, f)

            with open(temp_file_name, 'rb') as f:
                self.write_output_file(boundaries_file_uri, f)

            os.unlink(temp_file_name)

            self.get_logger(session).info(
                f"Boundaries file created: {boundaries_file_uri}")

        rdd = rdd.repartitionAndSortWithinPartitions(
            numPartitions=num_partitions,
            partitionFunc=lambda k: ZipNumClusterCdx.get_partition_id(k, boundaries)) \
            .mapPartitionsWithIndex(
                lambda idx, iter: ZipNumClusterCdx.process_partition(
                    idx, iter, num_lines, output_base_url, temporary_output_base_url)) \
            .collect()

        # loop over the output files and concatenate them into a single final file
        with open('cluster.idx', 'wb') as f:
            for idx_file, _ in rdd:
                with self.fetch_file(temporary_output_base_url + idx_file) as idx_fd:
                    for line in idx_fd:
                        f.write(line)

        with open('cluster.idx', 'rb') as f:
            self.write_output_file('cluster.idx', f, output_base_url)

        os.unlink('cluster.idx')


if __name__ == "__main__":
    job = ZipNumClusterCdx()
    job.run()
