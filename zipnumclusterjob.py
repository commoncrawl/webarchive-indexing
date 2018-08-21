import logging
import shutil
import sys
import os

import zlib
import urlparse
import json

from collections import OrderedDict
from tempfile import TemporaryFile

from mrjob.job import MRJob
from mrjob.conf import combine_dicts
from mrjob.protocol import RawProtocol, RawValueProtocol
from mrjob.util import log_to_stream


LOG = logging.getLogger('ZipNumClusterJob')
log_to_stream(format="%(asctime)s %(levelname)s %(name)s: %(message)s",name='ZipNumClusterJob')


#=============================================================================
class ZipNumClusterJob(MRJob):
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.CombineTextInputFormat'

    PARTITIONER = 'org.apache.hadoop.mapred.lib.TotalOrderPartitioner'

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCOL = RawProtocol

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.output.fileoutputformat.compress': 'false',
                'mapreduce.job.reduce.slowstart.completedmaps': '1.0',
                'mapreduce.job.jvm.numtasks': '-1'
               }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(ZipNumClusterJob, self).configure_options()

        self.add_passthru_arg('--numlines', dest='numlines',
                              type=int,
                              default=3000,
                              help='Number of lines per gzipped block')

        self.add_passthru_arg('--splitfile', dest='splitfile',
                              help='Split file to use for CDX shard split')

        self.add_passthru_arg('--convert', dest='convert',
                              action='store_true',
                              default=False,
                              help='Convert CDX through _convert_line() function')

        self.add_passthru_arg('--shards', dest='shards',
                              type=int,
                              help='Num ZipNum Shards to create, ' +
                                   '= num of entries in splits + 1' +
                                   '= num of reducers used')

    def jobconf(self):
        orig_jobconf = super(ZipNumClusterJob, self).jobconf()
        custom_jobconf = {'mapreduce.job.reduces': self.options.shards,
                          'mapreduce.totalorderpartitioner.path': self.options.splitfile}

        combined = combine_dicts(orig_jobconf, custom_jobconf)
        return combined

    def mapper_init(self):
        pass

    def mapper(self, _, line):
        line = line.split('\t')[-1]
        if not line.startswith(' CDX'):
            if self.options.convert:
                line = self._convert_line(line)
            yield line, ''

    def _convert_line(self, line):
        if not ', "status": "304",' in line:
            return line
        key, ts, json_string = line.split(' ', 2)
        try:
            data = json.loads(json_string, object_pairs_hook=OrderedDict)
        except ValueError as e:
            logging.error('Failed to parse json: {0} - {1}'.format(
                e, json_string))
            return line
        if 'status' in data and data['status'] == '304':
            data['mime'] = 'warc/revisit'
            if 'digest' not in data:
                # insert before "length"
                item_list = []
                item_list.append(data.popitem())
                while item_list[0][0] != 'length':
                    item_list.insert(0, data.popitem())
                data['digest'] = '3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ'
                for item in item_list:
                    data[item[0]] = item[1]
            elif data['digest'] == None:
                data['digest'] = '3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ'
            if 'mime-detected' in data:
                del data['mime-detected']
            return ' '.join([key, ts, json.dumps(data)])
        else:
            return line

    def _get_prop(self, proplist):
        for p in proplist:
            res = os.environ.get(p)
            if res:
                return res

    def reducer_init(self):
        self.curr_lines = []
        self.curr_key = ''

        self.part_num = self._get_prop(['mapreduce_task_partition', 'mapred_task_partition'])
        assert(self.part_num)

        self.part_name = 'cdx-%05d.gz' % int(self.part_num)

        self.output_dir = self._get_prop(['mapreduce_output_fileoutputformat_outputdir',
                                          'mapred.output.dir',
                                          'mapred_work_output_dir'])

        assert(self.output_dir)
        self.gzip_temp = TemporaryFile(mode='w+b')

    def reducer(self, key, values):
        if key:
            self.curr_lines.append(key)

        for x in values:
            if x:
                self.curr_lines.append(x)

        if len(self.curr_lines) == 1:
            self.curr_key = ' '.join(key.split(' ', 2)[0:2])

        if len(self.curr_lines) >= self.options.numlines:
            yield '', self._write_part()

    def reducer_final(self):
        if len(self.curr_lines) > 0:
            yield '', self._write_part()

        self._do_upload()

    def _do_upload(self):
        self.gzip_temp.flush()
        self.gzip_temp.seek(0)
        #TODO: move to generalized put() function
        if self.output_dir.startswith('s3://') or self.output_dir.startswith('s3a://'):
            import boto3
            import botocore
            boto_config = botocore.client.Config(
                read_timeout=180,
                retries={'max_attempts' : 20})
            s3client = boto3.client('s3', config=boto_config)

            parts = urlparse.urlsplit(self.output_dir)
            s3key = parts.path.strip('/') + '/' + self.part_name
            s3url = parts.scheme + '://' + parts.netloc + '/' + s3key

            LOG.info('Uploading index to ' + s3url)
            try:
                s3client.upload_fileobj(self.gzip_temp, parts.netloc, s3key)
            except botocore.client.ClientError as exception:
                LOG.error('Failed to upload {}: {}'.format(s3url, exception))
                return
            LOG.info('Successfully uploaded index file: ' + s3url)
        else:
            path = os.path.join(self.output_dir, self.part_name)

            with open(path, 'w+b') as target:
                shutil.copyfileobj(self.gzip_temp, target)

        self.gzip_temp.close()

    def _write_part(self):
        z = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)

        offset = self.gzip_temp.tell()

        buff = '\n'.join(self.curr_lines) + '\n'
        self.curr_lines = []

        buff = z.compress(buff)
        self.gzip_temp.write(buff)

        buff = z.flush()
        self.gzip_temp.write(buff)
        self.gzip_temp.flush()

        length = self.gzip_temp.tell() - offset

        partline = '{0}\t{1}\t{2}\t{3}'.format(self.curr_key, self.part_name, offset, length)

        return partline

if __name__ == "__main__":
    ZipNumClusterJob.run()
