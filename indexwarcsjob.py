import logging
import sys

from datetime import datetime

import boto3
import botocore

from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from mrjob.util import log_to_stream

from tempfile import TemporaryFile
from pywb.indexer.cdxindexer import write_cdx_index
from gzip import GzipFile


LOG = logging.getLogger('IndexWARCJob')
log_to_stream(format="%(asctime)s %(levelname)s %(name)s: %(message)s",
              name='IndexWARCJob')


### fix URL canonicalization function imported in pywb.indexer.archiveindexer
import pywb.utils.canonicalize
from pywb.utils.canonicalize import UrlCanonicalizeException
import surt
import re

def my_canonicalize(url, surt_ordered=True):
    """
    See canonicalize in pywb.utils.canonicalize.

    Changes: fix URLs which contain '%3F' instead '?' as separator before query
    """
    try:
        if '%3F' in url:
            sys.stderr.write('Fixing URL: ' + url + '\n')
            m = re.match('(https?://[^?]+)%3F(.*)', url)
            if m:
                sep = '?'
                if '/' not in m.group(1):
                    sep = '/?'
                fixed_url = m.group(1)
                if len(m.group(2)) > 0:
                    fixed_url += sep + m.group(2)
                sys.stderr.write('Fixed URL: {}\n        -> {}\n'.format(url, fixed_url))
                url = fixed_url

        key = surt.surt(url)
    except Exception as e:  #pragma: no cover
        # doesn't happen with surt from 0.3b
        # urn is already canonical, so just use as-is
        if url.startswith('urn:'):
            return url

        raise UrlCanonicalizeException('Invalid Url: ' + url)

    # if not surt, unsurt the surt to get canonicalized non-surt url
    if not surt_ordered:
        key = unsurt(key)

    return key

pywb.utils.canonicalize.canonicalize = my_canonicalize


### warcio.recordloader.ARCHeadersParser : allow white space in URLs in ARC record header
import warcio.recordloader

class MyARCHeadersParser(warcio.recordloader.ARCHeadersParser):
    """ See warcio.recordloader.ARCHeadersParser

    Changes: allow white space in URLs in ARC record header
    """

    def parse(self, stream, headerline=None):
        total_read = 0

        if headerline is None:
            headerline = stream.readline()

        headerline = warcio.recordloader.StatusAndHeadersParser.decode_header(headerline)

        header_len = len(headerline)

        if header_len == 0:
            raise EOFError()

        headerline = headerline.rstrip()

        headernames = self.headernames

        # if arc header, consume next two lines
        if headerline.startswith('filedesc://'):
            version = warcio.recordloader.StatusAndHeadersParser.decode_header(stream.readline())  # skip version
            spec = warcio.recordloader.StatusAndHeadersParser.decode_header(stream.readline())  # skip header spec, use preset one
            total_read += len(version)
            total_read += len(spec)

        parts = headerline.split(' ')

        if len(parts) != len(headernames):
            if len(parts) > len(headernames):
                sys.stderr.write('Wrong headerline (white space in URL?): ' + headerline + '\n')
                url = parts[0]
                sep = '%20'
                if '?' in url:
                    sep = '+'
                for i in range(1, (len(parts) - len(headernames) + 1)):
                    url += sep + parts[i]
                new_parts = [url]
                for i in range((len(parts) - len(headernames) + 1), len(parts)):
                    new_parts.append(parts[i])
                sys.stderr.write('Fixed headerline (removed white space in URL): {}\n'.format(new_parts))
                parts = new_parts
            else:
                msg = 'Wrong # of headers, expected arc headers {0}, Found {1}'
                msg = msg.format(headernames, parts)
                raise warcio.recordloader.StatusAndHeadersParserException(msg, parts)

        protocol, headers = self._get_protocol_and_headers(headerline, parts)

        return warcio.recordloader.StatusAndHeaders(statusline='',
                                headers=headers,
                                protocol='WARC/1.0',
                                total_len=total_read)

warcio.recordloader.ARCHeadersParser = MyARCHeadersParser

### warcio.archiveiterator.ArchiveIterator: relax if content-length in ARC header is incorrect
###   (does not matter with per-block/record compression)
import warcio.archiveiterator

class MyArchiveIterator(warcio.archiveiterator.ArchiveIterator):

    def _consume_blanklines(self):
        """ Consume blank lines that are between records
        - For warcs, there are usually 2
        - For arcs, may be 1 or 0
        - For block gzipped files, these are at end of each gzip envelope
          and are included in record length which is the full gzip envelope
        - For uncompressed, they are between records and so are NOT part of
          the record length

          count empty_size so that it can be substracted from
          the record length for uncompressed

          if first line read is not blank, likely error in WARC/ARC,
          display a warning
        """
        empty_size = 0
        first_line = True

        while True:
            line = self.reader.readline()
            if len(line) == 0:
                return None, empty_size

            stripped = line.rstrip()

            if len(stripped) == 0 or first_line:
                empty_size += len(line)

                if len(stripped) != 0:
                    # if first line is not blank,
                    # likely content-length was invalid, display warning
                    err_offset = self.fh.tell() - self.reader.rem_length() - empty_size
                    sys.stderr.write(self.INC_RECORD.format(err_offset, MyArchiveIterator.escape_string(line)))
                    self.err_count += 1

                first_line = False
                continue

            return "", empty_size

#warcio.archiveiterator.ArchiveIterator = MyArchiveIterator


### relax LimitReader to read remaining input from underlying decompression stream
### in case the record length is (slightly) off
from pywb.indexer.archiveindexer import DefaultRecordParser


class MyDefaultRecordParser(DefaultRecordParser):

    @staticmethod
    def escape_string(s):
        return ''.join(MyDefaultRecordParser.escape_non_printable_char(c) for c in s)

    @staticmethod
    def escape_non_printable_char(c):
        if c <= '\x7f':
            return c.encode('unicode_escape').decode('ascii')
        return r'\x{0:02x}'.format(ord(c))

    def create_record_iter(self, raw_iter):
        append_post = self.options.get('append_post')
        include_all = self.options.get('include_all')
        surt_ordered = self.options.get('surt_ordered', True)
        minimal = self.options.get('minimal')

        if append_post and minimal:
            raise Exception('Sorry, minimal index option and ' +
                            'append POST options can not be used together')

        for record in raw_iter:
            entry = None

            if not include_all and not minimal and (record.http_headers.get_statuscode() == '-'):
                continue

            if record.rec_type == 'arc_header':
                continue

            if record.format == 'warc':
                if (record.rec_type in ('request', 'warcinfo') and
                     not include_all and
                     not append_post):
                    continue

                elif (not include_all and
                      record.content_type == 'application/warc-fields'):
                    continue

                entry = self.parse_warc_record(record)
            elif record.format == 'arc':
                entry = self.parse_arc_record(record)

            if not entry:
                continue

            if entry.get('url') and not entry.get('urlkey'):
                entry['urlkey'] = pywb.utils.canonicalize.canonicalize(entry['url'], surt_ordered)

            compute_digest = False

            if (entry.get('digest', '-') == '-' and
                record.rec_type not in ('revisit', 'request', 'warcinfo')):

                compute_digest = True

            elif not minimal and record.rec_type == 'request' and append_post:
                method = record.http_headers.protocol
                len_ = record.http_headers.get_header('Content-Length')

                post_query = MethodQueryCanonicalizer(method,
                                                entry.get('_content_type'),
                                                len_,
                                                record.raw_stream)

                entry['_post_query'] = post_query

            entry.record = record

            self.begin_payload(compute_digest, entry)

            while True:
                buff = record.raw_stream.read(warcio.utils.BUFF_SIZE)
                if not buff:
                    break
                self.handle_payload(buff)

            ### read remaining input from underlying decompression stream
            last_buff = None
            while True:
                buff = raw_iter.reader.readline()
                if not buff:
                    break
                if last_buff and len(last_buff):
                    sys.stderr.write("Adding remaining content from stream: {}\t({})\n".format(
                        MyDefaultRecordParser.escape_string(last_buff), record.rec_headers.get_header('uri')))
                    self.handle_payload(last_buff)
                last_buff = buff
            trailing_newline = False
            if last_buff is not None:
                if last_buff == '\n':
                    sys.stderr.write("Stripped trailing \\n\t({})\n".format(record.rec_headers.get_header('uri')))
                    trailing_newline = True
                elif len(last_buff) > 1 and last_buff[-1] == '\n':
                    sys.stderr.write("Adding remaining content from stream (stripped trailing \\n): {}\t({})\n".format(
                        MyDefaultRecordParser.escape_string(last_buff[:-1]), record.rec_headers.get_header('uri')))
                    self.handle_payload(last_buff[:-1])
                    trailing_newline = True
                elif len(last_buff) > 0:
                    sys.stderr.write("Adding remaining content from stream (without trailing \\n): {}\t({})\n".format(
                        MyDefaultRecordParser.escape_string(last_buff), record.rec_headers.get_header('uri')))
                    self.handle_payload(last_buff)
                if not trailing_newline:
                    sys.stderr.write("No trailing \\n found\t({})\n".format(record.rec_headers.get_header('uri')))

            raw_iter.read_to_end(record)

            entry.set_rec_info(*raw_iter.member_info)
            self.end_payload(entry)

            yield entry

pywb.indexer.archiveindexer.DefaultRecordParser = MyDefaultRecordParser


#=============================================================================
class IndexWARCJob(MRJob):
    """ This job receives as input a manifest of WARC/ARC files and produces
    a CDX index per file

    The pywb.indexer.cdxindexer is used to create the index, with a fixed set of options
    TODO: add way to customized indexing options.

    """
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'

    JOBCONF =  {'mapreduce.task.timeout': '9600000',
                'mapreduce.input.fileinputformat.split.maxsize': '50000000',
                'mapreduce.map.speculative': 'false',
                'mapreduce.reduce.speculative': 'false',
                'mapreduce.job.jvm.numtasks': '-1',

                'mapreduce.input.lineinputformat.linespermap': 2,
                }

    def configure_options(self):
        """Custom command line options for indexing"""
        super(IndexWARCJob, self).configure_options()

        self.add_passthru_arg('--warc_bucket', dest='warc_bucket',
                              default='commoncrawl',
                              help='source bucket for warc paths, if input is a relative path (S3 Only)')

        self.add_passthru_arg('--cdx_bucket', dest='cdx_bucket',
                              default='my_cdx_bucket',
                              help='destination bucket for cdx (S3 Only)')

        self.add_passthru_arg('--skip-existing', dest='skip_existing', action='store_true',
                              help='skip processing files that already have CDX',
                              default=True)

        self.add_passthru_arg("--s3_local_temp_dir", dest='s3_local_temp_dir',
                              help='Local temporary directory to buffer content from S3',
                              default=None)

    def mapper_init(self):
        # Note: this assumes that credentials are set via
        # AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variables
        self.boto_config = botocore.client.Config(
            read_timeout=180,
            retries={'max_attempts' : 20})
        s3client = boto3.client('s3', config=self.boto_config)

        try:
            s3client.head_bucket(Bucket=self.options.warc_bucket)
        except botocore.exceptions.ClientError as e:
            LOG.error('Failed to access bucket {}: {}'.format(
                self.options.warc_bucket, e))
            return

        try:
            s3client.head_bucket(Bucket=self.options.cdx_bucket)
        except botocore.exceptions.ClientError as e:
            LOG.error('Failed to access bucket {}: {}'.format(
                self.options.cdx_bucket, e))
            return

        self.index_options = {
            'surt_ordered': True,
            'sort': True,
            'cdxj': True,
            #'minimal': True
        }

    def mapper(self, _, line):
        warc_path = line.split('\t')[-1]
        try:
            self._load_and_index(warc_path)
        except Exception as exc:
            LOG.error('Failed to index ' + warc_path)
            raise

    def _conv_warc_to_cdx_path(self, warc_path):
        # set cdx path
        cdx_path = warc_path.replace('crawl-data', 'cc-index/cdx')
        cdx_path = cdx_path.replace('.warc.gz', '.cdx.gz')
        cdx_path = cdx_path.replace('.arc.gz', '.cdx.gz')
        return cdx_path

    def _load_and_index(self, warc_path):

        cdx_path = self._conv_warc_to_cdx_path(warc_path)

        LOG.info('Indexing WARC: ' + warc_path)
        s3client = boto3.client('s3', config=self.boto_config)

        if self.options.skip_existing:
            try:
                s3client.head_object(Bucket=self.options.cdx_bucket,
                                          Key=cdx_path)
                LOG.info('Already Exists: ' + cdx_path)
                return
            except botocore.client.ClientError as exception:
                pass # ok, not found

        try:
            s3client.head_object(Bucket=self.options.warc_bucket,
                                      Key=warc_path)
        except botocore.client.ClientError as exception:
            LOG.error('WARC not found: ' + warc_path)
            return

        with TemporaryFile(mode='w+b',
                           dir=self.options.s3_local_temp_dir) as warctemp:
            LOG.info('Fetching WARC: ' + warc_path)
            try:
                s3client.download_fileobj(self.options.warc_bucket, warc_path, warctemp)
            except botocore.client.ClientError as exception:
                LOG.error('Failed to download {}: {}'.format(warc_path, exception))
                return

            warctemp.seek(0)
            LOG.info('Successfully fetched WARC: ' + warc_path)

            with TemporaryFile(mode='w+b',
                               dir=self.options.s3_local_temp_dir) as cdxtemp:
                with GzipFile(fileobj=cdxtemp, mode='w+b') as cdxfile:
                    # Index to temp
                    write_cdx_index(cdxfile, warctemp, warc_path, **self.index_options)

                # Upload temp
                cdxtemp.flush()
                cdxtemp.seek(0)
                LOG.info('Uploading CDX: ' + cdx_path)
                try:
                    s3client.upload_fileobj(cdxtemp, self.options.cdx_bucket, cdx_path)
                except botocore.client.ClientError as exception:
                    LOG.error('Failed to upload {}: {}'.format(cdx_path, exception))
                    return
                LOG.info('Successfully uploaded CDX: ' + cdx_path)


if __name__ == "__main__":
    IndexWARCJob.run()
