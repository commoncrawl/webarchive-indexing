import logging
from sparkcc import CCFileProcessorSparkJob

from tempfile import TemporaryFile
from pywb.indexer.cdxindexer import write_cdx_index
from gzip import GzipFile


LOG = logging.getLogger('IndexWARCJob')


class IndexWARCJob(CCFileProcessorSparkJob):
    """ This job receives as input a manifest of WARC/ARC files and produces
    a CDX index per file

    The pywb.indexer.cdxindexer is used to create the index, with a fixed set of options
    """

    name = 'IndexWARCJob'

    index_options = {
        'surt_ordered': True,
        'sort': True,
        'cdxj': True,
        #'minimal': True
    }

    def add_arguments(self, parser):
        super(CCFileProcessorSparkJob, self).add_arguments(parser)
        parser.add_argument("--output_base_url", required=True,
                            help="Destination for CDX output.")
        parser.add_argument("--skip-existing", dest='skip_existing', action='store_true',
                            help="Skip processing files for which the output CDX file already exists.")

    def _conv_warc_to_cdx_path(self, warc_path):
        cdx_path = warc_path.replace('crawl-data', 'cc-index/cdx')
        cdx_path = cdx_path.replace('.warc.gz', '.cdx.gz')
        cdx_path = cdx_path.replace('.warc.wet.gz', '.wet.cdx.gz')
        cdx_path = cdx_path.replace('.warc.wat.gz', '.wat.cdx.gz')
        return cdx_path

    def process_file(self, warc_path, tempfd):

        cdx_path = self._conv_warc_to_cdx_path(warc_path)

        LOG.info('Indexing WARC: %s', warc_path)

        if self.args.skip_existing and \
            self.check_for_output_file(cdx_path,self.args.output_base_url):
            LOG.info('Already Exists: %s', cdx_path)
            yield cdx_path, 0
            return

        with TemporaryFile(mode='w+b',
                           dir=self.args.local_temp_dir) as cdxtemp:

            success = False
            with GzipFile(fileobj=cdxtemp, mode='w+b') as cdxfile:
                try:
                    write_cdx_index(cdxfile, tempfd, warc_path, **self.index_options)
                    success = True
                except Exception as exc:
                    LOG.error('Failed to index %s: %s', warc_path, exc)

            cdxtemp.flush()
            cdxtemp.seek(0)

            if success:
                self.write_output_file(cdx_path, cdxtemp, self.args.output_base_url)
                LOG.info('Successfully uploaded CDX: %s', cdx_path)
                yield cdx_path, 1
            else:
                yield cdx_path, -1


if __name__ == "__main__":
    job = IndexWARCJob()
    job.run()
