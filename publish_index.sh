#!/bin/bash

YEARWEEK="$1"
MONTH="$2"

if [ -z "$YEARWEEK" ]; then
	echo "$0 <year-week-of-crawl-archive> [<month>]"
	exit 1
fi

set -x
set -e


## Create the metadata (title) for the index on the website
if ! [ -e $YEARWEEK-metadata.yaml ]; then
	if [ -n "$MONTH" ]; then
		YEAR=${YEARWEEK%%-*}
		echo "title: '${MONTH^} $YEAR Index'" >$YEARWEEK-metadata.yaml
	else
		aws s3 cp s3://commoncrawl/cc-index/collections/CC-MAIN-2015-18/metadata.yaml $YEARWEEK-metadata.yaml
		echo "Please, edit $YEARWEEK-metadata.yaml"
		exit 1
	fi
fi
aws s3 cp $YEARWEEK-metadata.yaml s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/metadata.yaml --acl public-read

test -d cdx-$YEARWEEK || mkdir cdx-$YEARWEEK
cd cdx-$YEARWEEK

## create cluster index
aws s3 cp --recursive --exclude '*' --include 'part-*' s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/ ./
cat part-* | awk '{printf "%s\t%s\n",$0,NR}' >cluster.idx
LC_ALL=C sort -c ./cluster.idx
#rm ./part-00*
aws s3 cp ./cluster.idx s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/cluster.idx --acl public-read

# remove obsolete data from bucket
#  - map-reduce _SUCCESS file/marker
aws s3 rm s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/_SUCCESS
#  - part-00* files concatenated to cluster.idx
aws s3 rm --recursive s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/ --exclude "*" --include "part-00*"

## TODO:
##   check why setting public-read permissions via
##     --jobconf "fs.s3a.acl.default=PublicRead"
##   does not work?
##
## set public permissions where needed (technically only the cdx-* need to be public)
## this should be already done, if not run:
s3cmd setacl s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/ --acl-public --recursive --exclude='*' --include='cdx-*.gz' --include='cluster.idx' --include='metadata.yaml'
## or:
# aws s3 cp \
#     --exclude='*' \
#     --include='cdx-*.gz' \
#     --include='cluster.idx' \
#     --include='metadata.yaml' \
#     --recursive \
#     s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/ \
#     s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/ \
#     --acl public-read

# make *.cdx.gz files (to be deleted later) private
#s3cmd setacl s3://commoncrawl/cc-index/CC-MAIN-$YEARWEEK/segments/ --acl-private --recursive
