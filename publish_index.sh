#!/bin/bash

YEARWEEK="$1"

if [ -z "$YEARWEEK" ]; then
    echo "$0 <year-week-of-crawl-archive>"
    exit 1
fi

set -x
set -e


## Create the metadata (title) for the index on the website
if ! [ -e $YEARWEEK-metadata.yaml ]; then
    s3cmd get s3://commoncrawl/cc-index/collections/CC-MAIN-2015-18/metadata.yaml $YEARWEEK-metadata.yaml
    echo "Please, edit $YEARWEEK-metadata.yaml"
    exit 1
fi
s3cmd put $YEARWEEK-metadata.yaml s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/metadata.yaml --acl-public

test -d cdx-$YEARWEEK || mkdir cdx-$YEARWEEK
cd cdx-$YEARWEEK

## create cluster index
s3cmd get --force s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/part-*
cat part-* | awk '{printf "%s\t%s\n",$0,NR}' > cluster.idx
export LC_ALL=C
sort -c ./cluster.idx
rm ./part-00*
s3cmd put ./cluster.idx s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/cluster.idx --acl-public

## set public permissions where needed (technically only the cdx-* need to be public)
s3cmd setacl s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/ --acl-private --recursive --exclude='*' --include='part-*' --include='_SUCCESS'
s3cmd setacl s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/ --acl-public --recursive --exclude='*' --include='cdx-*.gz' --include='cluster.idx' --include='metadata.yaml'

# make *.cdx.gz files (to be deleted later) private
s3cmd setacl s3://commoncrawl/cc-index/CC-MAIN-$YEARWEEK/segments/ --acl-private --recursive
