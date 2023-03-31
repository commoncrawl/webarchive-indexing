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
if ! [ -e "$YEARWEEK-metadata.yaml" ]; then
	if [ -n "$MONTH" ]; then
		YEAR=${YEARWEEK%%-*}
		echo "title: '${MONTH^} $YEAR Index'" >"$YEARWEEK-metadata.yaml"
	else
		aws s3 cp s3://commoncrawl/cc-index/collections/CC-MAIN-2015-18/metadata.yaml "$YEARWEEK-metadata.yaml"
		echo "Please, edit $YEARWEEK-metadata.yaml"
		exit 1
	fi
fi
aws s3 cp "$YEARWEEK-metadata.yaml" "s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/metadata.yaml"

test -d "cdx-$YEARWEEK" || mkdir "cdx-$YEARWEEK"
cd "cdx-$YEARWEEK"

## create cluster index

aws s3 cp --recursive --exclude '*' --include 'part-*' "s3://commoncrawl-index-temp/CC-MAIN-$YEARWEEK/indexes/" ./
cat part-* | awk '{printf "%s\t%s\n",$0,NR}' >cluster.idx
LC_ALL=C sort -c ./cluster.idx

aws s3 cp ./cluster.idx "s3://commoncrawl/cc-index/collections/CC-MAIN-$YEARWEEK/indexes/cluster.idx"
