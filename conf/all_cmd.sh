#!/usr/bin/env bash

#78
pssh -h ~/host.txt -i "ls /tmp/output/tweets/4/*/*/*/*/part* "
pssh -h ~/host.txt -i "cat /tmp/output/tweets/4/*/*/*/*/part* > /tmp/data/tweet_dataset_4"
ls /tmp/output/tweets/4/*/part*
cat /tmp/output/tweets/4/*/part*  > /tmp/data/tweet_dataset_4

pssh -h ~/host.txt -i "cat /tmp/data/tweet_dataset_4 |wc -l "


#75
scp  169.45.101.78:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.77:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.69:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.92:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.85:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.84:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.94:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.74:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
scp  169.45.101.88:/tmp/data/tweet_dataset_4 /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}

#change to todays date
du -sh /tmp/data/part-00000-20160225*

cat /tmp/data/part-00000-20160226* >> /tmp/data/tweet_dataset

mv /tmp/data/tweet_dataset /tmp/data/tweet_dataset.tmp

#pssh scp /tmp/data/tweet_dataset.tmp


# pssh -h ~/host.txt -i "ls /tmp/output/tweets/4/*/*/*/part* "
# pssh -h ~/host.txt -i "cat /tmp/output/tweets/*/*/*/*/part* >> /tmp/data/tweet_dataset_4"

#cat  /tmp/output/tweets/*/part* >> /tmp/data/tweet_dataset_4
