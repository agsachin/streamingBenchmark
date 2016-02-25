#!/usr/bin/env bash


# pssh -h ~/host.txt -i "cat /tmp/output/tweets/*/*/*/*/*/part* > /tmp/data/tweet_dataset_all"
# pssh -h ~/host.txt -i "cat /tmp/output/tweets/*/*/*/*/part* >> /tmp/data/tweet_dataset_all"

#pssh -h ~/host.txt -i "cat /tmp/data/tweet_dataset_all |wc -l "

#78
#cat /tmp/output/tweets/*/part*  > /tmp/data/tweet_dataset_all
#cat  /tmp/output/tweets/*/*/part* >> /tmp/data/tweet_dataset_all

#cat /tmp/data/tweet_dataset_all |wc -l

#75
#scp  169.45.101.78:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.77:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.69:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.92:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.85:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.84:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.94:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.74:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}
#scp  169.45.101.88:/tmp/data/tweet_dataset_all /tmp/data/part-00000-$(date -u +\%Y\%m\%dT\%H\%M\%S)Z.${RANDOM}

#du -sh /tmp/data/part-00000-20160225T055*

# cat /tmp/data/part-00000-* > /tmp/data/tweet_dataset