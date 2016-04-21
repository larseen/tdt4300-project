# tdt4300-project

The code in this repository assumes that you have the following files in this directory:

- dataset_TIST2015.tsv
- dataset_TIST2015_Cities.txt
- geotweets.tsv
- negative-words.txt
- positive-words.txt

##How to run

###Task1

###Task2

```bash
$ spark-submit 2.py input_file output_file positive_words negative_words
```
Example
```bash
$ spark-submit 2.py ../geotweets.tsv geotweets.tsv hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/positive-words.txt hdfs://dascosa09.idi.ntnu.no:8020/user/janryb/negative-words.txt
```
