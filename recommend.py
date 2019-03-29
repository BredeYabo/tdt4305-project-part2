import sys
from pyspark import SparkConf, SparkContext
sc = SparkContext()

def getParam(param):
    for i in range(len(param)):
        if param[i] == '-user':
            usr = param[i+1]
        elif param[i] == '-k':
            k = int(param[i+1])
        elif param[i] == '-file':
            in_file = param[i+1]
        elif param[i] == '-output':
            out_file = param[i+1]
    return usr, k, in_file, out_file

usr, k, in_file, out_file = getParam(sys.argv)

# usr \t message
tweets = sc.textFile(in_file) \
        .map(lambda line: (line.split("\t")))

# usr document
usr_tweets = tweets.filter(lambda x: x[0] == usr) \
        .reduceByKey(lambda x,y: x+y) \
        .flatMapValues(lambda x: x.split(" ")) \
        .values() \

words = tweets.flatMapValues(lambda x: (x.split(" ")))

user_words = sc.broadcast(usr_tweets.collect())
words_distinct = words.distinct() \
        .filter(lambda (x,y): y in user_words.value) \
        .map(lambda (x,y): (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(ascending=False)

user_words = sc.broadcast(usr_tweets.distinct().collect())
words = words.filter(lambda (x,y): y in user_words.value) \
        .map(lambda (x,y): (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(ascending=False)

top_10_words = words.zipWithIndex() \
        .filter(lambda x: x[1] < k) \
        .keys() \
        .map(lambda (x,y): (y,x))

top_10_words_distinct = words_distinct.zipWithIndex() \
        .filter(lambda x: x[1] < k) \
        .keys() \
        .map(lambda (x,y): (y,x))

top_10_words = top_10_words.join(top_10_words_distinct) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(ascending=False)

top_10_words.zipWithIndex() \
        .filter(lambda x: x[1] < k) \
        .keys() \
        .map(lambda x: "%s\t%s" %(x[0],x[1])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile(out_file)
