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
        .values()

words = tweets.flatMapValues(lambda x: (x.split(" ")))

user_words = sc.broadcast(usr_tweets.distinct().collect())
words = words.filter(lambda (x,y): y in user_words.value)

# count of words in tweet Y
all_words = words.map(lambda (x,y): (x,1)) \
        .reduceByKey(lambda x,y: x+y)

# count of words in tweet X
user_words = sc.broadcast(usr_tweets.collect())
words_distinct = words.distinct() \
        .map(lambda (x,y): (x,user_words.value.count(y))) \
        .reduceByKey(lambda x,y: x+y)

similarity = all_words.join(words_distinct) \
        .map(lambda (x,y): (x,min(y))) \
        .sortByKey(ascending=True) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(ascending=False) \
        .zipWithIndex() \
        .filter(lambda x: x[1] < k) \
        .keys() \
        .map(lambda x: "%s\t%s" %(x[1],x[0])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile(out_file)
