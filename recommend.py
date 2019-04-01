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

# Filter out user from words
user_words = sc.broadcast(usr_tweets.distinct().collect())
words = tweets.filter(lambda x: x[0] != usr) \
        .flatMapValues(lambda x: (x.split(" "))) \
        .filter(lambda (x,y): y in user_words.value)

# count of words in tweet Y
all_words = words.map(lambda (x,y): ((x,y),1)) \
        .reduceByKey(lambda x,y: x+y) \
# ((user,word),count)
user_words_count = usr_tweets.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
sim = sc.broadcast(dict(zip(user_words_count.keys().collect(), user_words_count.values().collect())))

all_words.map(lambda ((x,y),z): (x, min(z, sim.value[y]))) \
        .reduceByKey(lambda x,y: x+y) \
        .sortByKey(ascending=True) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(ascending=False) \
        .zipWithIndex() \
        .filter(lambda x: x[1] < k) \
        .keys() \
        .map(lambda x: "%s\t%s" %(x[1],x[0])) \
        .coalesce(1, shuffle = False) \
        .saveAsTextFile(out_file)
