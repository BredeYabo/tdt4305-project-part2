import sys
from pyspark import SparkConf, SparkContext
# from pyspark.mllib.feature import HashingTF # Requires Yarn

sc = SparkContext()

user = str(sys.argv[1])
k_rec_users = str(sys.argv[2])
in_file = str(sys.argv[3])
out_file = str(sys.argv[4])

# user \t message
tweets = sc.textFile(in_file) \
        .map(lambda line: (line.split("\t")))


# user document
user_tweets = tweets.filter(lambda x: x[0] == user) \
        .flatMapValues(lambda x: x.split(" ")) \
        .distinct() \
        .values() \
        .cache()
        # .map(lambda (x,y): (x, (y.split(" "),1))) # Tokenize message

# all_terms = tweets.map(lambda (x,y): y) \
#         .flatMap(lambda x: x.split(" "))

# Treating all tweets by a user as a document
documents = tweets.reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x,0)) \
        .join(user_tweets)
        # .map(lambda (x,y): ((x), y+x[1].count("Hello")))
        ##.map(lambda x: x[1].split(" "))

# hashingTF = HashingTF()

print(documents.count())
print(documents.take(1))
#print(all_terms.take(10))

# all_tweets.map(lambda x: "%s" %(x)) \
#         .coalesce(1, shuffle = False) \
#         .saveAsTextFile(out_file)
