import sys
from pyspark import SparkConf, SparkContext

sc = SparkContext()

user = str(sys.argv[1])
k_rec_users = str(sys.argv[2])
in_file = str(sys.argv[3])
out_file = str(sys.argv[4])

# Tweets schema:
# user \t message

tweets = sc.textFile("tweets.tsv") \
        .map(lambda line: (line.split("\t"))) \
        # .map(lambda (x,y): (x, (y.split(" "),1))) # Tokenize message

user_tweet = tweets.filter(lambda x: x[0] == user) \
        .map(lambda x: x[1]) \
        .flatMap(lambda x: x.split(" ")) \
        .distinct()



#sim = tweets.reduceByKey(lambda x, y: ((x[0],x[1]+1)) if x[0] == y[0] else ((x[0],x[1]+1)))

print(tweets.take(1))
#print(sim.take(1))
print(user_tweet.collect())

