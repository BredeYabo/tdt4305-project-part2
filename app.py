import sys
from pyspark import SparkConf, SparkContext, AccumulatorParam
# from pyspark.mllib.feature import HashingTF # Requires Yarn

class ListParam(AccumulatorParam):
    def zero(self, v):
        return []
    def addInPlace(self, acc1, acc2):
        acc1.extend(acc2)
        return acc1

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
        .reduceByKey(lambda x,y: x+y) \
        .flatMapValues(lambda x: x.split(" ")) \
        .distinct() \
        .values() \
        #.cache()
        # .map(lambda (x,y): (y,x)) \
        # .map(lambda (x,y): (x, (y.split(" "),1))) # Tokenize message

# all_terms = tweets.map(lambda (x,y): y) \
#         .flatMap(lambda x: x.split(" "))

# Treating all tweets by a user as a document
# documents = tweets.reduceByKey(lambda x,y: x+y) \
#         .map(lambda x: (x,0)) \
#         .join(user_tweets)
        # .map(lambda (x,y): ((x), y+x[1].count("Hello")))
        ##.map(lambda x: x[1].split(" "))
words = tweets.map(lambda x: (x[0],x[1].split(" "))) \
        .reduceByKey(lambda x,y: x+y) \
        #.leftOuterJoin(user_tweets)
        # map.(lambda x: (x,0))
        # map.(lambda)

# RDD operation in lambda

smallRDD = sc.broadcast(user_tweets.collect())
# join_a_b = words.filter(lambda: smallRDD.value == uu)
def file_read1(line):
    global list1 # Required otherwise the next line will fail
    list1 += [line]
    return line


def f(x):
    global num
    global list1
    for word in x:
        if word in smallRDD.value:
            num+=1
    list1 += [num]

num = sc.accumulator(0)
list1 = sc.accumulator([], ListParam())
words.foreach(lambda (x,y): f(y))

# hashingTF = HashingTF()

print(num.value)
print(list1.value[10])

# print(words.take(1))
#print(accum.count())
# print(smallRDD.value)
# print(user_tweets.take(1))
# print(highest.value)
# print(user_tweets.take(1))
# print(join_a_b.take(1))
#print(all_terms.take(10))

# all_tweets.map(lambda x: "%s" %(x)) \
#         .coalesce(1, shuffle = False) \
#         .saveAsTextFile(out_file)
