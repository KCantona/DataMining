# -*- coding:utf-8 -*-
"""
@author:Hao
@file:LSH.py
@time:10/19/20183:48 PM
"""
from pyspark import SparkContext
import sys

movieNum = 100
userNumber = 100
signatureNumber = 20
def printf(iterator):
    print(list(iterator))
    print("---------------------------------------")


def extraKeyVakue(line):
    lineSplit = line.split(",")

    key = int(lineSplit[0][1:])
    val = [0 for i in range(movieNum)]
    for index in lineSplit[1:]:
        val[int(index)] = 1

    return (key, val)

def extra(line):
    lineSplit = line.split(",")
    key = int(lineSplit[0][1:])
    val = [int(v) for v in lineSplit[1:]]
    return (key, val)

def getSignature(movies):
    sign = [0 for i in range(signatureNumber)]
    for i in range(len(sign)):
        sign[i] = getFirstMovie(movies, i)
    return sign

def getFirstMovie(movies, index):
    minV = float('inf')
    for movie in movies:
        h = (3 * movie + 13 * index) % 100
        minV = min(minV, h)
    return minV

def getBand(line):
    userID = line[0]
    signature = line[1]
    res = []
    i = 0
    while i < signatureNumber:
        bandKey = str(i) + str(signature[i]) + str(signature[i + 1]) + str(signature[i + 2]) + str(signature[i + 3])
        res.append((bandKey, userID))
        i += 4
    return res

def getCandidatePair(KV):
    list = KV[1]
    res = []
    for i in range(len(list) - 1):
        for j in range(i + 1, len(list)):
            small = min(list[i], list[j])
            large = max(list[i], list[j])
            res.append((small, large))
    return res

def getJaccard(pair):
    dic = inputDic
    a = pair[0]
    b = pair[1]
    setA = set(dic[a])
    setB = set(dic[b])
    inter = len(setA & setB)
    union = len(setA | setB)
    similarity = inter / union
    return (a, (b, similarity)), (b, (a, similarity))

def getTopFive(lst):
    lst.sort(key = lambda x:(-x[1],x[0]))
    res = []
    for pair in lst[:2]:
        res.append(pair[0])
    return res

def recommendMovie(userList):
    dic = {}
    for user in userList:
        for movie in inputDic[user]:
            dic[movie] = dic.get(movie, 0) + 1

    res = []
    for key, val in dic.items():
        res.append((key, val))
    res.sort(key=lambda x: x[1])
    mov = []
    for m in res[:3]:
        mov.append(m[0])
    return mov


if __name__ == '__main__':
    # init spark task
    sc = SparkContext(appName="Hao")

    # read input file
    # lines = sc.textFile("input_sample_large.txt").cache()
    lines = sc.textFile(sys.argv[1])
    printf(lines.collect())
    lineSplit = lines.map(extra)
    inputDic = dict(lineSplit.collect())

    # get signature
    signature = lineSplit.mapValues(getSignature)

    # divide into 5 bands
    bandMarix = signature.flatMap(getBand)

    # get candidatePair
    signatureCandidatePair = bandMarix.groupByKey().mapValues(lambda x:list(x)).filter(lambda x: len(x[1]) > 1)

    candidatePair = signatureCandidatePair.flatMap(getCandidatePair).distinct()
    printf(candidatePair.collect())

    #caculate user Jaccard
    jaccardSimlilarity = candidatePair.flatMap(getJaccard).groupByKey().mapValues(lambda x: list(x))
    printf(jaccardSimlilarity.collect())

    #top five
    topFiveUser = jaccardSimlilarity.mapValues(getTopFive)
    printf(topFiveUser.collect())

    #top 3 movie
    movies = topFiveUser.mapValues(recommendMovie).sortByKey()
    printf(movies.collect())

    f = open(sys.argv[2], 'w')
    for record in movies.collect():
        movie = ",".join([str(x) for x in record[1]])
        f.write('U' + str(record[0]) + "," + movie + "\n")
    f.close()











