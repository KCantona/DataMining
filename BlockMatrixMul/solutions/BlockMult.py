"""
@author:Hao
@file:BlockMult.py
@time:9/24/201812:07 PM
"""
import re
from pyspark import SparkContext

class Mapper(object):
    def mapA(self,txt):

        inputKey=re.search("\([0-9]+,[0-9]+\)",txt).group()
        inputValue=re.findall("\([0-9]+,[0-9]+,[0-9]+\)",txt)

        i=inputKey[1]
        k=inputKey[3]
        res = []
        for b in range(1, 4):
            res.append(((int(i),b), ['A', int(k), inputValue]))
        return res

    def mapB(self,txt):
        key=re.search("\([0-9]+,[0-9]+\)",txt).group()
        value=re.findall("\([0-9]+,[0-9]+,[0-9]+\)",txt)
        k=key[1]
        j=key[3]
        res = []
        for b in range(1, 4):
            res.append(((b, int(j)), ['B', int(k), value]))
        return res

# reduce
class Reducer(object):
    def reduce(self,matrix):
        index=matrix[0]
        dic = {}
        blockValue = []
        for i in range(len(matrix[1])):
            blockA = matrix[1][i][0][2]
            blockB = matrix[1][i][1][2]
            for itemA in blockA:
                for itemB in blockB:
                    elementA = itemA[1:len(itemA) - 1].split(",")
                    elementB = itemB[1:len(itemB) - 1].split(",")
                    if elementA[1]==elementB[0]:
                        if (elementA[0],elementB[1]) in dic.keys():
                            dic[(elementA[0],elementB[1])] += int(elementA[2]) * int(elementB[2])
                        else:
                            dic[(elementA[0],elementB[1])] = int(elementA[2]) * int(elementB[2])
        for key, value in dic.items():
            if value != 0:
                blockValue.append([int(key[0]),int(key[1]), value])
        return index, blockValue


if __name__=="__main__":
    def printf(iterator):
        print(list(iterator))
        print("---------------------------------------")

    # init spark task
    sc=SparkContext(appName="Hao")

    # read input file
    matrixA=sc.textFile("../input/file-A.txt").cache()
    matrixB=sc.textFile("../input/file-B.txt").cache()

    #do mapper task
    mapResA = matrixA.flatMap(Mapper().mapA)
    mapResB = matrixB.flatMap(Mapper().mapB)

    # join matrix A and B and filter one has k the other doesn't have the k value
    reducerInput = mapResA.join(mapResB).filter(lambda a: a[1][0][1] == a[1][1][1])

    # group by key and parse RDD address
    reduceTemp=reducerInput.groupByKey().map(lambda x: (x[0],list(x[1])))

    #multiplication in one block
    reducerOutput = reduceTemp.map(Reducer().reduce).filter(lambda x:len(x[1])>0).sortByKey()

    # write to output.txt
    with open('output.txt', 'w') as f:
        for block in reducerOutput.collect():
            f.write("%s\n" % str(block)[1:-1])






