# -*- coding:utf-8 -*-
"""
@author:Hao
@file:Toivonen.py
@time:10/3/201810:36 AM
"""
import math
import random

class Tovionen(object):
    # main function
    def my_tovionen(self):
        # # init spark task
        # sc = SparkContext(appName="HW2")
        #
        # # read textFile
        # dataset = sc.textFile("input/dataset.txt")
        #
        # lst = dataset.map(lambda x: x.strip("(").strip(")").split(','))
        # allBasket = lst.collect()
        allBasket = []
        with open('input/dataset.txt', 'r') as f:
            data = f.readlines()
            for line in data:
                line = line.strip("(").strip('\n').strip(")").split(',')
                allBasket.append([int(x) for x in line])

        # init value
        totalNum = len(allBasket)
        sampleRate = 0.1
        supportRatio = float(float(4) / float(15))
        sampleNum = int(math.ceil(totalNum * sampleRate))
        global_threshold = int(math.ceil(totalNum * supportRatio))
        local_threshold = int(math.ceil(sampleNum * supportRatio))
        it = 1
        while True:
            sampleData = self.getSampleData(allBasket, it, sampleNum, totalNum)
            frequentItemSets, neagtiveBorder = self.in_round(sampleData, local_threshold)

            trueFrquentItemsets = self.getFrequentItemSets(frequentItemSets, allBasket, global_threshold)

            f = open('output/OutputForIteration_' + str(it) + '.txt', 'w')  # 若是'wb'就表示写二进制文件
            f.write('Sample Created:\n')
            f.write(str(sampleData) + "\n")
            f.write("frequent itemsets:\n")
            f.write(str(trueFrquentItemsets) + "\n")
            f.write("negative border:\n")
            f.write(str(neagtiveBorder) + "\n")
            f.close()

            flag, falseNegative =  self.checkNegativeBorder(neagtiveBorder, allBasket, global_threshold)
            if flag:
                print 'False Negative:', falseNegative
                print '---' + str(it) + 'times iteration---'
                break
            else:
                print 'False Negative:', sorted(falseNegative)
            it += 1

    def getFrequentItemSets(self, frequentItemSets, allData, threshold):
        # build countTable
        trueFrquentItemsets = []
        countTable = {}
        for basket in allData:
            for itemset in frequentItemSets:
                if set(itemset).issubset(set(basket)):
                    countTable[tuple(itemset)] = countTable.get(tuple(itemset), 0) + 1

        # check frequentItemsets
        for key, value in countTable.items():
            if value >= threshold:
                trueFrquentItemsets.append(key)
        return trueFrquentItemsets

    def checkNegativeBorder(self, neagtiveBorder, allData, threshold):
        # build count table
        falseNegative = []
        flag = True
        countTable = {}
        for basket in allData:
            for itemset in neagtiveBorder:
                if set(itemset).issubset(set(basket)):
                    countTable[tuple(itemset)] = countTable.get(tuple(itemset), 0) + 1
        # check negative border
        for key, value in countTable.items():
            if value >= threshold:
                flag = False
                falseNegative.append(key)
        return flag, falseNegative

    def getSampleData(self, allData, it, sampleNum, totalNum):
        random.seed(it)
        startIndex = random.randint(0, totalNum - sampleNum)
        sampleData = allData[startIndex: startIndex + sampleNum]
        # i = 0
        # s = set()
        # while len(s) < sampleNum:
        #     s.add(random.randint(0, totalNum))
        # print(s)
        #
        # sampleData = []
        # for i in range(0, len(allData)):
        #     if i in s:
        #         sampleData.append([int(x) for x in allData[i]])

        # print sample Data
        return sampleData

    def checkSubSets(self, cTemp, frequentItemSets):
        for k in range(len(cTemp)):
            if k == len(cTemp) - 1:
                sub = cTemp[:k]
            else:
                sub = cTemp[:k] + cTemp[k + 1:]

            if sub is not None and sub not in frequentItemSets:
                return False
        return True

    def first_it(self, sampleData, threshold, frequentItemSets, neagtiveBorder):

        # generate candidates
        # first round
        countTable = {}
        for i in range(len(sampleData)):
            for j in range(len(sampleData[i])):
                countTable[sampleData[i][j]] = countTable.get(sampleData[i][j], 0) + 1
        # frequent list
        curFrequentItemSet = []
        for key, value in countTable.items():
            if value >= math.ceil(threshold):
                curFrequentItemSet.append([key])
            else:
                neagtiveBorder.append([key])

        frequentItemSets.extend([(x) for x in curFrequentItemSet])


        # generate candidates
        c = []
        for i in range(len(curFrequentItemSet) - 1):
            for j in range(i + 1, len(curFrequentItemSet)):
                v = curFrequentItemSet[i]
                w = curFrequentItemSet[j]
                # k - 1 common
                t = set(v).union(set(w))
                if len(t) == len(v) + 1:
                    cTemp = list(set(v).union(set(w)))
                    # check whether all subsets of cTemp
                    if self.checkSubSets(cTemp, curFrequentItemSet):
                        c.append(cTemp)

        frequentItemSets.extend(curFrequentItemSet)
        return c

    def in_round(self, sampleData, threshold):
        neagtiveBorder = []
        frequentItemSets = []
        # first round
        c = self.first_it(sampleData, threshold, frequentItemSets, neagtiveBorder)

        inner_round = 1
        while len(c) != 0:
            inner_round += 1
            # build count table
            # traverse buckets
            countTable = {}
            for i in range(len(sampleData)):
                for itemSet in c:
                    if set(itemSet).issubset(set(sampleData[i])):
                        item = tuple(sorted(itemSet))
                        countTable[item] = countTable.get(item, 0) + 1

            # get frequent Itemsets and negative border sets
            curFrequentItemSet = []
            for key, value in countTable.items():
                if value >= math.ceil(threshold):
                    curFrequentItemSet.append(list(key))
                else:
                    neagtiveBorder.append(key)

            frequentItemSets.extend(curFrequentItemSet)
            lastFrequentItemSet = curFrequentItemSet


            # next inner round candidates
            c = []
            for i in range(len(lastFrequentItemSet) - 1):
                for j in range(i + 1, len(lastFrequentItemSet)):
                    v = lastFrequentItemSet[i]
                    w = lastFrequentItemSet[j]
                    # k - 1 common
                    t = set(v).union(set(w))
                    if len(t) == len(v) + 1:
                        cTemp = list(set(v).union(set(w)))
                        # check whether all subsets of cTemp
                        if self.checkSubSets(cTemp, lastFrequentItemSet):
                            c.append(cTemp)

        return frequentItemSets, neagtiveBorder


if __name__ == '__main__':
    t = Tovionen()
    t.my_tovionen()




