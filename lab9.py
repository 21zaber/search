# -*- encoding: utf-8 -*-

from nltk.tokenize import RegexpTokenizer
import sys
import re
import time
import math
from heapq import *

tokenizer = RegexpTokenizer('[a-z]+')

bigrams = {}

bigramFirst = {}
bigramSecond = {}


bigramCount = 0

def calcTokensInBigrams(token1, token2):
    if token1 not in bigramFirst:
        bigramFirst[token1] = 1
    else:
        bigramFirst[token1] += 1

    if token2 not in bigramSecond:
        bigramSecond[token2] = 1
    else:
        bigramSecond[token2] += 1

def processArticle(text):
    global bigramCount

    tokens = [i for i in tokenizer.tokenize(text) if len(i) > 2]

    for i in range(0, len(tokens) - 3):
        token1 = tokens[i]
        token2 = tokens[i + 1]
        calcTokensInBigrams(token1, token2)

        curBigrams = ["{} {}".format(token1, token2)]

        if i + 2 < len(tokens):
            token3 = tokens[i + 2]
            curBigrams.append("{} {}".format(token1, token3))
            calcTokensInBigrams(token1, token3)

        if i + 3 < len(tokens):
            token4 = tokens[i + 3]
            curBigrams.append("{} {}".format(token1, token4))
            calcTokensInBigrams(token1, token4)

        bigramCount += len(curBigrams)

        for bigram in curBigrams:
            if bigram not in bigrams:
                bigrams[bigram] = 1
            else:
                bigrams[bigram] += 1






start = time.time()

from run_indexsation import get_ids

for i in get_ids():
    processArticle(i.get('text', ''))

answerT = []
answerChi = []

tDiscard = 0
chiDiscard = 0

for bigram in bigrams.keys():
    token1, token2 = bigram.split()

    c = bigrams[bigram]
    mean = c / float(bigramCount)
    distMean = bigramFirst[token1] * bigramSecond[token2] / float(bigramCount) / float(bigramCount)
    t = (mean - distMean) / (math.sqrt(mean/float(bigramCount)))
    if t > 2.576:
        heappush(answerT, (t, bigram))
    else:
        tDiscard += 1

    o11 = bigrams[bigram]
    o12 = bigramSecond[token2] - o11
    o21 = bigramFirst[token1] - o11
    o22 = bigramCount - bigramFirst[token1] - bigramSecond[token2]

    chi = bigramCount * (o11 * o22 - o12 * o21) * (o11 * o22 - o12 * o21) / float((o11 + o12) * (o11 + o21) * (o12 + o22) * (o21 + o22))
    if chi > 3.841:
        if o11 > 10 and o12 > 10 and o21 > 10 and o22 > 10:
            heappush(answerChi, (chi, bigram))
        else:
            chiDiscard += 1
    else:
        chiDiscard += 1

print ("T TEST")
print (len(answerT))
print ("t discard %d" % tDiscard)

answerT = nlargest(300, answerT)

for i in range(0, 300):
    print (answerT[i][1])

print ("CHI CRITERIA")
print (len(answerChi))
print ("chi discard %d" % chiDiscard)

answerChi = nlargest(300, answerChi)
for i in range(0, 300):
    print (answerChi[i][1])






 




