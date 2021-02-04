import os
import time
import datetime
import traceback
import logging

###snowflake
class snowflakeIds(object):
    ###default beginTime is 2000-01-01 00:00:00
    def __init__(self, workerId, timeBits=41, workerBits=10, seqBits=12, beginTime=946656000000):
        self.timeBits = timeBits
        self.workerBits = workerBits
        self.seqBits = seqBits
        self.workerId = workerId
        self.curSeq = 0
        self.lastTime = 0
        self.beginTime = beginTime
        self.workerId = int(workerId)
        self.workerIdNum = int(workerId) << seqBits
        self.seqRound = 1 << seqBits

    def nextId(self):
        curTime = int(round(time.time() * 1000))
        #time is decreasing, clock must gone away
        if curTime < self.lastTime:
            return -1
        if self.lastTime == curTime:
            self.curSeq = (self.curSeq + 1) % self.seqRound
        else:
            self.curSeq = 0
            self.lastTime = curTime
        diffTime = int(curTime - self.beginTime) & (~(1 << 63))
        #print ("diff1:",diffTime)
        diffTime = diffTime << (self.workerBits + self.seqBits)
        #print ("diff:",diffTime)
        #self.curSeq = (self.curSeq + 1) % self.seqRound
        #print ("seq:",self.curSeq)
        #print ("workerId:",self.workerIdNum)
        return diffTime + self.workerIdNum + self.curSeq

    def nextIdDate(self):
        return self.transferToDate(self.nextId())

    def transferToDate(self, sfId):
        timeMilisec = (sfId >> (self.workerBits + self.seqBits)) + self.beginTime
        #print ("time:",timeMilisec)
        modMilisec = timeMilisec % 1000
        timeSec = timeMilisec / 1000
        timeArray = time.localtime(timeSec)
        dateStr = time.strftime("%Y%m%d%H%M%S", timeArray)
        seqNum = (modMilisec << (self.workerBits + self.seqBits)) + self.workerIdNum + self.curSeq
        retStr = "%s%s" % (dateStr, seqNum)
        return retStr

if __name__ == "__main__":
    snowflake = snowflakeIds(0)
    for i in range(0,2):
        sfId = snowflake.nextId()
        print (sfId)
        print (snowflake.transferToDate(sfId))
        time.sleep(0.01)
