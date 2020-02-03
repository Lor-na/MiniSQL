# !_*_ coding:utf-8 _*_

import math
import struct
import os
import CatalogManager

BUFFER_MAX_SIZE = 32


class BPlusNode(object):
    def __init__(self, index, forkNum, myPos, isRoot=False):
        self.forkNum = forkNum
        self.recordList = list()
        self.__index = index
        self.myPos = myPos
        self.__isRoot = isRoot
        self.__dirty = False
        fileName = 'index\%s_%s.txt' % (index.indexName, index.tableName)
        file = open(fileName, 'rb')
        if self.__isRoot:
            b = file.read(4)
            self.myPos, = struct.unpack('i', b)
        file.seek(self.myPos, os.SEEK_SET)
        # print("I'm reading:", file.tell())
        b = file.read(4)
        sigLeaf, = struct.unpack("i", b)
        if sigLeaf % 2 != 0:
            self.__isLeaf = True
            recordLen = int((sigLeaf - 1) / 2)
        else:
            self.__isLeaf = False
            recordLen = int(sigLeaf / 2)
        if recordLen != 0:
            pat = index.pattern()
            # print("Index reading pattern : ", pat)
            size = index.getSize()
            for i in range(recordLen):
                b = file.read(size)
                record = struct.unpack(pat, b)
                self.recordList.append(record[0])
                self.recordList.append(record[1])
            self.recordList.pop()
        file.close()
        if self.__isLeaf:
            self.__min = math.ceil((forkNum - 1) / 2)
            self.__max = forkNum - 1
        else:
            self.__min = math.ceil(forkNum / 2) - 1
            self.__max = forkNum - 1

    def nodeIsLeaf(self):
        if self.__isLeaf:
            return True
        else:
            return False

    def setDirty(self):
        self.__dirty = True
        return

    def save(self):
        if not self.__dirty:
            return
        # write back
        fileName = 'index\%s_%s.txt' % (self.__index.indexName, self.__index.tableName)
        file = open(fileName, 'rb+')
        file.seek(self.myPos, os.SEEK_SET)
        recordLen = int((len(self.recordList) + 1) / 2)
        if self.__isLeaf:
            b = struct.pack("i", recordLen * 2 + 1)
        else:
            b = struct.pack("i", recordLen * 2)
        file.write(b)
        # write record
        writeList = self.recordList
        writeList.append(self.__index.emptyRecord()[1])
        pat = self.__index.pattern()
        for i in range(0, len(writeList), 2):
            b = struct.pack(pat, writeList[i], writeList[i+1])
            file.write(b)
        file.close()

    def readRecordList(self, l):
        self.recordList = l
        self.__dirty = True

    def checkChild(self, key):
        # if is leaf, return 0
        if self.__isLeaf:
            return 0
        # if not leaf, return the pointer to leaf
        for i in range(1, len(self.recordList), 2):
            if key < self.recordList[i]:
                return self.recordList[i - 1]
        return self.recordList[-1]

    def splitNode(self):
        self.__dirty = True
        resList = list()
        if self.__isLeaf:
            for i in range(2 * self.__min, len(self.recordList)):
                resList.insert(0, self.recordList.pop())
            self.recordList.append(0)
        else:
            for i in range(2 * self.__min + 1, len(self.recordList)):
                resList.insert(0, self.recordList.pop())
            resList.insert(0, self.recordList[-1])
        return resList

    def insertRecord(self, key, value):
        self.__dirty = True
        if self.__isRoot and len(self.recordList) == 0:
            self.recordList.append(value)
            self.recordList.append(key)
            self.recordList.append(0)
            return
        for i in range(1, len(self.recordList), 2):
            if key < self.recordList[i]:
                self.recordList.insert(i - 1, key)
                self.recordList.insert(i - 1, value)
                return
        temp = self.recordList.pop()
        self.recordList.append(value)
        self.recordList.append(key)
        self.recordList.append(temp)

    def insertIndex(self, value_left, key, value_right):
        self.__dirty = True
        if self.__isRoot and len(self.recordList) == 0:
            self.recordList.append(value_left)
            self.recordList.append(key)
            self.recordList.append(value_right)
            return
        for i in range(1, len(self.recordList), 2):
            if key < self.recordList[i]:
                self.recordList.pop(i-1)
                self.recordList.insert(value_right)
                self.recordList.insert(key)
                self.recordList.insert(value_left)
                return
        self.recordList.pop()
        self.recordList.append(value_left)
        self.recordList.append(key)
        self.recordList.append(value_right)

    def deleteRecord(self, key):
        self.__dirty = True
        for i in range(1, len(self.recordList), 2):
            if key == self.recordList[i]:
                self.recordList.pop(i-1)
                self.recordList.pop(i-1)

    def checkInsert(self):
        if (len(self.recordList) - 1) / 2 < self.__max:
            return True
        else:
            return False

    def checkExtraKey(self):
        if (len(self.recordList) - 1) / 2 > self.__min:
            return True
        else:
            return False

    def checkExistKey(self, key):
        for i in range(1, len(self.recordList), 2):
            if key == self.recordList[i]:
                return True
        return False

def newIndex(index):
    # new index file
    fileName = 'index\%s_%s.txt' % (index.indexName, index.tableName)
    file = open(fileName, 'w')
    file.close()
    file = open(fileName, 'rb+')
    # root node address // freelist // end of free list
    b = struct.pack("iii", BUFFER_MAX_SIZE, 1, 0)
    file.write(b)
    file.close()
    createIndexNode(index, isLeaf=True)


def createIndexNode(index, isLeaf):
    # read free List
    fileName = 'index\%s_%s.txt' % (index.indexName, index.tableName)
    file = open(fileName, 'rb+')
    file.seek(4, os.SEEK_SET)
    freeList = list()
    while True:
        b = file.read(4)
        No, = struct.unpack("i", b)
        freeList.append(No)
        if No == 0:
            break
    No = freeList.pop(0)
    if len(freeList) == 1:
        freeList.insert(0, No + 1)
    # write new free list back
    file.seek(4, os.SEEK_SET)
    for item in freeList:
        b = struct.pack("i", item)
        file.write(b)
    file.seek(No * BUFFER_MAX_SIZE, os.SEEK_SET)
    # write empty record in
    # 开头有一个leaf指示需要四字节
    maxNum = int((BUFFER_MAX_SIZE - 4) / index.getSize())
    emptyRecord = index.emptyRecord()
    pattern = index.pattern()
    if isLeaf:
        b = struct.pack('i', 1)
    else:
        b = struct.pack('i', 0)
    file.write(b)
    b = struct.pack(pattern, *emptyRecord)
    for i in range(maxNum):
        file.write(b)
        if file.tell() % 4 != 0:
            file.seek(4 - file.tell() % 4, os.SEEK_CUR)
    file.close()
    return No


class BPlusTree(object):
    def __init__(self, index):
        self.index = index
        self.forkNum = int((BUFFER_MAX_SIZE - 4) / index.getSize())
        self.node = BPlusNode(index, self.forkNum, 0, isRoot=True)

    def addIndex(self, key, value):
        fatherList = self.gotoLeaf(key)
        # check if exist
        checked = self.node.checkExistKey(key)
        if checked:
            self.node.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return False
        checked = self.node.checkInsert()
        self.node.insertRecord(key, value)
        if checked:
            self.node.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return True
        # split Node
        No = createIndexNode(self.index, isLeaf=True)
        tempNode = BPlusNode(self.index, self.forkNum, No * BUFFER_MAX_SIZE)
        recordList = self.node.splitNode()
        self.node.recordList.pop()
        self.node.recordList.append(No * BUFFER_MAX_SIZE)
        tempNode.readRecordList(recordList)
        value_left, key, value_right = self.node.myPos, tempNode.recordList[1], tempNode.myPos
        tempNode.save()
        self.node.save()
        # insert into index Node
        while True:
            if len(fatherList) == 0:
                # current node is already root, create a new node as root
                No = createIndexNode(self.index, isLeaf=False)
                fileName = 'index\%s_%s.txt' % (self.index.indexName, self.index.tableName)
                file = open(fileName, 'rb+')
                b = struct.pack("i", No * BUFFER_MAX_SIZE)
                file.write(b)
                file.close()
                self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
                self.node.insertIndex(value_left, key, value_right)
                self.node.save()
                break
            else:
                father = fatherList.pop()
                self.node = BPlusNode(self.index, self.forkNum, father)
                checked = self.node.checkInsert()
                self.node.insertIndex(value_left, key, value_right)
                if checked:
                    self.node.save()
                    break
                No = createIndexNode(self.index, isLeaf=False)
                tempNode = BPlusNode(self.index, self.forkNum, No * BUFFER_MAX_SIZE)
                recordList = self.node.splitNode()
                tempNode.readRecordList(recordList)
                tempNode.recordList.pop(0)
                key = tempNode.recordList.pop(0)
                value_left, value_right = self.node.myPos, tempNode.myPos
                tempNode.save()
                self.node.save()
        self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
        return True

    def dropIndex(self, key):
        fatherList = self.gotoLeaf(key)
        # check if exist
        checked = self.node.checkExistKey(key)
        if not checked:
            self.node.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return False
        # check if extra
        checked = self.node.checkExtraKey()
        self.node.deleteRecord(key)
        if checked:
            self.node.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return True
        # find its brothers
        if len(fatherList) == 0:
            # is root
            self.node.save()
            return True
        # is not root
        myPos = self.node.myPos
        fatherPos = fatherList[-1]
        fatherNode = BPlusNode(self.index, self.forkNum, fatherPos)
        leftPos, rightPos = 0, 0
        for i in range(0, len(fatherNode.recordList), 2):
            if fatherNode.recordList[i] == myPos:
                if i - 2 > 0:
                    leftPos = fatherNode.recordList[i - 2]
                if i + 2 < len(fatherNode.recordList):
                    rightPos = fatherNode.recordList[i + 2]
                break
        # try to borrow left brother's record
        checked, oldKey, newKey = self.borrowFromBrother(leftPos, 'left')
        if checked:
            # use position to locate key and change it
            for i in range(1, len(fatherNode.recordList), 2):
                if fatherNode.recordList[i] == oldKey:
                    fatherNode.recordList[i] = newKey
                    break
            fatherNode.setDirty()
            fatherNode.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return True
        # try to borrow right brother's record
        checked, oldKey, borrowKey = self.borrowFromBrother(rightPos, 'right')
        if checked:
            for i in range(1, len(fatherNode.recordList), 2):
                if fatherNode.recordList[i] == oldKey:
                    fatherNode.recordList[i] = newKey
                    break
            fatherNode.setDirty()
            fatherNode.save()
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
            return True
        # merge node
        if leftPos != 0:
            # merge list
            brotherNode = BPlusNode(self.index, self.forkNum, leftPos)
            brotherNode.recordList.pop()
            brotherNode.recordList.extend(self.node.recordList)
            brotherNode.setDirty()
            # adjust father


    def borrowFromBrother(self, brotherPos, relation):
        if brotherPos == 0:
            return False, 0, 0
        brotherNode = BPlusNode(self.index, self.forkNum, brotherPos)
        if not brotherNode.checkExtraKey():
            return False, 0, 0
        # have extra Key
        brotherNode.setDirty()
        self.node.setDirty()
        if relation == 'left':
            oldKey = self.node.recordList[1]
            brotherNode.recordList.pop()
            self.node.recordList.insert(0, brotherNode.recordList.pop())
            self.node.recordList.insert(0, brotherNode.recordList.pop())
            brotherNode.recordList.append(self.node.myPos)
            borrowKey = self.node.recordList[1]
        else:
            oldKey = brotherNode.recordList[1]
            self.node.recordList.pop()
            self.node.recordList.append(brotherNode.recordList.pop(0))
            self.node.recordList.append(brotherNode.recordList.pop(0))
            self.node.recordList.append(brotherNode.myPos)
            borrowKey = brotherNode.recordList[1]
        brotherNode.save()
        self.node.save()
        return True, oldKey, borrowKey

    # def changeIndex(self, oldKey, newKey):
    #     tempNode = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
    #     while True:
    #         toPos = tempNode.checkChild(key)
    #         for i in range(1, len(tempNode.recordList), 2):
    #             if tempNode.recordList[i] == oldKey:
    #                 tempNode.recordList[i] = newKey
    #                 tempNode.setDirty()
    #                 break
    #         tempNode.save()
    #         if toPos == 0:
    #             break
    #         tempNode = BPlusNode(self.index, self.forkNum, toPos)
    #     return

    def gotoLeft(self):
        while True:
            if self.node.nodeIsLeaf():
                return
            else:
                toPos = self.node.recordList[0]
                self.node = BPlusNode(self.index, self.forkNum, toPos)

    def gotoLeaf(self, key):
        fatherList = list()
        while True:
            toPos = self.node.checkChild(key)
            if toPos == 0:
                break
            fatherList.append(self.node.myPos)
            self.node.save()
            self.node = BPlusNode(self.index, self.forkNum, toPos)
        return fatherList

    def selectvalue(self, key, mode):
        resList = set()
        if mode == '=':
            self.gotoLeaf(key)
            for i in range(1, len(self.node.recordList), 2):
                if key == self.node.recordList[i]:
                    resList.add(self.node.recordList[i-1])
                    break
        elif mode == '<':
            self.gotoLeft()
            stop = False
            while True:
                for i in range(1, len(self.node.recordList), 2):
                    if key > self.node.recordList[i]:
                        resList.add(self.node.recordList[i-1])
                    else:
                        stop = True
                        break
                if stop:
                    break
                else:
                    toPos = self.node.recordList[-1]
                    if toPos == 0:
                        break
                    else:
                        self.node = BPlusNode(self.index, self.forkNum, toPos)
        elif mode == '<=':
            self.gotoLeft()
            stop = False
            while True:
                for i in range(1, len(self.node.recordList), 2):
                    if key >= self.node.recordList[i]:
                        resList.add(self.node.recordList[i - 1])
                    else:
                        stop = True
                        break
                if stop:
                    break
                else:
                    toPos = self.node.recordList[-1]
                    if toPos == 0:
                        break
                    else:
                        self.node = BPlusNode(self.index, self.forkNum, toPos)
        elif mode == '>':
            self.gotoLeaf(key)
            valueNo = len(self.node.recordList) - 1
            for i in range(1, len(self.node.recordList), 2):
                if key < self.node.recordList[i]:
                    valueNo = i - 1
                    break
            while True:
                for i in range(valueNo, len(self.node.recordList) - 1, 2):
                    resList.add(self.node.recordList[i])
                if self.node.recordList[-1] == 0:
                    break
                else:
                    toPos = self.node.recordList[-1]
                    self.node = BPlusNode(self.index, self.forkNum, toPos)
                    valueNo = 0
        elif mode == '>=':
            self.gotoLeaf(key)
            valueNo = len(self.node.recordList) - 1
            for i in range(1, len(self.node.recordList), 2):
                if key <= self.node.recordList[i]:
                    valueNo = i - 1
                    break
            while True:
                for i in range(valueNo, len(self.node.recordList) - 1, 2):
                    resList.add(self.node.recordList[i])
                if self.node.recordList[-1] == 0:
                    break
                else:
                    toPos = self.node.recordList[-1]
                    self.node = BPlusNode(self.index, self.forkNum, toPos)
                    valueNo = 0
        self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
        return resList

    def printTree(self, i, toPos):
        if i == 0:
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)
        else:
            self.node = BPlusNode(self.index, self.forkNum, toPos)
        print(i, self.node.recordList)
        if self.node.nodeIsLeaf():
            return
        PosList = list()
        for j in range(0, len(self.node.recordList), 2):
            PosList.append(self.node.recordList[j])
        for item in PosList:
            self.printTree(i + 1, item)
        if i == 0:
            self.node = BPlusNode(self.index, self.forkNum, 0, isRoot=True)


if __name__ == '__main__':
    i = CatalogManager.index('idhhh', 'id', 'stu', 'int')
    newIndex(i)
    B = BPlusTree(i)
    B.addIndex(0, 11)
    B.printTree(0, 0)
    B.addIndex(5, 21)
    B.printTree(0, 0)
    B.addIndex(2, 31)
    B.printTree(0, 0)
    B.addIndex(4, 41)
    B.printTree(0, 0)
    B.addIndex(7, 61)
    B.printTree(0, 0)
    B.addIndex(1, 71)
    B.printTree(0, 0)
    res = B.selectvalue(2, '<')
    print(res)
