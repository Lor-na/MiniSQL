# !_*_ coding:utf-8 _*_
import CatalogManager
import struct
import os

BLOCK_MAX_SIZE = 16
BLOCK_MAX_NUM = 4


class LRUList(object):
    def __init__(self):
        self.list = []

    def updateBlock(self, i):
        # move to the head
        self.list.insert(0, self.list.pop(i))

    def insertNewBlock(self, table, No):
        if len(self.list) == BLOCK_MAX_NUM:
            self.popBlock()
        # a new block
        r = recordBlock(table)
        r.read(No)
        self.list.insert(0, r)

    def dropBlock(self, tableName):
        for i in range(len(self.list)):
            if self.list[i].table.name == tableName:
                self.list.pop(i)

    def popBlock(self):
        l = self.list.pop()
        l.save()

    def printBlockList(self):
        print("I have %d buffers" % len(self.list))
        print("they are :")
        for item in self.list:
            print(item.table.name, "block", item.blockNo)

    def saveAllBuffer(self):
        for i in range(len(self.list)):
            if self.list[i].dirty:
                self.list[i].save()


bufferList = LRUList()


class recordBlock(object):
    def __init__(self, table):
        self.recordMaxNum = int(BLOCK_MAX_SIZE / table.getSize())
        self.recordLen = self.recordMaxNum * table.getSize()
        self.table = table
        self.blockNo = 1
        self.dirty = False
        self.initList()

    def initList(self):
        self.recordList = [[] for x in range(self.recordMaxNum)]
        self.freeList = [x for x in range(0, self.recordMaxNum)]

    def read(self, No):
        self.blockNo = No
        fileName = "record\%s.txt" % self.table.name
        file = open(fileName, 'rb')
        file.seek(4, os.SEEK_CUR)
        for i in range(0, No - 1):
            file.seek(4, os.SEEK_CUR)
            b = file.read(4)
            offset, = struct.unpack('i', b)
            file.seek(offset, os.SEEK_CUR)
        # just for test
        b = file.read(4)
        cur, = struct.unpack('i', b)
        b = file.seek(4, os.SEEK_CUR)
        # read free list
        b = file.read(4)
        freeLen, = struct.unpack('i', b)
        pattern = ''
        for i in range(freeLen):
            pattern += 'i'
        b = file.read(4 * freeLen)
        temp = struct.unpack(pattern, b)
        self.freeList = list(temp)
        # print(self.freeList)
        if self.recordMaxNum - freeLen != 0:
            file.seek((self.recordMaxNum - freeLen) * 4, os.SEEK_CUR)
        # print(file.tell())
        # read Record
        pattern = self.table.pattern()
        for i in range(self.recordMaxNum):
            b = file.read(self.table.getSize())
            temp = struct.unpack(pattern, b)
            self.recordList[i] = list(temp[:-1])
        self.recordList = self.table.charDecoding(self.recordList)
        # print(self.recordList)
        file.close()

    def addRecord(self, record):
        # judge if there is other space
        if len(self.freeList):
            index = self.freeList[0]
            del self.freeList[0]
            self.recordList[index] = record
            self.dirty = True
            return True
        else:
            return False

    def deleteRecord(self, i):
        self.recordList[i] = self.table.EmptyRecord()
        self.freeList.insert(0, i)
        for j in range(len(self.freeList) - 1):
            if self.freeList[j] > self.freeList[j+1]:
                self.freeList[j], self.freeList[j+1] = self.freeList[j+1], self.freeList[j]
            else:
                break
        self.dirty = True

    def save(self):
        if not self.dirty:
            return
        fileName = "record\%s.txt" % self.table.name
        file = open(fileName, 'rb+')
        file.seek(4, os.SEEK_CUR)
        for i in range(0, self.blockNo - 1):
            file.seek(4, os.SEEK_CUR)
            b = file.read(4)
            offset, = struct.unpack('i', b)
            file.seek(offset, os.SEEK_CUR)
        # just for test
        b = file.read(4)
        cur, = struct.unpack('i', b)
        b = file.seek(4, os.SEEK_CUR)
        # write free list
        b = struct.pack('i', len(self.freeList))
        file.write(b)
        pattern = ''
        for i in range(len(self.freeList)):
            pattern += 'i'
        b = struct.pack(pattern, *self.freeList)
        file.write(b)
        if self.recordMaxNum - len(self.freeList) != 0:
            file.seek((self.recordMaxNum - len(self.freeList)) * 4, os.SEEK_CUR)
        # print(file.tell())
        # write records
        pattern = self.table.pattern()
        tempList = self.table.charEncoding(self.recordList)
        for i in range(self.recordMaxNum):
            temp = tempList[i]
            temp.append(0)
            b = struct.pack(pattern, *temp)
            file.write(b)
            if file.tell() % 4 != 0:
                file.seek(4 - file.tell() % 4, os.SEEK_CUR)
        file.close()


    def printRecord(self):
        print("recordList:", self.recordList)
        print("freeList:", self.freeList)


def newRecordBlock(table):
    # an empty block
    recordMaxNum = int(BLOCK_MAX_SIZE / table.getSize())
    recordLen = recordMaxNum * table.getSize()
    freeList = [x for x in range(0, recordMaxNum)]
    # open file
    fileName = 'record\%s.txt' % table.name
    file = open(fileName, 'rb+')
    b = file.read(4)
    No, = struct.unpack('i', b)
    file.seek(0, os.SEEK_SET)
    b = struct.pack('i', No + 1)
    file.write(b)
    for i in range(0, No):
        file.seek(4, os.SEEK_CUR)
        b = file.read(4)
        offset, = struct.unpack('i', b)
        file.seek(offset, os.SEEK_CUR)
    # write block No and offset and free list length
    offset = len(freeList) * 4 + 4 + recordLen
    b = struct.pack('iii', No + 1, offset, len(freeList))
    file.write(b)
    # write free list
    pattern = ''
    for i in range(len(freeList)):
        pattern += 'i'
    b = struct.pack(pattern, *freeList)
    file.write(b)
    # space for record
    pattern = table.pattern()
    ansList = table.charEncoding([table.EmptyRecord(), ])
    record = ansList[0]
    record.append(0)
    b = struct.pack(pattern, *record)
    for i in range(recordMaxNum):
        file.write(b)
        if file.tell() % 4 != 0:
            file.seek(4 - file.tell() % 4, os.SEEK_CUR)
    file.close()


if __name__ == "__main__":
    t = CatalogManager.table('test')
    t.addColumn('name', 'char', len=2)
    # print(t.pattern(), t.getSize())
    # newTable(t)
    # print(t.EmptyRecord())
    # CatalogManager.readTables()
    # r = recordBlock(CatalogManager.tables[0])
    # r.read(1)
    # r.printRecord()
    # r.addRecord(['yy'])
    #     # r.addRecord(['hh'])
    #     # r.save()
    # r.deleteRecord(1)
    # r.printRecord()
