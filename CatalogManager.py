# !_*_ coding:utf-8 _*_
import json
import os
import re
from interpreter import SQLException

tables = []


class table:
    def __init__(self, tableName, primaryKey=0):
        self.name = tableName
        self.__primaryKey = primaryKey
        self.__col = []
        self.__size = 0
        self.blockNum = 1
        self.indexList = list()

    def save(self):
        fileDict = dict()
        fileDict['name'] = self.name
        fileDict['primaryKey'] = self.__primaryKey
        fileDict['column'] = self.__col
        fileDict['size'] = self.__size
        fileDict['blockNum'] = self.blockNum
        tempList = list()
        for item in self.indexList:
            indexInfoList = [item.indexName, item.attrName, item.type, item.len]
            tempList.append(indexInfoList)
        fileDict['index'] = tempList
        file = open('catalog\%s.txt' % self.name, 'w')
        file.write(json.dumps(fileDict))
        file.close()

    def read(self, fileName):
        fileDict = dict()
        file = open('catalog\%s' % fileName, 'r')
        data = json.loads(file.read())
        file.close()
        self.name = data['name']
        self.__primaryKey = data['primaryKey']
        self.__col = data['column']
        self.__size = data['size']
        self.blockNum = data['blockNum']
        tempList = data['index']
        for item in tempList:
            ind = index(item[0], item[1], self.name, item[2], item[3])
            self.indexList.append(ind)
        # self.printTable()

    def addIndex(self, indexName, attrName):
        # check other index
        for item in self.indexList:
            if item.indexName == indexName:
                print("Create Error : the index name has already existed")
                raise SQLException()
            if item.attrName == attrName:
                print("Create Error : the attribute already has an index")
                raise SQLException()
        # check integrity
        find = False
        for item in self.__col:
            if attrName == item['name']:
                find = True
                if not item['isUnique']:
                    print("Create Error : the attribute is not unique")
                    raise SQLException()
                # create and add in list
                i = index(indexName, attrName, item['type'], item['len'])
                self.indexList.append(i)
        if not find:
            print("Create Error : No such attribute exists")
            raise SQLException()

    def addColumn(self, name, type, isUnique=False, len=0):
        tempDict = {'name': name, 'type': type, 'isUnique': isUnique, 'len': len}
        self.__col.append(tempDict)
        if type == 'char':
            self.__size += len
        else:
            self.__size += 4

    def getSize(self):
        # 为了使读写都为4的整数倍加了一个整数0在每条记录的最后所以多加一个4
        if self.__size % 4 != 0:
            return (int(self.__size / 4) + 1) * 4 + 4
        else:
            return self.__size + 4

    def setUnique(self, name):
        for i in range(len(self.__col)):
            if self.__col[i]['name'] == name:
                self.__col[i]['isUnique'] = True

    def printTable(self):
        print("name", self.name)
        print("attr", self.__col)
        print("pri", self.__primaryKey)
        print("blockNum", self.blockNum)

    def checkIntegrity(self, record):
        result = list()
        for i in range(len(self.__col)):
            if self.__col[i]['type'] == 'char':
                match = re.match(r"'(.*)'", record[i])
                if not match:
                    print("Type Error : %s should be type char" % record[i])
                    raise SQLException()
                if len(record[i]) - 2 > self.__col[i]['len']:
                    print("Type Error : %s is too long" % record[i])
                    raise SQLException
                result.append(record[i].strip("'"))
            elif self.__col[i]['type'] == 'int':
                if not record[i].isdigit():
                    print("Type Error : %s should be type int" % record[i])
                    raise SQLException()
                result.append(int(record[i]))
            else:
                match = re.match("(\d+(\.\d+)?)", record[i])
                if match:
                    if match.group(0) != record[i]:
                        print("Type Error : %s should be type float" % record[i])
                        raise SQLException()
                else:
                    print("Type Error : %s should be type float" % record[i])
                    raise SQLException()
                result.append(float(record[i]))
        return result

    def checkCond(self, cond):
        res = list()
        s = self.getAttrList()
        for item in cond:
            find = False
            for i in range(len(self.__col)):
                if self.__col[i]['name'] == item[0]:
                    find = True
                    if self.__col[i]['type'] == 'char':
                        if item[1] != '=':
                            print("Operator Error : type char cannot be compared")
                            raise SQLException()
                        match = re.match(r"'(.*)'", item[2])
                        if not match:
                            print("Type Error : %s should be type char" % item[2])
                            raise SQLException()
                        res.append([item[0], item[1], item[2].strip("'")])
                    elif self.__col[i]['type'] == 'int':
                        if not item[2].isdigit():
                            print("Type Error : %s should be type int" % item[2])
                            raise SQLException()
                        res.append([item[0], item[1], int(item[2])])
                    else:
                        match = re.match("(\d+(\.\d+)?)", item[2])
                        if match:
                            if match.group(0) != item[2]:
                                print("Type Error : %d should be type float" % item[2])
                                raise SQLException()
                        else:
                            print("Type Error : %d should be type float" % item[2])
                            raise SQLException()
                        res.append([item[0], item[1], float(item[2])])
            if not find:
                print("Attribute Error : %s is not in this table" % item[0])
                raise SQLException()
        return res

    def getAttrList(self):
        res = list()
        for item in self.__col:
            res.append(item['name'])
        return res

    def getAttrInfo(self, attrName):
        for item in self.__col:
            if item['name'] == attrName:
                attrType = item['type']
                attrLen = item['len']
                attrUnique = item['isUnique']
                return attrType, attrLen, attrUnique
        return '', 0, False

    def getAttrNo(self, attrName):
        for i in range(len(self.__col)):
            if attrName == self.__col[i]['name']:
                return i
        return -1

    def getAttrWithIndex(self):
        resList = list()
        for index in self.indexList:
            resList.append(index.attrName)
        return resList

    def getUniqueList(self):
        res = list()
        for i in range(len(self.__col)):
            tempDict = dict()
            if self.__col[i]['isUnique']:
                tempDict['name'] = self.__col[i]['name']
                tempDict['index'] = i
                res.append(tempDict)
        return res

    def pattern(self):
        pat = ''
        for item in self.__col:
            if item['type'] == 'char':
                pat += '%ds' % item['len']
            elif item['type'] == 'int':
                pat += 'i'
            else:
                pat += 'f'
        pat += 'i'
        return pat

    def EmptyRecord(self):
        record = []
        for item in self.__col:
            if item['type'] == 'char':
                temp = ''
                for i in range(item['len']):
                    temp += '0'
                record.append(temp)
            elif item['type'] == 'int':
                record.append(0)
            else:
                record.append(1.0)
        return record

    def charEncoding(self, l):
        tempList = []
        for i in range(len(l)):
            temp = []
            for j in range(len(self.__col)):
                if self.__col[j]['type'] == 'char':
                    fixLen = self.__col[j]['len'] - len(l[i][j])
                    tempChar = l[i][j]
                    for i in range(fixLen):
                        tempChar += '#'
                    temp.append(tempChar.encode(encoding='utf-8'))
                else:
                    temp.append(l[i][j])
            tempList.append(temp)
        return tempList

    def charDecoding(self, l):
        tempList = []
        for i in range(len(l)):
            temp = []
            for j in range(len(self.__col)):
                if self.__col[j]['type'] == 'char':
                    temp.append(l[i][j].decode(encoding='utf-8').strip('#'))
                else:
                    temp.append(l[i][j])
            tempList.append(temp)
        return tempList

class index(object):
    def __init__(self, indexName, attrName, tableName, type, len=0):
        self.indexName = indexName
        self.tableName = tableName
        self.attrName = attrName
        self.type = type
        self.len = len
        self.setSize()

    def setSize(self):
        if self.type == 'char':
            self.size = 4 + self.len
        else:
            self.size = 8

    def getSize(self):
        if self.size % 4 != 0:
            return (int(self.size / 4) + 1) * 4
        else:
            return self.size

    def pattern(self):
        pat = 'i'
        if self.type == 'char':
            pat += '%ds' % self.len
        elif self.type == 'int':
            pat += 'i'
        else:
            pat += 'f'
        return pat

    def emptyRecord(self):
        record = list()
        record.append(0)
        if self.type == 'char':
            temp = ''
            for i in range(self.len):
                temp += '0'
            record.append(temp)
        elif self.type == 'int':
            record.append(0)
        else:
            record.append(1.0)
        return record


def readTables():
    filePath = 'catalog\\'
    fileList = os.listdir(filePath)
    for item in fileList:
        t = table('')
        t.read(item)
        tables.append(t)

def saveTables():
    for i in range(len(tables)):
        tables[i].save()


if __name__ == "__main__":
    # t = table('test')
    # t.addColumn('name', 'char', len=2)
    # t.addColumn('id', 'int')
    # t.addColumn('money', 'float')
    # t.setUnique('id')
    # t.save()
    t = table('')
    t.read('stu.txt')
    cond = [['id', '>=', '2'], ['name', '=', "'3'"]]
    res = t.checkCond(cond)
    print(res)


