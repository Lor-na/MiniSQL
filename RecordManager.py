# !_*_ coding:utf-8 _*_

from BufferManager import *
from interpreter import SQLException
import IndexManager

def newTable(table):
    # new record file
    fileName = 'record\%s.txt' % table.name
    file = open(fileName, 'w')
    file.close()
    file = open(fileName, 'rb+')
    b = struct.pack("i", 0)
    file.write(b)
    file.close()
    newRecordBlock(table)


def newRecord(tableName, record):
    # check tableName
    find = False
    for i in range(len(CatalogManager.tables)):
        if CatalogManager.tables[i].name == tableName:
            t = CatalogManager.tables[i]
            find = True
            break
    if not find:
        print("Insert Error : the table doesn't exist")
        raise SQLException()
    # check the integrity of record
    result = t.checkIntegrity(record)
    # check unique
    uniqueList = t.getUniqueList()
    if len(uniqueList) > 0:
        cond = list()
        for item in uniqueList:
            tempList = [item['name'], '=', result[item['index']]]
            cond.append(tempList)
        selectRes = traversalRecord(t, cond)
        if len(selectRes) > 0:
            print("Insert Error : the value is not unique")
            raise SQLException()
    # build No list for query
    Nolist = list(range(1, t.blockNum + 1))
    # write record
    find = False
    l = bufferList.list
    for i in range(len(l)):
        if tableName == l[i].table.name:
            Nolist.remove(l[i].blockNo)
            checked = l[i].addRecord(result)
            if checked:
                blockNo = l[i].blockNo
                find = True
                break
    # if there isn't appropriate block, update block
    if not find:
        for i in range(len(Nolist)):
            bufferList.insertNewBlock(t, Nolist[i])
            checked = bufferList.list[0].addRecord(result)
            if checked:
                blockNo = Nolist[i]
                find = True
                break
    # if all the block is full, add new
    if not find:
        newRecordBlock(t)
        t.blockNum += 1
        bufferList.insertNewBlock(t, t.blockNum)
        checked = bufferList.list[0].addRecord(result)
        if checked:
            blockNo = bufferList.list[0].blockNo
        else:
            pass
    return result, blockNo


def delRecord(tableName, cond):
    # check tableName
    find = False
    for i in range(len(CatalogManager.tables)):
        if CatalogManager.tables[i].name == tableName:
            t = CatalogManager.tables[i]
            find = True
            break
    if not find:
        print("Select Error : the table doesn't exist")
        raise SQLException()
    # check cond
    condRes = t.checkCond(cond)
    # traverse
    checked = traversalRecord(t, condRes, delete=True)
    if checked:
        print("Delete Succeed")

def selectRecord(tableName, attr, cond):
    # check tableName
    find = False
    for i in range(len(CatalogManager.tables)):
        if CatalogManager.tables[i].name == tableName:
            t = CatalogManager.tables[i]
            find = True
            break
    if not find:
        print("Select Error : the table doesn't exist")
        raise SQLException()
    # check attr
    standardList = t.getAttrList()
    if len(attr) == 1 and attr[0] == '*':
        attr = standardList
    else:
        for i in range(len(attr)):
            if attr[i] not in standardList:
                print("Select Error: the column %s doesn't exist" % attr[i])
                raise SQLException()
    # check cond
    condRes = t.checkCond(cond)
    # check index
    attrWithIndex = t.getAttrWithIndex()
    searchAttr = ''
    for item in condRes:
        if item[0] in attrWithIndex:
            searchAttr = item[0]
            key = item[2]
            mode = item[1]
            break
    # traverse
    if searchAttr != '':
        for i in range(len(t.indexList)):
            if t.indexList[i].attrName == searchAttr:
                ind = t.indexList[i]
        B = IndexManager.BPlusTree(ind)
        blockList = B.selectvalue(key, mode)
        traverRes = traversalRecordWithIndex(t, condRes, blockList)
    else:
        traverRes = traversalRecord(t, condRes)
    # return the needed attribute
    if len(traverRes) == 0:
        return traverRes
    selectRes = list()
    for i in range(len(traverRes)):
        tempDict = dict()
        for item in attr:
            tempDict[item] = traverRes[i][item]
        selectRes.append(tempDict)
    return selectRes


def traversalRecordWithIndex(t, cond, blockList):
    res = list()
    # find in bufferList
    l = bufferList.list
    for i in range(len(l)):
        if l[i].table.name == t.name and l[i].blockNo in blockList:
            blockList.remove(l[i].blockNo)
            for j in range(l[i].recordMaxNum):
                if j not in l[i].freeList:
                    tempDict = checkOneRecord(t, l[i].recordList[j], cond)
                    if tempDict is not None:
                        res.append(tempDict)
            bufferList.updateBlock(i)
    # load the other blocks
    for i in blockList:
        bufferList.insertNewBlock(t, i)
        for j in range(l[0].recordMaxNum):
            if j not in l[0].freeList:
                tempDict = checkOneRecord(t, l[0].recordList[j], cond)
                if tempDict is not None:
                    res.append(tempDict)
    return res


def traversalRecord(t, cond, delete=False, withBlockNo=False):
    res = list()
    Nolist = list(range(1, t.blockNum + 1))
    # find in bufferList
    l = bufferList.list
    for i in range(len(l)):
        if l[i].table.name == t.name:
            Nolist.remove(l[i].blockNo)
            for j in range(l[i].recordMaxNum):
                if j not in l[i].freeList:
                    tempDict = checkOneRecord(t, l[i].recordList[j], cond)
                    if tempDict is not None:
                        if delete:
                            l[i].deleteRecord(j)
                        else:
                            if withBlockNo:
                                tempDict['BlockNo'] = l[i].blockNo
                            res.append(tempDict)
            bufferList.updateBlock(i)
    # load the other blocks
    for i in range(len(Nolist)):
        bufferList.insertNewBlock(t, Nolist[i])
        for j in range(l[0].recordMaxNum):
            if j not in l[0].freeList:
                tempDict = checkOneRecord(t, l[0].recordList[j], cond)
                if tempDict is not None:
                    if delete:
                        l[0].deleteRecord(j)
                    else:
                        if withBlockNo:
                            tempDict['BlockNo'] = l[0].blockNo
                        res.append(tempDict)
    if delete:
        return True
    return res


def checkOneRecord(t, record, cond):
    # build the dict
    d = dict()
    l = t.getAttrList()
    for i in range(len(l)):
        d[l[i]] = record[i]
    # print(d)
    # check the cond
    meet = True
    for item in cond:
        if item[1] == '=':
            if d[item[0]] != item[2]:
                meet = False
                break
        elif item[1] == '>':
            if d[item[0]] <= item[2]:
                meet = False
                break
        elif item[1] == '>=':
            if d[item[0]] < item[2]:
                meet = False
                break
        elif item[1] == '<':
            if d[item[0]] >= item[2]:
                meet = False
                break
        else:
            if d[item[0]] > item[2]:
                meet = False
                break
    if meet:
        return d
    else:
        return None


if __name__ == "__main__":
    # CatalogManager.readTables()
    # r = recordBlock(CatalogManager.tables[0])
    # r.read(3)
    # # r.printRecord()
    # bufferList.list.append(r)
    # # newRecord('stu', ['0', "'err'"])
    # # newRecord('stu', ['0', "'yyk'"])
    # # newRecord('stu', ['1', "'wjy'"])
    # # newRecord('stu', ['2', "'szx'"])
    # # bufferList.saveAllBuffer()
    # cond = [['id', '>=', '1']]
    # attr = ['id', 'name']
    # tableName = 'stu'
    # res = selectRecord(tableName, attr, cond)
    t = CatalogManager.table('test')
    t.addColumn('name', 'char', len=2)
    newTable(t)




