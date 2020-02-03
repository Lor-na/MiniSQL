# !_*_ coding:utf-8 _*_
from CatalogManager import *
from BufferManager import *
from RecordManager import *
from interpreter import *
from IndexManager import *
import prettytable


def createTable(created):
    # duplicate of name judgement
    for item in tables:
        if created.name == item.name:
            print("Create Error : The table %s is already existed" % created.name)
            raise SQLException()
    # build table
    t = table(created.name, created.pri)
    for key in created.attr:
        if created.attr[key].isdigit():
            t.addColumn(key, 'char', len=int(created.attr[key]))
        else:
            t.addColumn(key, created.attr[key])
    for item in created.uni:
        t.setUnique(item)
    tables.append(t)
    return t


def createNewIndex(indexName, tableName, attrName):
    # check tableName
    find = False
    for i in range(len(tables)):
        if tableName == tables[i].name:
            find = True
            # check attrName
            t = tables[i]
            attrList = tables[i].getAttrList()
            if attrName not in attrList:
                print("Create Error : attribute doesn't exist")
                raise SQLException()
            # check indexName
            checked = True
            for item in tables[i].indexList:
                if item.indexName == indexName:
                    checked = False
                    break
            if not checked:
                print("Create Error : Index name already doesn't exist")
                raise SQLException()
            attrType, attrLen, attrUnique = tables[i].getAttrInfo(attrName)
            if not attrUnique:
                print("Create Error : attribute is not unique")
                raise SQLException()
            break
    if not find:
        print("Create Error : table doesn't exist")
        raise SQLException()
    thisIndex = index(indexName, attrName, tableName, attrType, attrLen)
    for i in range(len(tables)):
        if tableName == tables[i].name:
            tables[i].indexList.append(thisIndex)
            break
    newIndex(thisIndex)
    # add index
    # find attr with blockNo
    sql = 'select %s from %s;' % (attrName, tableName)
    s = Select()
    s.setAttr(sql)
    s.setTableName(sql)
    s.setCond(sql)
    condRes = t.checkCond(s.cond)
    res = traversalRecord(t, condRes, withBlockNo=True)
    # add index into
    B = BPlusTree(thisIndex)
    for item in res:
        B.addIndex(item[attrName], item['BlockNo'])

def addintoIndex(record, tableName, blockNo):
    for i in range(len(tables)):
        if tables[i].name == tableName:
            t = tables[i]
    for item in t.indexList:
        attrName = item.attrName
        attrNo = t.getAttrNo(attrName)
        key = record[attrNo]
        B = BPlusTree(item)
        B.addIndex(key, blockNo)


def printTableWidget(dictList):
    if len(dictList) == 0:
        print("Empty Set")
        return
    attrList = dictList[0].keys()
    tb = prettytable.PrettyTable()
    tb.field_names = attrList
    for item in dictList:
        tempList = list()
        for attr in attrList:
            tempList.append(item[attr])
        tb.add_row(tempList)
    print(tb)
    print("Query OK, time 0.03s")

def interpret(sql):
    match = re.match("quit\s?;", sql)
    if match:
        raise EOFError
    sql = ' '.join(sql.split())
    sig = re.match(r"(.*?) (.*?) ", sql)
    if sig is None:
        print("Syntax Error")
        raise SQLException
    op = sql.split(" ")[0]
    ob = sql.split(" ")[1]
    if op == 'create' and ob == 'table':
        c = CreateT()
        c.new_table(sql)
        t = createTable(c)
        t.save()
        newTable(t)
        print("Create Succeed")
    elif op == 'create' and ob == 'index':
        indexName, tableName, attrName = createIndex(sql)
        createNewIndex(indexName, tableName, attrName)

    elif op == 'drop' and ob == 'table':
        tableName = dropTable(sql)
        # judge if exist and remove from tables
        exist = False
        for i in range(len(tables)):
            if tableName == tables[i].name:
                exist = True
                del tables[i]
        if not exist:
            print("Drop Error : the table doesn't exist")
            raise SQLException()
        # remove from bufferList
        bufferList.dropBlock(tableName)
        # delete text
        os.remove('catalog\%s.txt' % tableName)
        os.remove('record\%s.txt' % tableName)
        print("Drop Success")
    elif op == 'drop' and ob == 'index':
        indexName = dropIndex(sql)
        for i in range(len(tables)):
            for j in range(len(tables[i].indexList)):
                if tables[i].indexList[j].indexName == indexName:
                    fileName = 'index\%s_%s.txt' % (indexName, tables[i].name)
                    os.remove(fileName)
                    tables[i].indexList.pop(j)
        print("Drop Success")
    elif op == 'select':
        s = Select()
        s.setAttr(sql)
        s.setTableName(sql)
        s.setCond(sql)
        res = selectRecord(s.table, s.attr, s.cond)
        printTableWidget(res)
    elif op == 'insert':
        i = insertRecord()
        i.insertRecord(sql)
        res, blockNo = newRecord(i.tableName, i.record)
        addintoIndex(res, i.tableName, blockNo)
        print("Insert Success")
    elif op == 'delete':
        d = deleteRecord()
        d.setCond(sql)
        d.setTableName(sql)
        delRecord(d.table, d.cond)
        print("Delete Success")
    elif op == 'quit':
        raise EOFError
    else:
        print("Syntax Error: invalid instruction")
        raise SQLException()


if __name__ == "__main__":
    readTables()
    sql = "select id from stu where id >= 0;"
    interpret(sql)
