# !_*_ coding:utf-8 _*_
import re


class SQLException(Exception):
    def __init__(self, err=''):
        Exception.__init__(self, err)


class Select(object):
    def __init__(self):
        self.attr = []
        self.table = ''
        self.cond = []
        self.operator = ['=', '>', '<', '>=', '<=']

    def print(self):
        print("attr:", self.attr)
        print("tableName:", self.table)
        print("cond:", self.cond)

    def setAttr(self, sql):
        match = re.search(r"select (.*) from", sql)
        if match:
             attrList = match.group(1).split(',')
             for i in range(len(attrList)):
                 self.attr.append(attrList[i].strip())
        else:
            print("Syntax Error near 'select'")
            raise SQLException()

    def setTableName(self, sql):
        match = re.search(r"from (\w+)[\s;]", sql)
        if match:
            self.table = match.group(1).strip()
        else:
            print("Syntax Error near 'from'")
            raise SQLException()

    def setCond(self, sql):
        if 'where' not in sql:
            return []
        match = re.search(r"where (.*);", sql)
        if match:
            condList = match.group(1).split('and')
            for i in range(len(condList)):
                matchCond = re.match(r"(\w+)\s?([><=]+)\s?('?\w+'?)", condList[i].strip())
                if matchCond:
                    # string only deserved = but not >/<
                    if "'" in matchCond.group(3) and matchCond.group(2) != '=':
                        print("Syntax Error : string can only be judged equal or not")
                        raise SQLException()
                    # check operator
                    if matchCond.group(2) not in self.operator:
                        print("Syntax Error : invalid operator")
                        raise SQLException()
                    self.cond.append(list(matchCond.groups()))
                else:
                    print("Syntax Error : condition '%s' is invalid" % condList[i].strip())
                    raise SQLException()
        else:
            print("Syntax Error near 'where'")
            raise SQLException()

class CreateT(object):
    def __init__(self):
        self.name = ''
        self.attr = {}
        self.pri = ''
        self.uni = []
        self.type = ['int', 'float']

    def print(self):
        print("name", self.name)
        print("attr", self.attr)
        print("pri", self.pri)
        print("uni", self.uni)

    def new_table(self, sql):
        self.match_table(sql)
        self.match_attr(sql)

    def match_table(self, sql):
        match = re.search(r"table (\w+)\s?[(]", sql)
        if match:
            self.name = match.group(1)
        else:
            print("Syntax Error: Only table or index can be created")
            raise SQLException()

    def match_attr(self, sql):
        match = re.search(r"[(](.*)[)]", sql)
        if match:
            defList = match.group(1).split(',')
            for item in defList:
                matchAttr = re.search(r"(\w+)\s(\w+)\s?(\w+)?", item.strip())
                if matchAttr:
                    if matchAttr.group(1) == 'primary':
                        # handle primary key
                        matchPri = re.search(r"primary key [(](\w+)[)]", item.strip())
                        if matchPri and matchPri.group(1) in self.attr:
                            self.pri = matchPri.group(1)
                        else:
                            print("Error in using primary key : '%s'" % item.strip())
                            raise SQLException()
                    elif matchAttr.group(2) == 'char':
                        # handle attr in type char
                        matchChar = re.search(r"(\w+)\s(\w+)\s?[(](\d+)[)]\s?(\w+)?", item.strip())
                        if matchChar:
                            self.attr[matchChar.group(1)] = matchChar.group(3)
                            if int(matchChar.group(3)) > 255 or int(matchChar.group(3)) < 0:
                                print("Error: the size of variable in type char out of domain")
                                raise SQLException()
                            if matchChar.group(4) == 'unique':
                                self.uni.append(matchChar.group(1))
                            elif matchChar.group(4) is not None:
                                print("Syntax Error near '%s'" % matchChar.group(4))
                                raise SQLException()
                        else:
                            print("Syntax Error near '%s'" % item.strip())
                            raise SQLException()
                    elif matchAttr.group(2) in self.type:
                        # handle attr in type int or float
                        self.attr[matchAttr.group(1)] = matchAttr.group(2)
                        if matchAttr.group(3) == 'unique':
                            self.uni.append(matchAttr.group(1))
                        elif matchAttr.group(3) is not None:
                            print("Syntax Error near '%s'" % matchAttr.group(3))
                            raise SQLException()
                    else:
                        print("Illegal varible type or misspelling of words")
                        raise SQLException()
                else:
                    print("Syntax Error in '%s'" % item)
                    raise SQLException()
        else:
            print("Syntax Error: lack of parenthesis")
            raise SQLException()


def dropTable(sql):
    match = re.search(r"table (\w+)\s?;", sql)
    if match:
        tableName = match.group(1).strip(' ')
        return tableName
    else:
        print("Syntax Error : please check your instruction")
        raise SQLException()

def dropIndex(sql):
    match = re.match(r"drop index (\w+)\s?;", sql)
    if match:
        indexName = match.group(1)
        return indexName
    else:
        print("Syntax Error : please check your instruction")

class insertRecord(object):
    def __init__(self):
        self.tableName = ''
        self.record = []

    def print(self):
        print("tableName : ", self.tableName)
        print("record : ", self.record)

    def insertRecord(self, sql):
        match = re.match(r"insert into (\w+) values\s?[(](.*)[)]\s?;", sql)
        if match:
            self.tableName = match.group(1)
            # handle record
            tempList = match.group(2).split(',')
            for item in tempList:
                self.record.append(item.strip())
        else:
            print("Syntax Error : please check your instruction")
            raise SQLException()


class deleteRecord(Select):
    def __init__(self):
        super().__init__()

def createIndex(sql):
    match = re.match(r"create index (\w+) on (\w+)\s?[(](.*)[)]\s?;", sql)
    if match:
        indexName = match.group(1)
        tableName = match.group(2)
        attrName = match.group(3).strip()
        return indexName, tableName, attrName
    else:
        print("Syntax Error : please check your instruction")
        raise SQLException()
