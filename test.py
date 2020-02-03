# !_*_ coding:utf-8 _*_
import struct
import os
import re


def test():
    n = 3
    f = 4.4
    s = 'lala'
    l = [s.encode(encoding='utf-8'), f, n]
    b = struct.pack('6sfi', *l)
    f = open('test.txt', 'rb+')
    print(f.tell())
    f.write(b)
    f.close()

def testread():
    file = open('test.txt', 'rb')
    b = file.read(6)
    s= struct.unpack('6s', b)
    print(s, f, n)
    file.close()

def allread():
    file = open('test.txt', 'rb')
    b = file.read()
    print(b)


if __name__ == '__main__':
    # test()
    # testread()
    # allread()
    # strl = "a = 1"
    # match = re.match(r"(\w+)\s?([>=<]+)\s?('?\w+'?)", strl)
    # if match:
    #     print(match.groups())
    # filePath = "catalog\\"
    # print(os.listdir(filePath))
    # l = [[], ['a', 'b']]
    # l1 = l
    # l1.append(['hahha'])
    # print(l)
    a = '1.12a'
    # print(type(eval(a)))
    # if '.' in a:
    #     print('hahha')
    # a = '1.12'
    # match = re.match("(\d+(\.\d+)?)", a)
    # if match:
    #     print(match.group(0))
    # Nolist = list(range(1, 5 + 1))
    # Nolist.insert(0, Nolist.pop(4))
    # print(Nolist)
    # l = [0 , 1, 4]
    # l = [0, 1, 3]
    # a = list()
    # a.append(l[0])
    # print(a)
    # a.clear()
    # print(l)
    # a = 'a234235'
    # b = 'bwfwf'
    # if a < b:
    #     print("yes")
    # fileName = 'testnew.txt'
    # file = open(fileName, 'w')
    # file.close()
    # file = open(fileName, 'rb+')
    # b = file.read(4)
    # a, = struct.unpack("i", b)
    # print(a)
    a = list()
    a.append(1)
    a.append(1)
    a.append(1)
    b = set()
    b.add(1)
    b.add(3)
    b.add(2)
    print(a, b[2])


