#coding:__utf8__
#!/bin/env python

import time
import sys
import beanstalkc
import MySQLdb as mysql
import json

insertTimeTriger = 1
insertUserCountTriger = 100

lastInsertTime = time.time()
lastInsertUserCount = 0
messageCount = 0

insertSQL = 'INSERT INTO niupu_user(id,username,password,nickname) '


def insertToMySQL(insertSQL):
    SQL = insertSQL
    print 'SQL: %s' % SQL
    try:
        mysqlConn = mysql.connect(host='192.168.11.232',user='niupu',passwd='niupu',db='niupu',port=3306) 
        mysqlCur = mysqlConn.cursor()
        mysqlCur.execute(SQL)
        mysqlCur.close()
        mysqlConn.close()
        print "OK"
    except mysql.Error,e:
        print "MySQL Error %d: %s" % (e.args[0], e.args[1])


def getUser(host, port, tube):
    beanstalk = beanstalkc.Connection(host=host, port=int(port))
    beanstalk.use(tube)
    beanstalk.watch(tube)
    beanstalk.ignore('default')
    #print beanstalk.using(), beanstalk.watching()
    global messageCount,lastInsertUserCount,lastInsertTime,insertSQL

    while True:
        message = beanstalk.reserve(timeout=0)
        if message is not None:
            messageCount += 1
            lastInsertUserCount += 1
            data = json.loads(message.body)
            userId = data['id']
            userName = data['username']
            password = data['password']
            nickName = data['nickname']

            #print userId
            
            if messageCount == 1:
                insertSQL = insertSQL + "VALUES('%s','%s','%s','%s')" % (userId,userName,password,nickName)
            else:
                insertSQL = insertSQL + ",('%s','%s','%s','%s')" % (userId,userName,password,nickName)

            message.delete()

        insertInterval = time.time() - lastInsertTime
        #print insertInterval
        if insertInterval >= insertTimeTriger or lastInsertUserCount >= insertUserCountTriger:
            print "timeout"
            if insertSQL != 'INSERT INTO niupu_user(id,username,password,nickname) ':
                insertToMySQL(insertSQL)
                #print "I am in the air"
            messageCount = 0
            insertSQL = 'INSERT INTO niupu_user(id,username,password,nickname) '
            lastInsertTime = time.time()
            lastInsertUserCount = 0



if __name__ == '__main__':
    host = '192.168.11.234'
    port = 11211
    tube = 'userAdd'
    getUser(host,port,tube)
