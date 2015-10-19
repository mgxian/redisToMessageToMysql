#coding:__utf8__
#!/bin/env python

import time
import sys
import stomp
import MySQLdb as mysql
import json

insertTimeTriger = 1
insertUserCountTriger = 100

lastInsertTime = 0
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


class MyListener(object):
    def on_error(self, headers, message):
        print('received an error %s' % message)
    def on_message(self, headers, message):
        #print 'received a message %s' % message
	global messageCount,lastInsertUserCount,lastInsertTime,insertSQL
	messageCount += 1
        lastInsertUserCount += 1
	data = json.loads(message)
   	userId = data['id']
	userName = data['username']
	password = data['password']
	nickName = data['nickname']
	
        #print userId

        if messageCount == 1:
            insertSQL = insertSQL + "VALUES('%s','%s','%s','%s')" % (userId,userName,password,nickName)
        else:
            insertSQL = insertSQL + ",('%s','%s','%s','%s')" % (userId,userName,password,nickName)

	insertInterval = time.time() - lastInsertTime
        #print insertInterval
	if insertInterval >= insertTimeTriger or lastInsertUserCount >= insertUserCountTriger:
            insertToMySQL(insertSQL)
            #print "I am in the air"
            messageCount = 0
            insertSQL = 'INSERT INTO niupu_user(id,username,password,nickname) '
            lastInsertTime = time.time()
            lastInsertUserCount = 0
		

conn = stomp.StompConnection12([('192.168.11.234',61613)])  
#conn = stomp.StompConnection12([('192.168.11.234',61613),('192.168.11.235',61613),('192.168.11.236',61613)])  
conn.set_listener('', MyListener())
conn.start()
conn.connect()

while 1:
    conn.subscribe(destination='/queue/test', id=1, ack='auto')
    if lastInsertUserCount >= 1:
        print "I am the last one !!!"
        insertToMySQL(insertSQL)
        messageCount = 0
        insertSQL = 'INSERT INTO niupu_user(id,username,password,nickname) '
        lastInsertTime = time.time()
        lastInsertUserCount = 0
    time.sleep(2)
    conn.unsubscribe(id=1)

time.sleep(2)
conn.disconnect()
