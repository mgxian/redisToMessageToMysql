#coding:__utf8__
#!/bin/env python

import time
import beanstalkc
import MySQLdb as mysql


def getKeyId(table):
    SQL = 'SELECT MAX(id) from %s' % table
    #print 'SQL: %s' % SQL
    try:
        mysqlConn = mysql.connect(host='192.168.11.232',user='niupu',passwd='niupu',db='niupu',port=3306) 
        mysqlCur = mysqlConn.cursor()
        mysqlCur.execute(SQL)
        keyId = mysqlCur.fetchone()[0]
        mysqlCur.close()
        mysqlConn.close()
        return keyId
    except mysql.Error,e:
        print "MySQL Error %d: %s" % (e.args[0], e.args[1])
        mysqlCur.close()
        mysqlConn.close()

def main():
    lastInsertId = 0
    limitAddUserNumber = 5000
    limitRestUserIdNumber = 2000 
    beanstalk = beanstalkc.Connection(host='192.168.11.234', port=11211)
    beanstalk.use('userId')
    beanstalk.watch('userId')
    beanstalk.ignore('default')

    beanstalkUser = beanstalkc.Connection(host='192.168.11.234', port=11211)
    beanstalkUser.use('userAdd')
    beanstalkUser.watch('userAdd')
    beanstalkUser.ignore('default')

    #print beanstalk.using(), beanstalk.watching()

    #delete all userId
    tmpId = beanstalk.reserve(timeout=0)
    while tmpId is not None: 
        tmpId.delete()
        tmpId = beanstalk.reserve(timeout=0)

    #delete all message from userAdd
    tmpId = beanstalkUser.reserve(timeout=0)
    while tmpId is not None: 
        tmpId.delete()
        tmpId = beanstalkUser.reserve(timeout=0)

    dbMaxId = getKeyId('niupu_user')

    while 1:
        idReady = beanstalk.stats_tube('userId')['current-jobs-ready']
        if idReady < limitRestUserIdNumber and lastInsertId == 0:
            print "Null and add id"
            for i in xrange(dbMaxId + 1, dbMaxId + limitAddUserNumber + 1):
                ret = beanstalk.put(str(i))
            else:
                lastInsertId = i

        elif idReady < limitRestUserIdNumber and lastInsertId != 0:
            print "not Null and add id"
            for i in xrange(lastInsertId + 1, lastInsertId + limitAddUserNumber + 1):
                ret = beanstalk.put(str(i))
            else:
                lastInsertId = i
        else:
            time.sleep(1)


if __name__ == '__main__':
    print getKeyId('niupu_user')
    main()
