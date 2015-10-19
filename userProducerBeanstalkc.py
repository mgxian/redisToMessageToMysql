#coding:__utf8__
#!/bin/env python

import time
import beanstalkc
import json
import redis
import sys


def insertToRedis(redisConn, data):
    userIdtmp = data['id']
    key = 'user:' + str(userIdtmp)
    redisConn[key+':username'] = data['username']
    redisConn[key+':password'] = data['password']
    redisConn[key+':nickname'] = data['nickname']
    redisConn['user:'+ data['username'] + ':id'] = userIdtmp

def deleteFromRedis(redisConn, data):
    userIdtmp = data['id']
    key = 'user:' + str(userIdtmp)
    redisConn.delete(key+':username')
    redisConn.delete(key+':password')
    redisConn.delete(key+':nickname')
    redisConn.delete('user:'+ data['username'] + ':id')

redisConn = redis.Redis(host='192.168.11.231', port=19000)


beanstalk = beanstalkc.Connection(host='192.168.11.234', port=11211)
beanstalk.use('userAdd')
print beanstalk.using(), beanstalk.watching()

beanstalkUserId = beanstalkc.Connection(host='192.168.11.234', port=11211)
beanstalkUserId.use('userId')
beanstalkUserId.watch('userId')
beanstalkUserId.ignore('default')

userdic = {}
while 1:
    userIdData = beanstalkUserId.reserve(timeout=0)
    try:
        userId = int(userIdData.body)
    except:
        print "OK userId exausted !!!"
        sys.exit(0)
    userdic['id'] = userId
    userdic['username'] = 'User%s' % userId
    userdic['password'] = 'User%s' % userId
    userdic['nickname'] = 'will:%s' % userId
    print userdic 
    data = json.dumps(userdic)
    try:
        insertToRedis(redisConn, userdic)
        ret = beanstalk.put(data)
    except:
        print "put the userId into the useId queue"
        userIdData.release()
        deleteFromRedis(redisConn, userdic)
    userIdData.delete()
    print data
    #time.sleep(0.2)

beanstalk.close()
