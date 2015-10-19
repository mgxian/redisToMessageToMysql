#coding:__utf8__
#!/bin/env python

import time
import sys
import stomp
import json

class MyListener(object):
    def on_error(self, headers, message):
        print('received an error %s' % message)
    def on_message(self, headers, message):
        print('received a message %s' % message)

conn = stomp.StompConnection12([('192.168.11.234',61613)])  
#conn = stomp.StompConnection12([('192.168.11.234',61613),('192.168.11.235',61613),('192.168.11.236',61613)])  
#conn.set_listener('', MyListener())
conn.start()
conn.connect()

userdic = {}
userId = 1
#for userId in xrange(1,100):
while 1:
    userdic['id'] = userId
    userdic['username'] = 'User%s' % userId
    userdic['password'] = 'User%s' % userId
    userdic['nickname'] = 'will:%s' % userId
    #print userdic 
    data = json.dumps(userdic)
    conn.send(body=data, destination='/queue/test', headers={'persistent': 'true'})
    print data
    userId += 1
    #time.sleep(0.1)


#time.sleep(1)
conn.disconnect()
