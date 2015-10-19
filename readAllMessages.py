
#coding:__utf8__
#!/bin/env python

import time
import sys
import stomp
import json

class MyListener(object):
    msg_list = []

    def __init__(self):
        self.msg_list = []

    def on_error(self, headers, message):
        self.msg_list.append('(ERROR) ' + message)

    def on_message(self, headers, message):
        self.msg_list.append(message)


conn = stomp.StompConnection12([('192.168.11.234',61613)])  
lst = MyListener()
conn.set_listener('', lst)
conn.start()
conn.connect()
conn.subscribe(destination='/queue/test', id=1, ack='auto')
time.sleep(2)
messages = lst.msg_list
conn.disconnect()
print messages
