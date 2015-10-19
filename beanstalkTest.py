#!/bin/env python


import beanstalkc



beanstalk = beanstalkc.Connection(host='192.168.11.234', port=11211)
beanstalk.put('hey!')
job = beanstalk.reserve()
print job.body
job.delete()

