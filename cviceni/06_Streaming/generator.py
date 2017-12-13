#!/usr/bin/env python

import socket
import time
import sys
import json


def get_alarm_data(cust_id):
    data = {
        'ts': time.time(),
        'event_type': 'alarm',
        'cust_id': cust_id
    }
    
    return data
    
def get_temp_data(cust_id):
    import random
    
    data = {
        'ts': time.time(),
        'event_type': 'temp_update',
        'temperature': random.gauss(25, 3),
        'cust_id': cust_id
    }
    
    return data

if __name__ == '__main__':
    import random
    
    if len(sys.argv) != 2:
        print 'Usage: python generator.py <port>'
        exit(1)

    TCP_IP = 'hador.ics.muni.cz'
    TCP_PORT = int(sys.argv[1])

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)

    conn, addr = s.accept()
    
    while 1:
        p = random.random()
        if p > 0.7:
            cust_id = random.randint(1, 10)
            data = get_alarm_data(cust_id)
            string_data = json.dumps(data)
            print 'alarm! {}'.format(string_data)
            conn.send("{}\n".format(string_data))

        for cust_id in xrange(1, 10):
            data = get_temp_data(cust_id)
            string_data = json.dumps(data)
            print 'temperature measurements {}'.format(string_data)
            conn.send("{}\n".format(string_data))
            print 'sleep'
        time.sleep(1)

    conn.close()