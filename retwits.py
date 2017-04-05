#!/usr/bin/env python

import redis
import random, time

n_users = 1000
len_timeline = 50
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def init():
    for i in range(1000):
        register()
    for i in range(8*1000):
        follow()

def follow():
    uid = random.randint(0, n_users)
    fuid = random.randint(0, n_users)
    ti = int(time.time())
    r.zadd('following:'+str(uid), ti, fuid)
    r.zadd('followers:'+str(fuid), ti, uid)

def post():
    uid = random.randint(0, n_users)
    pid = r.incr('next_post_id')
    status = 'this is a status for ' + str(pid) + ' from ' + str(uid)
    r.hmset('post:'+str(pid), 'user_id', uid, 'time', int(time.time()), 'body', status)
    followers = r.zrange('followers:'+str(uid), 0, -1)
    for fid in followers:
        r.lpush('posts:'+str(fid), pid)
        r.ltrim('posts:'+str(fid), 0, len_timeline)
    r.lpush('timeline', pid)
    r.ltrim('timeline', 0, len_timeline)

def timeline():
    uid = random.randint(0, n_users)
    posts = r.lrange('posts:'+str(uid), 0, -1)
    for pid in posts:
        post = r.hgetall('post:'+str(pid))

def register():
    global n_users
    pid = r.incr('next_user_id')
    n_users = pid

if __name__ == '__main__':
    init()
    for i in range(1):
        rand = random.random()
        if rand < 0.05:
            register()
        elif rand < 0.20:
            follow()
        elif rand < 0.50:
            post
        else:
            timeline()
