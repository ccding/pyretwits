#!/usr/bin/env python

import redis
import os, sys, random, time

init_n_users = 1000
init_avg_followers = 8
len_timeline = 50

total_txns = 100*1000
count_ops = 0

# redis connection
r = redis.StrictRedis(host='localhost', port=6379, db=0)

def init():
    r.flushall()
    for i in range(init_n_users):
        register()
    for i in range(init_avg_followers * init_n_users):
        follow()

def follow():
    uid = random.randint(0, init_n_users)
    fuid = random.randint(0, init_n_users)
    ti = int(time.time())
    r.zadd('following:'+str(uid), ti, fuid)
    r.zadd('followers:'+str(fuid), ti, uid)
    global count_ops
    count_ops += 2

def post():
    uid = random.randint(0, init_n_users)
    pid = r.incr('next_post_id')
    status = 'this is a status for ' + str(pid) + ' from ' + str(uid)
    r.hmset('post:'+str(pid),
                {'user_id': uid,
                 'time': int(time.time()),
                 'body': status})
    followers = r.zrange('followers:'+str(uid), 0, -1)
    for fid in followers:
        r.lpush('posts:'+str(fid), pid)
        r.ltrim('posts:'+str(fid), 0, len_timeline)
    r.lpush('timeline', pid)
    r.ltrim('timeline', 0, len_timeline)
    global count_ops
    count_ops += 5 + 2*len(followers)

def timeline():
    uid = random.randint(0, init_n_users)
    posts = r.lrange('posts:'+str(uid), 0, -1)
    for pid in posts:
        post = r.hgetall('post:'+str(pid))
    global count_ops
    count_ops += 1+len(posts)

def register():
    pid = r.incr('next_user_id')
    global init_n_users
    init_n_users = pid
    global count_ops
    count_ops += 1

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'init':
        init()
        sys.exit()
    start = time.time()
    for i in range(total_txns):
        rand = random.random()
        if rand < 0.05:
            register() # at probability of 5%
        elif rand < 0.20:
            follow() # at probability of 15%
        elif rand < 0.50:
            post() # at probability of 30%
        else:
            timeline() # at probability of 50%
    end = time.time()
    print 'txns/sec:', total_txns/(end-start)
    print 'ops/sec:', count_ops/(end-start)
