#!/usr/bin/env python
# -----------------------------------------------------------------------------
from gevent.monkey import patch_all
patch_all()
from gevent import spawn, sleep
from gevent.pool import Group

import argparse
from ftplib import FTP, error_temp
from time import time
import uuid
# -----------------------------------------------------------------------------
BUFFER_LEN = 65536
CHUNK = "x" * BUFFER_LEN

NEED_CLEANUP = set()

s_1K = 1024
s_1M = s_1K * s_1K
# -----------------------------------------------------------------------------


class FTPPool(object):

    def __init__(self, addr, user, pwd, min_=1, max_=0, stat=None):
        self.addr = addr
        self.user = user
        self.pwd = pwd
        self.pool = set()
        self.busy = Group()
        self.stat = stat
        self.max_ = max_
        
        for _ in xrange(min_):
            self.pool.add(self._connect())

    def _connect(self):
        ftp = FTP(self.addr)
        try:
            ftp.login(self.user, self.pwd)
        except error_temp as e:
            print e
            return
        return ftp

    def _get_ftp(self):
        if self.pool:
            ftp = self.pool.pop()
        else:
            while self.max_ and len(self.busy) > self.max_:
                sleep(0.1)

            while True:
                ftp = self._connect()
                if ftp:
                    break

        return ftp

    def _release_ftp(self, gr, ftp):
        if gr.successful():
            self.pool.add(ftp)
        else:
            ftp.close()
            del ftp

        self.busy.discard(gr)
        if self.stat:
            self.stat.set_busy(len(self.busy))

    def spawn_ftp(self, func, *a, **kw):
        ftp = self._get_ftp()
        gr = spawn(func, ftp, *a, **kw)
        gr.link(lambda g: self._release_ftp(g, ftp))
        self.busy.add(gr)
        if self.stat:
            self.stat.set_busy(len(self.busy))

    def wait(self):
        self.busy.join()

    def break_all(self):
        self.busy.kill()


def stor(ftp, path, size, stat):
    try:
        ftp.voidcmd("TYPE I")  # binary mode
        conn = ftp.transfercmd("STOR " + path)
        total_sent = 0
        while 1:
            chunk_size = len(CHUNK)
            while chunk_size:
                sent = conn.send(CHUNK[:chunk_size])
                chunk_size -= sent
                total_sent += sent
                stat.incr_bytes(sent)
            if total_sent == size:
                break
            elif total_sent > size:
                raise RuntimeError(
                    "total_sent > size: %s > %s" % (total_sent, size)
                )
        conn.close()
        ftp.voidresp()
    except Exception:
        stat.inrc_err()
        raise
    else:
        stat.incr_stor()


def runworkers(ftp_pool, count, size, stat, basedir):
    for _ in xrange(count):
        path = "%s/bench_write-%s" % (basedir, uuid.uuid1().hex)
        NEED_CLEANUP.add(path)
        ftp_pool.spawn_ftp(stor, path, size, stat)


def clean(addr, user, password):
    print "-- cleanning..."
    sleep(5)
    ftp_pool = FTPPool(addr, user, password, min_=10, max_=10)

    def _delete(ftp, name):
        try:
            ftp.delete(name)
        except Exception as e:
            print name, e

    while NEED_CLEANUP:
        name = NEED_CLEANUP.pop()        
        ftp_pool.spawn_ftp(_delete, name)

    ftp_pool.wait()


class Stats(object):
    
    def __init__(self):
        self.last_time = time()
        self.stor = self.last_stor = 0
        self.bytes_ = self.last_bytes = 0
        self.busy = 0
        self.errors = self.last_errors = 0

    def incr_bytes(self, bytes_):
        self.bytes_ += bytes_

    def incr_stor(self):
        self.stor += 1

    def set_busy(self, count):
        self.busy = count

    def inrc_err(self):
        self.errors += 1

    def snap_stats(self):
        now = time()
        delta_time, self.last_time = now - self.last_time, now
        delta_stor, self.last_stor = self.stor - self.last_stor, self.stor
        delta_bytes, self.last_bytes \
            = self.bytes_ - self.last_bytes, self.bytes_
        delta_errors, self.last_errors \
            = self.errors - self.last_errors, self.errors
        
        return {
            "delta_time": delta_time,
            "stor": delta_stor, "bytes": delta_bytes,
            "busy": self.busy,
            "errors": delta_errors,
        }


def bytes2human(n, frmt="%(value).1f%(symbol)s"):
    """
    >>> bytes2human(10000)
    '9K'
    >>> bytes2human(100001221)
    '95M'
    """
    symbols = ['B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i+1)*10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return frmt % locals()
    return frmt % dict(symbol=symbols[0], value=n)


def print_stat(snap):
    delta_time = snap["delta_time"]
    if delta_time < 0.5:
        return  # skip

    rate_stor = snap["stor"] / delta_time
    rate_bytes = snap["bytes"] / delta_time

    print "busy", snap["busy"], "err", snap["errors"], "r_stor", round(rate_stor, 1), "r_mb", bytes2human(rate_bytes), "delta_t", round(delta_time, 2) 
 

def bench(addr, login, passwod, basedir, count, size, pool):
    stat = Stats()
    ftp_pool = FTPPool(addr, login, passwod, min_=pool, stat=stat)
    try:
        while 1:
            print_stat(stat.snap_stats())
            runworkers(ftp_pool, count, size, stat, basedir)
            sleep(1)
    except KeyboardInterrupt:
        print "break"
        ftp_pool.break_all()
        ftp_pool.wait()
    finally:
        clean(addr, login, passwod)


def bench_auth(addr, login, passwod, pool):
    ftp_pool = FTPPool(addr, login, passwod, min_=pool)
    try:
        while 1:
            print "connections %s" % len(ftp_pool.pool)
            sleep(1)
    except KeyboardInterrupt:
        print "break"
        ftp_pool.break_all()
        ftp_pool.wait()
    finally:
        pass


def main():
    parser = argparse.ArgumentParser(
        description="FTP bench mark for swift"
    )
    # actions
    parser.add_argument(
        "bench", action="store",
        help="bench actions: stor"
    )
    # options
    parser.add_argument(
        "-c", action="store", type=int, dest="count", default=10,
        help="command per second (default: 10)"
    )
    parser.add_argument(
        "-s", action="store", type=int, dest="size", default=1,
        help="file size for stor and retr command (default: 1)"
    )
    parser.add_argument(
        "-a", action="store", dest="addr", default="127.0.0.1:21",
        help="host address (default 127.0.0.1:21)"
    )
    parser.add_argument(
        "-u", action="store", dest="user", required=True,
        help="ftp user"
    )
    parser.add_argument(
        "-p", action="store", dest="password", required=True,
        help="ftp password"
    )
    parser.add_argument(
        "-b", action="store", dest="basedir", default="/",
        help="ftp basedir for actions (default: /)"
    )
    parser.add_argument(
        "--pool", action="store", type=int, dest="pool",
        help="default size for connnections pool (default: equal count)"
    )
    args = parser.parse_args()

    if args.bench == "stor":
        bench(
            addr=args.addr, login=args.user, passwod=args.password,
            basedir=args.basedir,
            count=args.count, size=args.size * 1024 * 1024,
            pool=args.pool and args.pool or args.count * 2
        )
    elif args.bench == "auth":
        bench_auth(
            addr=args.addr, login=args.user, passwod=args.password,
            pool=args.pool
        )
    else:
        pass

if __name__ == "__main__":
    main()
