"""
See COPYING for license information.
"""
from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.web.client import HTTPConnectionPool

import ftplib
import tempfile
import shutil
import time
import os

from . import get_config, has_item, create_test_file, clean_swift, \
    compute_md5, upload_file, utf8_chars, get_swift_client

conf = get_config()


class FTPFuncTest(unittest.TestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.pool = HTTPConnectionPool(reactor, persistent=True)
        self.swift = get_swift_client(conf, pool=self.pool)
        self.tmpdir = tempfile.mkdtemp()
        self.ftp = self.get_ftp_client()
        yield clean_swift(self.swift)

    @defer.inlineCallbacks
    def tearDown(self):
        shutil.rmtree(self.tmpdir)
        self.ftp.close()
        yield clean_swift(self.swift)
        yield self.pool.closeCachedConnections()

    def get_ftp_client(self):
        return get_ftp_client(conf)


def validate_config(config):
    for key in 'ftp_host ftp_port account username password'.split():
        if key not in config:
            raise unittest.SkipTest("%s not set in the test config file" % key)


def get_ftp_client(config):
    validate_config(config)
    if config.get('debug'):
        ftplib.FTP.debugging = 5
    ftp = ftplib.FTP()
    ftp.connect(config['ftp_host'], int(config['ftp_port']))
    ftp.login("%s:%s" % (config['account'], config['username']),
              config['password'])
    return ftp


class BasicTests(unittest.TestCase):
    def test_get_client(self):
        ftp = get_ftp_client(conf)
        ftp.getwelcome()
        ftp.quit()

    def test_get_client_close(self):
        ftp = get_ftp_client(conf)
        ftp.getwelcome()
        ftp.close()

    def test_get_client_sock_close(self):
        for n in range(100):
            ftp = get_ftp_client(conf)
            ftp.getwelcome()
            ftp.sock.close()
            ftp.file.close()


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.active_connections = []

    def tearDown(self):
        for conn in self.active_connections:
            try:
                conn.close()
            except:
                pass

    def get_client(self):
        conn = get_ftp_client(conf)
        self.active_connections.append(conn)
        return conn

    def test_get_many_client(self):
        for i in range(32):
            ftp = get_ftp_client(conf)
            ftp.close()

    # This test assumes sessions_per_user = 10
    def test_get_many_concurrent(self):
        validate_config(conf)
        for i in range(100):
            conn = ftplib.FTP()
            conn.connect(conf['ftp_host'], int(conf['ftp_port']))
            self.active_connections.append(conn)
        time.sleep(10)

    # This test assumes sessions_per_user = 10
    def test_concurrency_limit(self):
        for i in range(10):
            self.get_client()
        self.assertRaises(ftplib.error_temp, self.get_client)

    # This test assumes sessions_per_user = 10
    def test_concurrency_limit_disconnect_one(self):
        for i in range(10):
            self.get_client()

        conn = self.active_connections.pop()
        conn.close()

        # This should not raise an error
        self.get_client()


class RenameTests(FTPFuncTest):
    def test_rename_account(self):
        self.assertRaises(ftplib.error_perm, self.ftp.rename, '/', '/a')

    @defer.inlineCallbacks
    def test_rename_container(self):
        yield self.swift.put_container('ftp_tests')

        self.ftp.rename('ftp_tests', 'ftp_tests_2')
        r, listing = yield self.swift.get_account()

        self.assertTrue(has_item('ftp_tests_2', listing))
        self.assertFalse(has_item('ftp_tests', listing))

    @defer.inlineCallbacks
    def test_rename_container_populated(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', 'a')

        self.assertRaises(ftplib.error_perm, self.ftp.rename, 'ftp_tests',
                          'ftp_tests_2')

    @defer.inlineCallbacks
    def test_rename_object(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', 'a')
        yield self.swift.put_object(
            'ftp_tests', 'b',
            headers={'Content-Type': 'application/directory'})
        yield self.swift.put_object('ftp_tests', 'b/nested')
        yield self.swift.put_object('ftp_tests', 'c/nested')

        self.ftp.rename('ftp_tests/a', 'ftp_tests/a1')

        r, listing = yield self.swift.get_container('ftp_tests')

        self.assertTrue(has_item('a1', listing))
        self.assertFalse(has_item('a', listing))

        self.assertRaises(ftplib.error_perm, self.ftp.rename, 'ftp_tests/b',
                          'ftp_tests/b1')
        self.assertRaises(ftplib.error_perm, self.ftp.rename, 'ftp_tests/c',
                          'ftp_tests/c1')

    def test_rename_object_not_found(self):
        self.assertRaises(ftplib.error_perm, self.ftp.rename, 'ftp_tests/a',
                          'ftp_tests/b')


class DownloadTests(FTPFuncTest):
    @defer.inlineCallbacks
    def _test_download(self, size, name, callback=None):
        yield self.swift.put_container('ftp_tests')
        src_path, md5 = create_test_file(self.tmpdir, size)
        yield upload_file(self.swift, 'ftp_tests', name, src_path, md5)
        dlpath = '%s/%s.dat' % (self.tmpdir, name)

        if not callback:
            resp = self.ftp.retrbinary('RETR ftp_tests/%s' % name,
                                       callback=open(dlpath, 'wb').write)
            self.assertEqual(os.stat(dlpath).st_size, size)
            self.assertEqual(md5, compute_md5(dlpath))
        else:
            resp = self.ftp.retrbinary('RETR ftp_tests/%s' % name,
                                       callback=callback)
        self.assertEqual('226 Transfer Complete.', resp)

    def test_zero_byte_file(self):
        return self._test_download(0, '0b.dat')

    def test_32kb_file(self):
        return self._test_download(32 * 1024 + 1, '32kb.dat')

    def test_1mb_file(self):
        return self._test_download(1024 * 1024, '1mb.dat')

    def test_10mb_file(self):
        return self._test_download(1024 * 1024 * 10, '10mb.dat')

    def test_file_leak(self):
        class Callback(object):
            def __init__(self):
                self.i = 0

            def cb(self, data):
                self.i += 1
                if self.i == 2:
                    time.sleep(5)  # relatively long sleep
                # TODO: FIND PROCESS AND CHECK FOR MEMORY BLOAT
                #       For now, just monitor memory usage

        return self._test_download(1024 * 1024 * 100, '100mb.dat',
                                   callback=Callback().cb)

    @defer.inlineCallbacks
    def test_read_timeout(self):
        class Callback(object):
            def __init__(self):
                self.i = 0

            def cb(self, data):
                self.i += 1
                if self.i == 2:
                    time.sleep(40)  # The timeout is actually 30 seconds

        try:
            yield self._test_download(1024 * 1024 * 100, '100mb.dat',
                                      callback=Callback().cb)
        except ftplib.error_temp:
            pass
        except:
            raise
        else:
            self.fail("Expected timeout error")


class UploadTests(FTPFuncTest):
    @defer.inlineCallbacks
    def _test_upload(self, size, name):
        yield self.swift.put_container('ftp_tests')
        src_path, md5 = create_test_file(self.tmpdir, size)

        resp = self.ftp.storbinary('STOR ftp_tests/%s' % name,
                                   open(src_path, 'rb'))
        self.assertEqual('226 Transfer Complete.', resp)

        headers = yield self.swift.head_object('ftp_tests', name)
        self.assertEqual(md5, headers['etag'])
        self.assertEqual(size, int(headers['content-length']))

    def test_zero_byte_file(self):
        return self._test_upload(0, '0b.dat')

    def test_32kb_file(self):
        return self._test_upload(1024 * 32 + 1, '32kb.dat')

    def test_1mb_file(self):
        return self._test_upload(1024 * 1024, '1mb.dat')

    def test_10mb_file(self):
        return self._test_upload(1024 * 1024 * 10, '10mb.dat')


class SizeTests(FTPFuncTest):
    def test_size_root(self):
        # Testing For Error Only
        self.ftp.size('')

    @defer.inlineCallbacks
    def test_size_container(self):
        yield self.swift.put_container('ftp_tests')

        size = self.ftp.size('ftp_tests')
        self.assertEqual(0, size)

    @defer.inlineCallbacks
    def test_size_directory(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests', 'test_size_directory',
            headers={'Content-Type': 'application/directory'})

        size = self.ftp.size('ftp_tests/test_size_directory')
        self.assertEqual(0, size)

    @defer.inlineCallbacks
    def test_size_object(self):
        yield self.swift.put_container('ftp_tests')
        src_path, md5 = create_test_file(self.tmpdir, 1024)
        yield upload_file(self.swift, 'ftp_tests', 'test_size_object',
                          src_path, md5)

        size = self.ftp.size('ftp_tests')
        self.assertEqual(1024, size)

    def test_size_container_missing(self):
        self.assertRaises(ftplib.error_perm, self.ftp.size, 'ftp_tests')

    def test_size_object_missing(self):
        self.assertRaises(ftplib.error_perm, self.ftp.size,
                          'ftp_tests/test_size_container_missing')

    @defer.inlineCallbacks
    def test_size_dir_dir(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests',
            '%s/%s' % (utf8_chars.encode('utf-8'), utf8_chars.encode('utf-8')))
        size = self.ftp.size('ftp_tests/%s' % utf8_chars.encode('utf-8'))
        self.assertEqual(0, size)


class DeleteTests(FTPFuncTest):
    @defer.inlineCallbacks
    def test_delete_populated_container(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests', 'dir1',
            headers={'Content-Type': 'application/directory'})
        self.assertRaises(ftplib.error_perm, self.ftp.rmd, 'ftp_tests')

    @defer.inlineCallbacks
    def test_delete_populated_dir(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests', 'dir1',
            headers={'Content-Type': 'application/directory'})
        yield self.swift.put_object('ftp_tests', 'dir1/obj2')
        self.ftp.rmd('ftp_tests/dir1')

    @defer.inlineCallbacks
    def test_delete_populated_dir_not_existing(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', 'dir1/obj2')
        self.ftp.rmd('ftp_tests/dir1')


class ListingTests(FTPFuncTest):
    def test_listing(self):
        listing = self.ftp.nlst('')
        self.assertNotIn('ftp_tests', listing)

    @defer.inlineCallbacks
    def test_listing_exists(self):
        yield self.swift.put_container('ftp_tests')
        listing = self.ftp.nlst('')
        self.assertIn('ftp_tests', listing)

    @defer.inlineCallbacks
    def test_directory_listing(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests', 'dir1',
            headers={'Content-Type': 'application/directory'})
        yield self.swift.put_object(
            'ftp_tests', 'dir2',
            headers={'Content-Type': 'application/directory'})
        yield self.swift.put_object('ftp_tests', 'dir2/obj1')
        yield self.swift.put_object('ftp_tests', 'dir3/obj2')

        listing = []
        self.ftp.dir('ftp_tests', listing.append)
        self.assertIn('dir1', listing[0])
        self.assertIn('dir2', listing[1])
        self.assertIn('dir3', listing[2])
        self.assertEqual(3, len(listing))

        listing = self.ftp.nlst('ftp_tests/dir1')
        self.assertEqual(0, len(listing))

        listing = self.ftp.nlst('ftp_tests/dir2')
        self.assertIn('obj1', listing)
        self.assertEqual(1, len(listing))

        listing = self.ftp.nlst('ftp_tests/dir3')
        self.assertIn('obj2', listing)
        self.assertEqual(1, len(listing))

    @defer.inlineCallbacks
    def test_long_listing(self):
        obj_count = 10010
        yield self.swift.put_container('ftp_tests')
        deferred_list = []
        sem = defer.DeferredSemaphore(200)
        for i in range(obj_count):
            d = sem.run(self.swift.put_object, 'ftp_tests', str(i))
            deferred_list.append(d)
        yield defer.DeferredList(deferred_list, consumeErrors=True)
        time.sleep(2)

        # The original FTP client can timeout while doing the setup
        self.ftp = self.get_ftp_client()
        listing = []
        self.ftp.dir('ftp_tests', listing.append)
        self.assertTrue(len(listing) > 10000)

    @defer.inlineCallbacks
    def test_long_listing_nested(self):
        obj_count = 10010
        yield self.swift.put_container('ftp_tests')
        deferred_list = []
        sem = defer.DeferredSemaphore(200)
        for i in range(obj_count):
            d = sem.run(self.swift.put_object, 'ftp_tests', 'subdir/' + str(i))
            deferred_list.append(d)
        yield defer.DeferredList(deferred_list, consumeErrors=True)
        time.sleep(2)

        # The original FTP client can timeout while doing the setup
        self.ftp = self.get_ftp_client()
        listing = []
        self.ftp.dir('ftp_tests/subdir', listing.append)
        self.assertTrue(len(listing) > 10000)


class MkdirTests(FTPFuncTest):

    @defer.inlineCallbacks
    def test_make_container(self):
        self.ftp.mkd('ftp_tests')
        yield self.swift.head_container('ftp_tests')

    @defer.inlineCallbacks
    def test_make_object_dir(self):
        yield self.swift.put_container('ftp_tests')
        self.ftp.mkd('ftp_tests/mkdir')
        yield self.swift.head_object('ftp_tests', 'mkdir')

    @defer.inlineCallbacks
    def test_make_nested_object_dir(self):
        yield self.swift.put_container('ftp_tests')
        self.ftp.mkd('ftp_tests/nested/mkdir')
        yield self.swift.head_object('ftp_tests', 'nested/mkdir')


class RmdirTests(FTPFuncTest):

    @defer.inlineCallbacks
    def test_rmdir_container(self):
        yield self.swift.put_container('ftp_tests')
        self.ftp.rmd('ftp_tests/nested/mkdir')
        resp, listing = yield self.swift.get_account('ftp_tests')
        self.assertNotIn('ftp_tests', listing)

    @defer.inlineCallbacks
    def test_rmdir_container_populated(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', utf8_chars.encode('utf-8'))
        self.assertRaises(ftplib.error_perm, self.ftp.rmd, 'ftp_tests')

    @defer.inlineCallbacks
    def test_rmdir_object_dir(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', 'nested/dir')
        self.ftp.rmd('ftp_tests/nested/dir')
        resp, listing = yield self.swift.get_container('ftp_tests')
        self.assertEqual(len(listing), 0)

    @defer.inlineCallbacks
    def test_rmdir_nested_object_dir(self):
        yield self.swift.put_container('ftp_tests')
        self.ftp.rmd('ftp_tests/nested/mkdir')
        resp, listing = yield self.swift.get_container('ftp_tests')
        self.assertEqual(len(listing), 0)


class RemoveTests(FTPFuncTest):

    @defer.inlineCallbacks
    def test_remove_file(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', utf8_chars.encode('utf-8'))
        self.ftp.delete('ftp_tests/%s' % utf8_chars.encode('utf-8'))
        resp, listing = yield self.swift.get_container('ftp_tests')
        self.assertEqual(len(listing), 0)

    @defer.inlineCallbacks
    def test_remove_container(self):
        yield self.swift.put_container('ftp_tests')
        self.assertRaises(ftplib.error_perm, self.ftp.delete, 'ftp_tests')


class CwdTests(FTPFuncTest):

    def test_cwd_root(self):
        self.ftp.cwd('/')

    @defer.inlineCallbacks
    def test_cwd_container(self):
        yield self.swift.put_container('ftp_tests')
        self.ftp.cwd('ftp_tests')

    def test_cwd_container_not_found(self):
        self.assertRaises(
            ftplib.error_perm, self.ftp.cwd, 'ftp_tests_not_found')

    @defer.inlineCallbacks
    def test_cwd_object(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object('ftp_tests', utf8_chars.encode('utf-8'))
        self.assertRaises(
            ftplib.error_perm,
            self.ftp.cwd, 'ftp_tests/%s' % utf8_chars.encode('utf-8'))

    @defer.inlineCallbacks
    def test_cwd_object_directory(self):
        yield self.swift.put_container('ftp_tests')
        yield self.swift.put_object(
            'ftp_tests', utf8_chars.encode('utf-8'),
            headers={'Content-Type': 'application/directory'})
        self.ftp.cwd('ftp_tests/%s' % utf8_chars.encode('utf-8'))
