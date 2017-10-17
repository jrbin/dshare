import csv
import datetime
import io
import logging
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from multiprocessing import JoinableQueue, Process, Queue
from threading import Thread

import pytz
import requests
from bypy import ByPy

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
LOG.addHandler(ch)
del formatter
del ch

TIME_AM_OPEN = datetime.time(9, 29, 30)
TIME_AM_CLOSE = datetime.time(11, 30, 5)
TIME_PM_OPEN = datetime.time(12, 59, 55)
TIME_PM_CLOSE = datetime.time(15, 0, 5)
TIME_UPLOAD = datetime.time(23, 0, 0)
TIMEZONE = pytz.timezone('Asia/Shanghai')

STOCK_INFO_URL = 'http://file.tushare.org/all.csv'
SINA_STOCK_URL = 'http://hq.sinajs.cn/rn=%s&list=%s'
SINA_LINE_PATTERN = r'.+(\d{6})="[^,]+,(.+)";'

CHUNK_SIZE = 100
INTERVAL = 10
RETRY = 3
N_WORKERS = 4
N_THREADS_OF_WORKER = 3


def simple_random(n=13):
    start = 10**(n-1)
    end = (10**n)-1
    return str(random.randint(start, end))


def get_stock_codes():
    resp = requests.get(STOCK_INFO_URL)
    resp.encoding = 'GBK'
    reader = csv.reader(io.StringIO(resp.text))
    next(reader)  # skip the header
    return [row[0] for row in reader]


def get_quotes(stock_codes):
    codes_with_prefix = ['sh' + code if code.startswith('6') else 'sz' + code
                         for code in stock_codes]
    url = SINA_STOCK_URL % (simple_random(), ','.join(codes_with_prefix))
    resp = requests.get(url)
    resp.encoding = 'GBK'
    result = ''
    for line in resp.text.splitlines():
        match_obj = re.match(SINA_LINE_PATTERN, line)
        if match_obj is None:
            LOG.warning('Cannot match line: %s', line)
            continue
        result += ','.join(match_obj.groups()) + '\n'
    return result


def worker_thread(queue_worker, queue_writer):
    while True:
        stock_codes, group_id = queue_worker.get()
        queue_worker.task_done()
        for _ in range(RETRY):
            try:
                csv_string = get_quotes(stock_codes)
                queue_writer.put((csv_string, group_id))
                break
            except:
                LOG.error(traceback.format_exc())
                time.sleep(1)


def worker(queue_worker, queue_writer):
    threads = []
    for _ in range(N_THREADS_OF_WORKER):
        thread = Thread(target=worker_thread, args=(queue_worker, queue_writer))
        thread.daemon = True
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def writer(queue):
    while True:
        csv_string, group_id = queue.get()
        now = datetime.datetime.now(TIMEZONE)
        path = 'data/%04d%02d%02d' % (now.year, now.month, now.day)
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)
        if not os.path.exists(path):
            LOG.debug('Create directory %s', path)
            os.makedirs(path)
        with open(path + '/%d.csv' % group_id, 'a') as csv_file:
            csv_file.write(csv_string)


def compress_and_upload(date):
    date_string = '%04d%02d%02d' % (date.year, date.month, date.day)
    folder_path = 'data/' + date_string
    folder_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), folder_path)
    if not os.path.exists(folder_path):
        LOG.error('%s does not exist', folder_path)
        return
    archive_path = folder_path + '.7z'
    if os.path.exists(archive_path):
        LOG.warning('%s already exists, it is removed first', archive_path)
        os.remove(archive_path)
    LOG.info('Compress and remove %s', folder_path)
    ret = subprocess.run(['7za', 'a', '-mmt=off', archive_path, folder_path]).returncode
    if ret != 0:
        LOG.error('Compressing %s returns nonzero code %d', folder_path, ret)
        return
    shutil.rmtree(folder_path, ignore_errors=True)
    LOG.info('Upload and remove %s', archive_path)
    bp = ByPy()
    ret = bp.upload(archive_path, 'dshare/data')
    if ret != 0 and ret != 60:
        LOG.error('Bypy upload returns nonzero code %d', ret)
        return
    os.remove(archive_path)


def main():
    queue_writer = Queue()
    queue_worker = JoinableQueue()

    thread = Thread(target=writer, args=(queue_writer,))
    thread.daemon = True
    thread.start()

    signal.signal(signal.SIGINT, signal.SIG_IGN)
    procs = []
    for i in range(N_WORKERS):
        proc = Process(target=worker, args=(queue_worker, queue_writer))
        proc.start()
        procs.append(proc)

    def sigint_handler(signum, frame):
        for proc in procs:
            proc.terminate()
        sys.exit(0)
    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    stock_codes = []
    last_empty = -1
    last_day_of_week = -1
    last_upload = -1

    while True:
        now = datetime.datetime.now(TIMEZONE)
        day_of_week = now.weekday()
        time_of_day = now.time()
        # updating stock info daily
        if last_day_of_week != day_of_week:
            last_day_of_week = day_of_week
            LOG.info('Downloading stock info')
            stock_codes = get_stock_codes()
        if 0 <= day_of_week <= 4 and last_upload != day_of_week and time_of_day >= TIME_UPLOAD:
            last_upload = day_of_week
            thread = Thread(target=compress_and_upload, args=(now,))
            thread.daemon = True
            thread.start()
        if not (0 <= day_of_week <= 4
                and (TIME_AM_OPEN <= time_of_day <= TIME_AM_CLOSE
                     or TIME_PM_OPEN <= time_of_day <= TIME_PM_CLOSE)):
            time.sleep(1)
            continue
        if last_empty != -1:
            time_diff = time.time() - last_empty
            LOG.info('Done in %.3f secs', time_diff)
            time_diff = INTERVAL - time_diff
            if time_diff > 0:
                time.sleep(time_diff)
        last_empty = time.time()

        LOG.info('Start %d tasks', len(stock_codes))
        for i in range(0, len(stock_codes), CHUNK_SIZE):
            queue_worker.put((stock_codes[i:i+CHUNK_SIZE], i//CHUNK_SIZE+1))
        queue_worker.join()


if __name__ == '__main__':
    main()
