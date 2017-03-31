# -*- coding:utf-8 -*-
from time import sleep

import happybase
import traceback
import logging

from datetime import datetime, timedelta

from DatetimeKeyGenerator import DatetimeKeyGenerator
from Generator import Generator

__Author__ = 'YuChuanQi'
__Time__ = '2017-03-31 11:41:00'
__API__ = 'https://happybase.readthedocs.io/en/latest/user.html#establishing-a-connection'

_HBASE_TABLE = 'log_susuan_biz-log'
# 数据保留的天数
_DATA_REMAIN_DAY = 5

_DATE_FORMAT = '%Y-%m-%d 00:00:00'

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s] %(filename)s:%(lineno)s %(name)s:%(levelname)s:%(message)s")


def get_connection_pool():
    pool = None
    try:
        # 设置timeout，防止查询数据量大时socket超时
        pool = happybase.ConnectionPool(size=3, timeout=1200000)
        logging.info('connect hbase success')
    except Exception,e:
        traceback.print_exc()

    return pool

def release_connection(connection):
    if connection:
        connection.close()

def get_row_key(datetime_str, use_old_generator=True):
    rk_generator = DatetimeKeyGenerator()

    if use_old_generator:
        rk_generator = Generator()
    row_key = rk_generator.generateTimeRowKeyStart(datetime_str)
    return row_key

def select(row_key_start, row_kay_stop):
    pass

def select_by_rowkey():
    pass

def batch_delete(table, row_key_start, row_key_stop):
    logging.info('delete data in table: %s' % table.name)
    try:
        del_count = 0
        with table.batch(batch_size=10000) as b:
            scanner = table.scan(row_key_start, row_key_stop, batch_size=10000)
            logging.info('read data success')
            for row_key, data in scanner:
                b.delete(row_key)
                del_count += 1
                if del_count % 1000000 == 0:
                    logging.info('delete rows:%s' % del_count)
            logging.info('delete rows:%s' % del_count)
    except Exception,e:
        traceback.print_exc()

if __name__ == '__main__':
    pool = get_connection_pool()

    day_offset = -_DATA_REMAIN_DAY

    begin_time = datetime(year=2016, month=6, day=1)
    end_time = datetime(year=2016, month=6, day=4)
    #end_time = datetime.now() + timedelta(days=day_offset)

    begin_time_str = begin_time.strftime(format=_DATE_FORMAT)
    end_time_str = end_time.strftime(format=_DATE_FORMAT)

    while begin_time < end_time:
        connection = None
        try:
            with pool.connection() as connection:
                table = connection.table(_HBASE_TABLE)

            row_key_start = get_row_key(begin_time_str, use_old_generator=True)

            end_time_tmp = begin_time + timedelta(days=1)
            end_time_tmp_str = end_time_tmp.strftime(_DATE_FORMAT)
            row_key_stop = get_row_key(end_time_tmp_str, use_old_generator=True)

            logging.info('row key : %s,%s' % (row_key_start, row_key_stop))
            logging.info('delete time between:%s~%s' % (begin_time_str, end_time_tmp_str))

            #batch_delete(table, row_key_start, row_key_stop)
        except Exception, e:
            traceback.print_exc()
            break
        finally:
            release_connection(connection)

        begin_time = end_time_tmp
        begin_time_str = begin_time.strftime(format=_DATE_FORMAT)
        sleep(1)

