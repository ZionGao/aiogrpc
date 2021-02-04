# -*- coding: utf-8 -*-
# @Time : 2021/1/13 10:15
# @Author : gaozq
# @File : aioclent.py
# @Software: PyCharm
# @contact: gaozq3@sany.com.cn
# -*- 功能说明 -*-
#
# -*- 功能说明 -*-

import logging
import asyncio
import sys
import uuid
import os
from grpc import aio
import grpc
from rpc_package.data_message_pb2 import *
from rpc_package.data_message_pb2_grpc import *
import json
import time
import pandas as pd
import zipfile
import io
from io import StringIO
from concurrent.futures import ProcessPoolExecutor
import logger
from sanydata import datatools
from functools import partial
from tqdm import tqdm
import gc
lock = asyncio.Lock()
log = logger.Logger(name=__name__).get_log


def static(data):
    def static1(data):
        df = data[(data["偏航状态"] == 1) | (data["CW电机反馈"] == 1) | (data["CCW电机反馈"] == 1)].copy()
        bins = [0, 5, 10, 15, 20, 25, 50]
        labels = ['0-5', '5-10', '10-15', '15-20', '20-25', '>25']

        df = df[(df['风速1'] >= 0) & (df['风速1'] < 50)]
        df['wind_speed_cut'] = pd.cut(df['风速1'], bins=bins, labels=labels)
        result_rs = list()
        # 风向分组
        df['direction'] = df['机舱位置'] + df['风向1']
        # df.loc[:, 'direction'][df['direction'] < 0] = df['direction'] + 360
        df['direction'] = df['direction'].apply(lambda x: (x + 360) if x < 0 else x)

        dir_bins = [0] + list(range(5, 365, 10)) + [370]
        dir_labels = [x for x in list(range(0, 365, 10))]
        df['direction_cut'] = pd.cut(df['direction'], bins=dir_bins, labels=dir_labels)
        df['direction_cut'] = df['direction_cut'].replace(360, 0)
        o = df.groupby(by=['wind_speed_cut', 'direction_cut']).agg({"turbine_num": "count"}).T
        return o

    data["time"] = pd.to_datetime(data["time"], format='%Y-%m-%dT%H:%M:%SZ')
    data["day"] = data["time"].apply(lambda s: s.strftime('%Y-%m-%d'))
    o = data.groupby(by=["windfarm", "turbine_num", "day"]).apply(static1)

    return o


def data_load(item, file, columns=None, columns_mapping=None):
    if item is None:
        return None
    else:
        f = zipfile.ZipFile(file=io.BytesIO(item))
        if len(f.namelist()) > 0:
            file_name = f.namelist()[0]
            file_type = file_name.split('.')[-1]
            file_size = f.getinfo(file_name)
            turbine_num = file_name.split('#')[0]
            turbine_num = turbine_num.zfill(3)
            pure_data = f.read(file_name)
            if file_type == 'parquet':
                fio = io.BytesIO(pure_data)
                # time_now = uuid.uuid1()
                # with open('./parquet_file_grpc_temp_{}.parquet'.format(time_now), "wb") as outfile:
                #     outfile.write(fio.getbuffer())
                # df = pd.read_parquet('./parquet_file_grpc_temp_{}.parquet'.format(time_now), columns=columns)
                # os.remove('./parquet_file_grpc_temp_{}.parquet'.format(time_now))
                df = pd.read_parquet(fio, engine='fastparquet')
            elif file_type == 'csv':
                sio = io.StringIO(pure_data.decode('utf-8'))
                df = pd.read_csv(sio)
                # df = pd.read_csv(StringIO(pure_data.decode('gbk')))
            else:
                log.info('unkown file type')
            if columns:
                df = df[columns]
            if columns_mapping:
                df = df.rename(columns=columns_mapping)
            df['turbine_num'] = turbine_num
            windfarm = file.split('/')[2]
            df['windfarm'] = windfarm
            df = df.loc[:, ~df.columns.duplicated()]

            res = static(df)
            del df, pure_data, f, item
            return res


async def data_handler(q, executor, i, columns=None, columns_mapping=None):
    # log.info('Mapper {} : starting'.format(i))
    loop = asyncio.get_running_loop()
    l = []

    global count
    global StreamNum
    global MapperNum
    global FileNum
    last_count = 0
    my_count = 0

    pbar = tqdm(total=FileNum, position=1, ncols=150)
    pbar.set_description('Mapper {} Start'.format(i), refresh=True)
    # pbar.set_postfix({f'进程{i}已处理': '{}'.format(my_count), '处理文件': ''}, refresh=True)

    while True:
        file, item = await q.get()
        # log.info('队列大小 {} load file {}'.format(q.qsize(), file))
        try:
            if item is None:
                log.info('Mapper {} : ending'.format(i))
                break
            else:
                item = item.output
                res = await loop.run_in_executor(executor, partial(data_load, item, file, columns, columns_mapping))
                l.append(res)
            async with lock:
                count = count+1
                my_count += 1
                pbar.set_description('Mapper {}'.format(i), refresh=False)
                pbar.set_postfix({f'进程{i}已处理': '{}'.format(my_count), '处理文件': file}, refresh=False)
                pbar.update((count-last_count))
                last_count = count
            del file, item
            gc.collect()

        except Exception as e:
            log.error(e)
            log.error(file)
        finally:
            q.task_done()

    pbar.close()


    return l


async def fetch_data(q, file_list, i):

    global count
    global StreamNum
    global MapperNum
    global FileNum

    options = [('grpc.max_message_length', 1024 * 1024 * 1024), ('grpc.max_receive_message_length', 1024 * 1024 * 1024)]
    async with grpc.aio.insecure_channel(target='192.168.2.4:9898', options=options) as channel:
        stub = BridgeMessageStub(channel)
        stream = stub.GetTargetFile(GetTargetFileInput(filelist=json.dumps(file_list)), timeout=1000)
        file_index = 0
        pbar = tqdm(total=len(file_list), position=0, ncols=150)
        pbar.set_description('Stream {} Start'.format(i), refresh=True)
        # pbar.set_postfix({'缓冲队列': '{}/{}'.format(q.qsize(), MapperNum), '获取文件': ''}, refresh=True)
        try:
            async for resp in stream.__aiter__():
                file = file_list[file_index]
                # log.info('fetch file {}'.format(file))
                await q.put((file, resp))
                pbar.set_description('Stream {}'.format(i), refresh=False)
                pbar.set_postfix({'缓冲队列': '{}/{}'.format(q.qsize(), MapperNum), '获取文件': file},refresh=False)
                pbar.update(1)
                file_index += 1


        except grpc.RpcError as e:
            log.error(e)
            log.info("10s 后重试 : {}".format(file_list[file_index:]))
            await asyncio.sleep(10)
            await fetch_data(q, file_list[file_index:])
        finally:
            log.info("producer {} over".format(i))
            stream.cancel()
            pbar.close()


async def get_data_async(files, columns=None, producer_num=4, file_handler_num=4):
    # df_list = []
    # try:
    queue = asyncio.Queue(maxsize=file_handler_num)
    global count
    global StreamNum
    global MapperNum
    global FileNum
    StreamNum = producer_num
    MapperNum = file_handler_num
    FileNum = len(files)
    count=0
    data_type = files[0].split('.')[-1]
    windfarm = files[0].split('/')[2]
    if data_type == 'parquet':
        df_all_columns = pd.io.excel.ExcelFile('/tmp/14125.xlsx')
        all_name_dict = dict()
        for sheet in df_all_columns.sheet_names[2:]:
            df_columns = pd.read_excel(df_all_columns, sheet_name=sheet)
            df_columns_new = df_columns.dropna(subset=['SCADA编号'])  # 去除为空的行
            df_columns_new = df_columns_new.set_index('SCADA编号', drop=True)  # 调整index
            name_dict = df_columns_new['中文描述'].T.to_dict()  # 转换为字典
            all_name_dict.update(name_dict)
    else:
        all_name_dict = None

    if columns:
        columns = [k for k, v in all_name_dict.items() if v in columns]
        columns = columns + list(set(columns).difference(set(list(all_name_dict.values()))))
    else:
        columns = columns

    loop = asyncio.get_running_loop()
    executor = ProcessPoolExecutor(max_workers=file_handler_num)
    consumers = [
        loop.create_task(data_handler(queue, executor, i, columns, all_name_dict))
        for i in range(file_handler_num)
    ]

    prod = [
        loop.create_task(fetch_data(queue, files[i::producer_num], i))
        for i in range(producer_num)
    ]

    await asyncio.wait(prod)
    await queue.join()
    for i in range(file_handler_num):
        await queue.put((None, None))
    await asyncio.wait(consumers)
    all_res = [res for future in consumers for res in future.result()]

    # for res in asyncio.as_completed(all_res):
    #     df = await res
    #     df_list.append(df)
    tmp = pd.concat(all_res, axis=0)
    log.info('dump file')
    tmp.to_excel('./{}.xlsx'.format(windfarm))
    del queue
    executor.shutdown()

    return tmp

if __name__ == '__main__':
    dt = datatools.DataTools()
    # wf = datatools.WindFarmInf(graphql_url='http://192.168.3.4:8080/graphql')
    windfarms = [
                 'HXZFCB',
                 'HXZFCC',
                 'HXZFCD',
                 'HXZFCE',
                 'HNFC',
                 'HLXFC',
                 'THLCFC',
                 'HLYJFCB',
                 'TSLFCA',
                 'XCFC',
                 'SXFC',
                 'QDFC',
                 'GSFCA',
                 'ASFC',
                 'SHZFCB',
                 'PDSYXFC',
                 'BKWXFC',
                 'FJFC',
                 'NMFC',
                 'XHTFC',
                 'GJBFCB',
                 'GZXLFCA',
                 'WLTFC',
                 'GZJLFC',
                 'XQFC',
                 'WWZFC',
                 'TMHSFC',
                 'HZPFC',
                 'LXSFC',
                 'HNZBFC',
                 'YGFCA',
                 'FXJSFC',
                 'YTHFC',
                 'BLHFCB',
                 'XHWSFC',
                 'TDBYFCB',
                 'DLSFC',
                 'JXFCLHLSFC',
                 'YJFC',
                 'HXJHFC',
                 'ZJZFC',
                 'PLSFCC',
                 'THFC',
                 'DWSFC',
                 'QXRFFC',
                 'XSFC',
                 'YJWFC',
                 'QSGFC',
                 'JGFC',
                 'CSYFC',
                 'BDLFC',
                 'LJMFC',
                 'LFFC',
                 'HBRXFC',
                 'TKXFC',
                 'GSFCB',
                 'XYBWYFC',
                 'ZBCSFC']
    logging.basicConfig()
    # for windfarm in windfarms:
    #     try:
    #         files1 = dt.get_files("192.168.2.4:9898", windfarm, "second", "2020-11-25", "2021-01-25")
    #         print(files1)
    #         if len(files1)>0:
    #             asyncio.run(get_data_async(files1, producer_num=2, file_handler_num=6))
    #             # asyncio.run(get_data_async(files1,producer_num=2,file_handler_num=2))
    #     except Exception as e:
    #         continue

    files1 = dt.get_files("192.168.2.4:9898", "TYSFCB", "second", "2020-12-15", "2021-01-25", '001')
    # files1 = dt.get_files("192.168.2.4:9898", "TYSFCB", "second", "2020-10-15", "2021-01-25")
    asyncio.run(get_data_async(files1,producer_num=2,file_handler_num=6))
