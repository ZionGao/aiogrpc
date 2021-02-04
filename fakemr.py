# -*- coding: utf-8 -*-
# @Time : 2021/1/29 15:27
# @Author : gaozq
# @File : fakemr.py
# @Software: PyCharm
# @contact: gaozq3@sany.com.cn
# -*- 功能说明 -*-
# 对MapReduce的拙劣模仿
# -*- 功能说明 -*-

import sys
import logging
from rpc_package.data_message_pb2 import *
from rpc_package.data_message_pb2_grpc import *
import json
import pandas as pd
import zipfile
import io
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from sanydata import datatools
from functools import partial
from tqdm import tqdm
import gc
import asyncio
from aiostream import stream, pipe, async_, await_

lock = asyncio.Lock()


class PbarHandler():

    def __init__(self, file_num):
        self.file_num = file_num
        self.pbar_dict = {}
        self.func_process = {}
        self.position = 0
        self.register(file_stream, self.file_num)

    def register(self, fuc, n):
        try:
            self.position += 1
            pbar = tqdm(total=n, position=self.position, ncols=150)
            pbar.set_description(fuc.__name__, refresh=False)
            self.pbar_dict[fuc.__name__] = pbar
            self.func_process[fuc.__name__] = 0
        except Exception:
            raise

    def get_pbar(self, name):
        try:
            pbar = self.pbar_dict[name]
        except Exception:
            raise
        return pbar

    def get_index(self, name):
        index = self.func_process[name]
        return index

    def update_index(self, name):
        index = self.func_process[name]
        self.func_process[name] = index + 1
        return index + 1


async def file_stream(file_list, url='192.168.2.4:9898'):
    global pbar_handler
    name = sys._getframe().f_code.co_name

    options = [('grpc.max_send_message_length', 1024 * 1024 * 1024),
               ('grpc.max_receive_message_length', 1024 * 1024 * 1024),
               ('grpc.enable_retries', True),
               ('grpc.service_config',
                '{ "retryPolicy":{ "maxAttempts": 4, "initialBackoff": "5s", "maxBackoff": "10s", "backoffMutiplier": '
                '2, "retryableStatusCodes": [ "UNAVAILABLE" ] } }')]
    async with grpc.aio.insecure_channel(target=url, options=options) as channel:
        stub = BridgeMessageStub(channel)
        file_stream = stub.GetTargetFile(GetTargetFileInput(filelist=json.dumps(file_list)), timeout=1000)
        file_index = 0
        pbar = pbar_handler.get_pbar(name)
        try:
            async for resp in file_stream.__aiter__():
                file = file_list[file_index]
                async with lock:
                    pbar.set_postfix({'获取文件': file}, refresh=False)
                    pbar.update(1)
                    pbar_handler.update_index(name)
                    file_index += 1
                yield file, resp.output
        except grpc.RpcError as e:
            print(e)
            raise e
        finally:
            file_stream.cancel()
            pbar.close()


def concat(x, y):
    return pd.concat([x, y], axis=0)


def data_load(item, file, columns=None, columns_mapping=None, func=None):
    if item is None:
        return None
    else:
        f = zipfile.ZipFile(file=io.BytesIO(item))
        if len(f.namelist()) > 0:
            file_name = f.namelist()[0]
            file_type = file_name.split('.')[-1]
            turbine_num = file_name.split('#')[0]
            turbine_num = turbine_num.zfill(3)
            pure_data = f.read(file_name)
            if file_type == 'parquet':
                fio = io.BytesIO(pure_data)
                df = pd.read_parquet(fio, engine='fastparquet', columns=columns)
            elif file_type == 'csv':
                sio = io.StringIO(pure_data.decode('utf-8'))
                df = pd.read_csv(sio)
                if columns:
                    df = df[columns]
            else:
                print('unkown file type')
            if columns_mapping:
                df = df.rename(columns=columns_mapping)
            df['turbine_num'] = turbine_num
            windfarm = file.split('/')[2]
            df['windfarm'] = windfarm
            df = df.loc[:, ~df.columns.duplicated()]

            del pure_data, f, item

            if func:
                return func(df)

            return df


def get_parquet_mapping(path='/tmp/14125.xlsx'):
    df_all_columns = pd.io.excel.ExcelFile(path)
    columns_mapping = dict()
    for sheet in df_all_columns.sheet_names[2:]:
        df_columns = pd.read_excel(df_all_columns, sheet_name=sheet)
        df_columns_new = df_columns.dropna(subset=['SCADA编号'])  # 去除为空的行
        df_columns_new = df_columns_new.set_index('SCADA编号', drop=True)  # 调整index
        name_dict = df_columns_new['中文描述'].T.to_dict()  # 转换为字典
        columns_mapping.update(name_dict)

    return columns_mapping


async def mapper(x, func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    name = sys._getframe().f_code.co_name
    global pbar_handler
    global executor
    pbar = pbar_handler.get_pbar(name)
    try:
        file, data = x
        res = loop.run_in_executor(executor, partial(func, data, file, *args, **kwargs))
        async with lock:
            pbar.set_postfix({'处理文件': file}, refresh=False)
            pbar.update(1)
            pbar_handler.update_index(name)

        gc.collect()
    except Exception as e:
        print(e)
        raise e

    return file, res


async def reducer(x, y, func):
    name = sys._getframe().f_code.co_name
    global pbar_handler
    pbar = pbar_handler.get_pbar(name)
    try:
        res = func(x, y)
        async with lock:
            pbar.update(1)
            pbar_handler.update_index(name)
    except Exception as e:
        print(e)
        raise e

    return res


async def data_stream_async(url, files, columns=None, map_func=None, reduce_func=None, initializer=None, producer_num=2,
                            data_handler_num=2, executor_type='process'):
    data_type = files[0].split('.')[-1]
    columns_mapping = get_parquet_mapping() if data_type == 'parquet' else None

    if columns:
        c = [k for k, v in columns_mapping.items() if v in columns]
        c = c + list(set(columns).difference(set(list(columns_mapping.values()))))
    else:
        c = columns

    global pbar_handler
    pbar_handler = PbarHandler(len(files))

    global executor
    if executor_type == 'process':
        executor = ProcessPoolExecutor(max_workers=data_handler_num)
    elif executor_type == 'thread':
        executor = ThreadPoolExecutor(max_workers=data_handler_num)

    if map_func:
        map_task = partial(data_load, func=map_func)
    else:
        map_task = data_load
    pbar_handler.register(mapper, len(files))

    if reduce_func:
        reduce_task = partial(reducer, func=reduce_func)
    else:
        reduce_task = partial(reducer, func=concat)
        initializer = pd.DataFrame()

    file_streams = [
        stream.preserve(file_stream(files[i::producer_num], url))
        for i in range(producer_num)]
    file_list = []

    aws = (stream.merge(*file_streams)
           | pipe.map(async_(lambda x: mapper(x, map_task, c, columns_mapping)), task_limit=data_handler_num)
           | pipe.map(async_(lambda x: file_list.append(x[0]) or x[1]), task_limit=data_handler_num)
           )

    if reduce_func:
        pbar_handler.register(reducer, len(files) - 1)
        rs = stream.reduce(aws, async_(reduce_task), initializer)
        reduced = await stream.takelast(rs, 1)
        return reduced
    else:
        data_list = await asyncio.gather(stream.list(aws))
        data_list = data_list[0]
        tmp_list = list(zip(file_list, data_list))
        tmp_list = sorted(tmp_list, key=lambda pair: files.index(pair[0]))
        if map_func:
            return tmp_list
        else:
            return pd.concat(list(map(lambda pair: pair[1], tmp_list)), axis=0)


def get_data_async(files, columns=None, map_func=None, reduce_func=None, initializer=None, producer_num=2,
                   data_handler_num=2, url='192.168.2.4:9898', executor_type='process'):
    """
    数据获取与批处理
    @param files: 文件列表
    @param columns: 所需数据字段
    @param map_func: Map阶段数据处理函数，异步多线程/多进程进行。未传入则将文件解析为Dataframe并concat后返回，顺序为传入文件列表顺序；传入则返回list[tuple],其中tuple[0]为文件地址,tuple[1]为文件经map_func处理后的结果
    @param reduce_func: Reduce阶段数据处理函数，主进程进行。未传入则返回Map结果，传入则返回Reduce结果
    @param initializer: reduce阶段初始化对象
    @param producer_num: 使用协程同时建立的grpc连接数量，数据获取速度随之成正比，但会受数据处理速度而阻塞。加入重试机制（3次）
    @param data_handler_num: 并行处理数据的协程数量，数据处理速度随之成正比，但会受环境核数、内存限制而任务失败
    @param url: grpc服务地址（ip:port）
    @param executor_type: 并行执行类型，‘process’ - 进程池，适合秒级数据；‘thread’ - 线程池, 适合history,event数据
    @return: linux环境、pycharm环境与以往数据接口(get_data,get_data_process)行为一致，jupyter环境受python原语限制只能返回task，异步调用task.result()取回结果
    """
    coro = data_stream_async(url, files,
                             columns=columns,
                             map_func=map_func,
                             reduce_func=reduce_func,
                             initializer=initializer,
                             producer_num=producer_num,
                             data_handler_num=data_handler_num,
                             executor_type=executor_type
                             )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        tsk = loop.create_task(coro)
        tsk.add_done_callback(
            lambda t: print('Task done: ', t.result()))
        return tsk
    else:
        return asyncio.run(coro)


if __name__ == '__main__':
    dt = datatools.DataTools()
    logging.basicConfig()
    files1 = dt.get_files("192.168.2.4:9898", "TYSFCB", "second", "2020-12-20", "2020-12-25", '001')
    columns = ['time']
    data = get_data_async(files1, producer_num=3, data_handler_num=3,
                          # map_func=lambda x: x,
                          # reduce_func=concat
                          )
    print(data)
