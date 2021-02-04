import os
from logging.handlers import RotatingFileHandler

import logging
import tqdm

class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.tqdm.write(msg)
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

class Logger:
    def __init__(self, name=__name__, file_name='t.log', log_path='/tmp/log/'):
        # log_path = os.path.join(os.path.dirname(__file__), "/tmp/log/'")
        if not os.path.exists(log_path):
            os.mkdir(log_path)

        logging.basicConfig()
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        # 日志格式
        fmt = logging.Formatter(
            '[%(asctime)s][%(filename)s][line:%(lineno)d][%(levelname)s]: %(message)s',
            '%Y-%m-%d %H:%M:%S')

        # sh = logging.StreamHandler()
        # sh.setFormatter(fmt)
        # sh.setLevel(logging.INFO)
        # self.logger.addHandler(sh)

        sh = TqdmLoggingHandler()
        sh.setFormatter(fmt)
        sh.setLevel(logging.DEBUG)
        self.logger.addHandler(sh)


        self.logger.propagate=False

        #将正常日志记录在file_name中，按天滚动，保存14天
        if file_name is not None:
            tf = logging.handlers.TimedRotatingFileHandler(os.path.join(log_path, file_name),
                                                           when='D',
                                                           backupCount=14)
            tf.suffix = "%Y-%m-%d"
            tf.setFormatter(fmt)
            tf.setLevel(logging.DEBUG)
            self.logger.addHandler(tf)


    @property
    def get_log(self):
        """定义一个函数，回调logger实例"""
        return self.logger


