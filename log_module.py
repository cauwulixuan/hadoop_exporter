#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

def basic():
    # basic usage
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d]-[%(levelname)s]: %(message)s')
    logger = logging.getLogger("__name__")
    logger.debug("This is basic debug.")
    logger.info("This is basic info")
    logger.warning("This is basic warning")
    logger.error("This is basic error")
    logger.critical("This is basic critical")

def basic_file():
    # basic usage by writting log in a single file.
    logger2 = logging.getLogger('__name__')

    # 定义一个handler用于输出日志到文件.
    handler = logging.FileHandler("log_module.log")
    # logger.setLevel(Level1) 表示当前模块应该记录Level1级别的日志.
    logger2.setLevel(logging.DEBUG)
    # handler.setLevel(Level2) 表示当前handler应该记录Level2级别的日志.
    # 前提是Level1级别比Level2级别低.
    # 比如, 当Level1=DEBUG, Level2=WARNING时, 实际会记录所有Level1(即DEBUG)的日志,
    # 但是因为handler设置了写入"log_module.log"的是WARNING级别的日志, 所以该日志文件中只会保留Level2级别的日志.
    # 反过来，如果Level1=WARNING, Level2=DEBUG时, 因为只记录了WARNING级别的日志, 即时handler设置DEBUG级别的日志，
    # 也无法往"log_module.log"文件中写入DEBUG级别的日志, 所以该日志文件还是保留WARNING级别的日志.
    handler.setLevel(logging.ERROR)

    # 定义一个handler用于输出日志到控制台
    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)

    fmt = logging.Formatter(fmt='%(asctime)s %(filename)s[line:%(lineno)d]-[%(levelname)s]: %(message)s')
    handler.setFormatter(fmt)
    stream.setFormatter(fmt)

    logger2.addHandler(handler)
    logger2.addHandler(stream)

    logger2.debug("This is basic file debug.")
    logger2.info("This is basic file info")
    logger2.warning("This is basic file warning")
    logger2.error("This is basic file error")
    logger2.critical("This is basic file critical")

def log_config():
    import logging.config
    logging.config.fileConfig("logging.conf", disable_existing_loggers=False)

def expensive_func1():
    print "expensive_func1"
    return "expensive_func1"

def expensive_func2():
    print "expensive_func2"
    return "expensive_func2"

def best_practice():
    import logging.config
    logging.config.fileConfig("logging.conf")
    logger = logging.getLogger("log_module")
    logger1 = logging.getLogger()
    print logging.getLogger().name

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('Message with %s, %s' %(expensive_func1(), expensive_func2()))
    if logger1.isEnabledFor(logging.WARNING):
        logger1.error('Message with %s, %s' %(expensive_func1(), expensive_func2()))

def main():
    # basic()
    # basic_file()
    # best_practice()
    from utils import get_module_logger
    log = get_module_logger(__name__)
    log.info("log module info msg.")

if __name__ == '__main__':
    main()