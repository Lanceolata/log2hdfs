#!/usr/bin/python
# coding: utf-8

import datetime

[INFO, WARNING, ERROR, UNKNOW] = range(4)

levels = {
    'INFO': INFO,
    'WARNING': WARNING,
    'WARN': WARNING,
    'ERROR': ERROR,
    'UNKNOW': UNKNOW,
    INFO: 'info',
    WARNING: 'warning',
    ERROR: 'error',
    UNKNOW: 'unknow',
}


class LogFormat(object):

    @staticmethod
    def init(t):
        if t == 'old':
            return OldLogFormat()
        elif t == 'new':
            return NewLogFormat()
        else:
            return None

    def parse(self):
        raise NotImplementedError


class OldLogFormat(LogFormat):
    def __init__(self):
        self.ts = 0
        self.level = UNKNOW
        self.msg = ""

    def parse(self):
        raise NotImplementedError


class NewLogFormat(LogFormat):
    def __init__(self):
        self.ts = 0
        self.level = UNKNOW
        self.msg = ""

    def parse(self, line):
        line_list = line.strip().split(" ", 3)
        self.ts = datetime.datetime.strptime(
            "%s %s" % (line_list[0], line_list[1]), "%Y-%m-%d %H:%M:%S.%f")
        self.level = levels.get(line_list[2], UNKNOW)
        self.msg = line_list[3]
