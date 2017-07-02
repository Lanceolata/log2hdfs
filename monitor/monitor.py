#!/usr/bin/python
# coding: utf-8

import os
import sys
import optparse
import ConfigParser
from log_format import *
from email_utils import *

warn_log = {}
err_log = {}

def AddLog(msg, excludes, dic):
    if excludes:
        for exclude in excludes:
            if msg.find(exclude):
                return

    if msg not in dic:
        dic[msg] = 1
    else:
        dic[msg] +=1


def ParseLogFile(conf, section, record):
    logformat = LogFormat.init(conf.get(section, "type"))
    if not logformat:
        print "unknow log format type"
        return
    path = conf.get(section, "path")
    if conf.has_option(section, "exclude"):
        excludes = conf.get(section, "exclude").split(",")
    else:
        excludes = None

    with open(path, 'r') as fp:
        first_line = fp.readline().strip()
        offset = 0
        if record.has_option(section, "first") and first_line == record.get(section, "first"):
            if record.has_option(section, "offset"):
                offset = record.getint(section, "offset")
        else:
            if not record.has_section(section):
                record.add_section(section)
            record.set(section, "first", first_line)

        fp.seek(offset)
        for line in fp:
            try:
                logformat.parse(line.strip())
                if logformat.level == WARNING:
                    AddLog(logformat.msg, excludes, warn_log)
                elif logformat.level == ERROR:
                    AddLog(logformat.msg, excludes, err_log)
            except Exception, e:
                print(e)

        record.set(section, 'offset', fp.tell())


def main():
    parse = optparse.OptionParser('usage: -c <conf file path>')
    parse.add_option('-c', '--conf', dest='conf_path', type='string')
    (options, args) = parse.parse_args()
    conf_path = options.conf_path

    if not os.path.exists(conf_path):
        print 'conf file not exists'
        sys.exit(1)

    conf = ConfigParser.ConfigParser()
    conf.read(conf_path)

    record_path = "monitor.rec"
    record = ConfigParser.ConfigParser()
    record.read(record_path)

    for section in conf.sections():
        if section == 'global':
            continue
        try:
            ParseLogFile(conf, section, record)
        except Exception, e:
            print(e)

    record.write(open(record_path, "w"))

    warn = []
    count = 0
    if warn_log:
        warn.append("[warn]\n")
        for k, v in warn_log.items():
            if count >= 20:
                break
            warn.append('%s:%d\n' % (k, v))

    err = []
    count = 0
    if err_log:
        err.append("[error]\n")
        for k, v in err_log.items():
            if count >= 20:
                break
            warn.append('%s:%d\n' % (k, v))

    if warn or err:
        msg = "".join(warn) + "".join(err)
        host = conf.get("global", "host")
        user = conf.get("global", "user")
        password = conf.get("global", "password")
        to_addr = conf.get("global", "to_addr")
        send_email_delay_random_sec(host, user, password, to_addr, msg)


if __name__ == "__main__":
    main()