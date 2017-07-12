#!/usr/bin/python
# coding: utf-8

import os
import sys
import time
import datetime
import optparse
import subprocess
import ConfigParser

def exec_cmd(command):
    print command
    try:
        output = subprocess.check_output(command, shell=True,
                                         stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError, e:
        print(e)
        print(output)
        return None

    return output


def get_option(conf, section, option, default):
    if conf.has_option(section, option):
        return conf.get(section, option)
    else:
        return default


def set_option(conf, section, option, value):
    if conf.has_section(section):
        conf.set(section, option, value)
    else:
        conf.add_section(section)
        conf.set(section, option, value)


def make_complete_mark(path):
    cmd = "hadoop fs -touchz %s/__done__.k2h" % path
    res = exec_cmd(cmd)


def count_hdfs_files(path):
    cmd = "hadoop fs -ls %s | wc -l" % path
    count_new = exec_cmd(cmd)
    try:
        count_new = int(count_new)
    except Exception, e:
        print(e)
        count_new = sys.maxint
    return count_new


def diff_hdfs_files(topic, format, record):
    ts = int(get_option(record, topic, "ts", time.time() - 3600))
    count = int(get_option(record, topic, "count", sys.maxint))
    print count
    hdfs_path = time.strftime(format, time.localtime(ts))
    count_new = count_hdfs_files(hdfs_path)
    if not count_new:
        print("count_hdfs_files failed")
        return False

    print "count_new:", count_new
    if count_new == count:
        make_complete_mark(hdfs_path)
        ts = ts + 3600
        hdfs_path = time.strftime(format, time.localtime(ts))
        count_new = count_hdfs_files(hdfs_path)
        print "update ts:", ts, " count:", count_new

    count = count_new
    set_option(record, topic, "count", str(count))
    set_option(record, topic, "ts", str(ts))
    return True


def main():
    parse = optparse.OptionParser('usage: python k2h_done.py -c <conf file path>')
    parse.add_option('-c', '--conf_path', dest='conf_path', type='string')
    (options, args) = parse.parse_args()
    conf_path = options.conf_path

    if not os.path.isfile(conf_path):
        print("Ivalid conf path[%s]" % conf_path)
        sys.exit(2)

    conf = ConfigParser.ConfigParser()
    conf.read(conf_path)

    record_path = get_option(conf, "global", "record", "k2h_done.rec")

    record = ConfigParser.ConfigParser()
    record.read(record_path)

    for section in conf.sections():
        if section == "global":
            continue

        format = get_option(conf, section, 'path_format', None)
        if not format:
            print("Invalid topic[%s] format[%s]" % (section, format))
            sys.exit(2)

        if not diff_hdfs_files(section, format, record):
            print("diff_hdfs_files failed")
            sys.exit(2)

    record.write(open(record_path, "w"))

if __name__ == '__main__':
        main()
