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
    print("exec cmd[%s]" % command)
    try:
        output = subprocess.check_output(command, shell=True,
                                         stderr=subprocess.STDOUT)
        print(output)
    except subprocess.CalledProcessError, e:
         print(e)


def rsync(dtime, dir_path, ips, command):
    for ip in ips.split(','):
        sub_dir = "%s/%s" % (dir_path, ip)
        if not os.path.isdir(sub_dir):
            os.makedirs(sub_dir)

        cmd = command.replace('%ip', ip)
        cmd = "%s %s" % (dtime.strftime(cmd), sub_dir)
        exec_cmd(cmd)


def aggregator(dtime, dir_path, command, section):
    file_format = os.path.basename(command)
    tmp_file = "/tmp/%s" % section
    cmd = "cat %s/*/%s > %s" % (dir_path, dtime.strftime(file_format), tmp_file)
    exec_cmd(cmd)
    return tmp_file


def copy2hdfs(dtime, local_path, hdfs_path):
    if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
        hdfs_path = dtime.strftime(hdfs_path)
        hdfs_dir = os.path.dirname(hdfs_path)
        cmd = "hadoop fs -mkdir -p %s" % hdfs_dir
        exec_cmd(cmd)
        cmd = "hadoop fs -put %s %s" % (local_path, hdfs_path)
        exec_cmd(cmd)


def clean_file(dir_path, ips, retention):
    for ip in ips.split(','):
        sub_dir = "%s/%s" % (dir_path, ip)
        for name in os.listdir(sub_dir):
            inner = os.path.join(sub_dir, name)
            if time.time() - os.path.getmtime(inner) > retention:
                os.remove(inner)


def rsync_to_hdfs(conf, section):
    if not conf or not section:
        return

    if not conf.has_option(section, "root.dir") or \
       not conf.has_option(section, "delay.seconds") or \
       not conf.has_option(section, "ips") or \
       not conf.has_option(section, "command") or \
       not conf.has_option(section, "hdfs.path") or \
       not conf.has_option(section, "retention.seconds"):
        print("rsync_to_local invalid section")

    root_dir = conf.get(section, "root.dir")
    delay = conf.getint(section, "delay.seconds")
    ips = conf.get(section, "ips")
    command = conf.get(section, "command")
    hdfs_path = conf.get(section, "hdfs.path")
    retention = conf.getint(section, "retention.seconds")

    dir_path = os.path.join(os.path.abspath(root_dir), section)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)

    dtime = datetime.datetime.now()-datetime.timedelta(seconds=delay)

    rsync(dtime, dir_path, ips, command)
    local_path = aggregator(dtime, dir_path, command, section)
    copy2hdfs(dtime, local_path, hdfs_path)
    if os.path.isfile(local_path):
        os.remove(local_path)
    clean_file(dir_path, ips, retention)

def main():
    parse = optparse.OptionParser('usage: python rsync2hdfs.py [OPTION]...')
    parse.add_option('-c', '--conf', dest='conf', type='string',
                     default=None)
    (options, args) = parse.parse_args()
    conf_path = options.conf

    if not conf_path or not os.path.exists(conf_path):
        print("invalid conf path")
        sys.exit(2)

    conf = ConfigParser.ConfigParser()
    conf.read(conf_path)

    for section in conf.sections():
        rsync_to_hdfs(conf, section)


if __name__ == '__main__':
    main()
