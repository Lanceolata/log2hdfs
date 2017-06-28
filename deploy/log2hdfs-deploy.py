#!/usr/bin/python
# coding: utf-8

from ipinyou_env import *
from fabric.api import *

env.user = user
env.password = password

env.roledefs = {
    'dsp': dsp,
    'adp': adp,
    'stats': stats,
    'vt': vt,
    'sandbox': sandbox
}

def log2kafka_deploy():
    run('rm -rf log2hdfs.tgz log2hdfs')
    run('wget http://192.168.145.242:6666/log2hdfs.tgz')
    run('tar -zxf log2hdfs.tgz')
    with cd('log2hdfs'):
        with cd('thirdparty'):
            run('sh download_thirdparty.sh')
            run('sh build_thirdparty.sh')

        run('run build_log2kafka.sh')


def copy_conf(name):
    run('mkdir -p log2kafka')
    run('cp log2hdfs/conf/%s.conf %s-log.conf log2kafka/' % (name, name))
    run('cp log2hdfs/bin/run_log2kafka.sh log2kafka/')

@roles('sandbox')
def sandbox_deploy():
    log2kafka_deploy()
    #copy_conf('sandbox')


@roles('dsp')
def dsp_test_deploy():
    log2kafka_deploy()
    copy_conf('dsp-test')

