#!/usr/bin/python
# coding: utf-8

import time
import random
import socket
import smtplib
import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class EmailUtils(object):
    def __init__(self, host, user, password, to_addrs, from_addr="toolkit",
                 port=25, charset='utf-8'):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        if isinstance(to_addrs, basestring):
            self._to_addrs = to_addrs.split(',')
        elif isinstance(to_addrs, list):
            self._to_addrs = to_addrs
        else:
            raise TypeError("to_addrs not a list or a valid string: %r" %
                            to_addrs)
        self.from_addr = from_addr
        self._charset = charset
        self._msg = MIMEMultipart()

    def set_subject(self, subject, charset=None):
        self._msg['Subject'] = Header(subject, charset)

    def set_content(self, mail_type, content):
        if mail_type == "text":
            part = MIMEText(content, 'plain', self._charset)
            self._msg.attach(part)

    def send(self):
        self._msg['From'] = "toolkit"
        self._msg['To'] = ','.join(self._to_addrs)

        if self._msg['Subject'] is None:
            self._msg['Subject'] = socket.gethostname() + ":" + \
                                   datetime.datetime.now().strftime('%Y%m%d%H%M%S')

        server = smtplib.SMTP()
        server.connect(self._host, self._port)
        server.starttls()
        server.login(self._user, self._password)
        server.sendmail(self._user, self._to_addrs,
                        self._msg.as_string())
        server.close()


def send_email_delay_random_sec(host, user, passwd, to_addr, msg):
    t_out = random.randint(0, 30)
    time.sleep(t_out)
    eu = EmailUtils(host, user, passwd, to_addr)
    eu.set_content('text', msg)
    eu.send()
