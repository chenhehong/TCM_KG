# -*- coding:utf-8 -*-
import datetime


class DateTimeUtil(object):

    @staticmethod
    def get_datetime_now(self):
        now = datetime.datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%s")