# -*- coding:utf-8 -*-

class FileUtil(object):

    DEFAULTLOGFILE = 'log.txt'

    @staticmethod
    def add_string(fileName,string):
        with open(fileName,'a') as f:
            f.write((string+'\n').encode('utf-8'))

    @staticmethod
    def write_string(fileName, string):
        with open(fileName, 'w') as f:
            f.write((string + '\n').encode('utf-8'))

    @staticmethod
    def print_string(string,isStore=False):
        print(string)
        if(isStore):
            FileUtil.add_string(FileUtil.DEFAULTLOGFILE,string)

if __name__ == '__main__':
    FileUtil.add_string('log.txt',u'对付对付的')