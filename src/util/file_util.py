# -*- coding:utf-8 -*-

class FileUtil(object):


    @staticmethod
    def add_string(fileName,string):
        with open(fileName,'a') as f:
            f.write((string+'\n').encode('utf-8'))

    @staticmethod
    def write_string(fileName, string):
        with open(fileName, 'w') as f:
            f.write((string + '\n').encode('utf-8'))

    @staticmethod
    def print_string(string,fileName,isStore=False):
        print(string)
        if(isStore):
            FileUtil.add_string(fileName,string)

if __name__ == '__main__':
    ""