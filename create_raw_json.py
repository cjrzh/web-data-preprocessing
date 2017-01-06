# coding: utf-8
'''
将Solr中的所有文档拖到hdfs上，作为Slor的备份，也可以用于分词语料库
注：已经拖了18854 条文档
'''
import sys
import os
import urllib.request
import json
from hdfs import *
import datetime
import iso8601


def write_to_hdfs(fpath,fname,flist,data,Force=False):
    '''
    将单个文件写到hdfs上，使用python的hdfs包
    fpath，fname表示文件路径和文件名
    flist表示当前路径下的文件名列表
    Force表示在待写入文件已存在的情况下是否强制执行
    '''
    if  fname in flist and not Force:
        return
    try:
        Hclient.write(fpath+'/'+fname, data=data, encoding='utf-8')
    except Exception as e:
        print(e)


def get_filename(doc):
    '''
    获取文件路径和文件名
    文件路径通过文档时间组织，通过解析lastModified字段获得
    文件名为id字段转换得到
    '''
    lastime = iso8601.parse_date(doc['lastModified']).astimezone()
    fpath = '/corpus/raw_json/' + str(lastime.year) + '/' + str(lastime.month) + '/' + str(lastime.day)
    fname=doc['id'].replace(':','@').replace('/','@')+'.json'
    return fpath,fname


def get_doc(start=0):
    query = 'http://bigdata107:8983/solr/test4/select?q=*%3A*&wt=json'
    response = urllib.request.urlopen(query)
    str_res = response.read().decode('utf-8')
    json_res = json.loads(str_res)
    total_num = json_res['response']['numFound']
    print(total_num, "documents found.")
    #start=5000
    rows=10
    while start+rows<=total_num:
        current_query=query+'&start='+str(start)+'&rows='+str(rows)
        #print(current_query)
        print("真在处理第("+str(start)+"-"+str(start+rows)+")文档/共"+str(total_num)+"条文档...............")
        response = urllib.request.urlopen(current_query)
        str_res = response.read().decode('utf-8')
        json_res = json.loads(str_res)
        for doc in json_res['response']['docs']:
            fpath,fname=get_filename(doc)
            #print("导出:    "+doc['title'])
            print(fpath+'/'+fname)
            Hclient.makedirs(fpath)
            flist=Hclient.list(fpath)
            write_to_hdfs(fpath,fname,flist,json.dumps(doc,ensure_ascii=False))

        print("=====================================================================================================")
        start+=10


if __name__ == '__main__':
    Hclient = Client("http://bigdata107:50070", root="/", timeout=100)
    get_doc()
