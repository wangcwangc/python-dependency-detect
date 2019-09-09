class getoutofloop(Exception): pass
from DBUtils.PooledDB import PooledDB
from threading import RLock
LOCK = RLock()
import urllib.request
import requests
from requests.exceptions import RequestException
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import re
import time
import urllib
import urllib.request
import pymysql
import datetime
from bs4 import BeautifulSoup
import DownloadFile


class getoutofloop(Exception): pass

POOL_watchman = PooledDB(
     creator = pymysql, #使用链接数据库的模块
     maxconnections = None,
     mincached = 10,
     maxcached = 0,
     maxshared = 0,
     blocking = True,
     setsession = [],
     ping = 0,
     host = '219.216.64.161',
     port = 3306,
     user = 'pypi',
     password = '12345678',
     database = 'watchman',
     charset = 'utf8'
 )

def get_page(url):
    headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0;Win64;x64) AppleWebKit/537.36 (KHTML, likeGecko) Chrome/74.0.3729.157 Safari/537.36'}
    try:
        response = requests.get(url,headers=headers)
        if response.status_code == 200:
            return response.text
        return None
    except RequestException:
        return None

#获取每日新增文件
def allfilename():
    response = urllib.request.urlopen("https://pypi.tuna.tsinghua.edu.cn/simple/")
    # response = urllib.request.urlopen("http://mirrors.aliyun.com/pypi/simple/")
    html = response.read()
    data = html.decode('utf-8')
    soup = BeautifulSoup(data, 'lxml')

    i = 0;
    for item in soup.find_all("a"):
        file_name_db = item.string
        #print()
        if file_name_db == None:
            continue
        else:
            executor.submit(do_allfilename,file_name_db, i)
    print("文件更新数：", i)

#获取每日新增文件
def do_allfilename(file_name_db,i):
    with LOCK:
        sql = "SELECT COUNT(*) FROM pypi_info WHERE file_name = '%s'" % (file_name_db)
        try:

            db = POOL_watchman.connection()
            # db = pymysql.connect("localhost", mysqlusername, "12345678", "pypidb")
            # 执行sql语句
            cursor_selectpypiinfo = db.cursor()
            cursor_selectpypiinfo.execute(sql)
            nums = cursor_selectpypiinfo.fetchone()
            cursor_selectpypiinfo.close()
            num = nums[0]
            if num == 0:
                now = datetime.datetime.now()
                now = now.strftime("%Y-%m-%d %H:%M:%S")
                sql_insert = "INSERT INTO pypi_info (file_name, file_date) VALUES ('%s','%s')" % (file_name_db, now)
                try:
                    cursor_insertpypi = db.cursor()
                    # 执行sql语句
                    cursor_insertpypi.execute(sql_insert)
                    # 执行sql语句
                    db.commit()
                    cursor_insertpypi.close()
                    print("INSERT:", file_name_db, "FINISHED")
                    i = i + 1
                except:
                    print("INSERT pypi_info error")
                    # 发生错误时回滚
                    db.rollback()
            else:
                print("该文件名已存在：", file_name_db)
            db.close()
        except:
            print("find_filename dberror")


def allversionname():
    # SQL 查询语句
    sql = "SELECT * FROM pypi_info"
    try:
        db = POOL_watchman.connection()
        # 执行sql语句
        allversionname = db.cursor()
        allversionname.execute(sql)
        data = allversionname.fetchall()  # 获取所有的数据
        allversionname.close()
        db.close()
    except:
        print("allversionname()  dberror")
    num = 0
    for i in data:
        fileid = i[0]
        filename = i[1]
        try:
            executor.submit(parse_first_page, fileid, filename)
        except Exception:
            pass
        num = num + 1


def parse_first_page(fileid,filename):
    f_lists=find_fileinfo(filename)
    for f_list in f_lists:

        f_version=f_list[0]
        f_date=f_list[1]
        url = 'https://pypi.org/project/' + filename +'/'+f_version+ '/#files'
        try:
            date_html = get_page(url)
        except Exception:
            pass
        url2 = "https://pypi.tuna.tsinghua.edu.cn/simple/" +filename+'/'
        try:
            date_html2 = get_page(url2)
        except Exception:
            pass
        count = find_versionname(filename, f_version)
        if count == 0:
            pattern_all = re.search('<table class="table table--downloads">(.*?)</table>', date_html, re.S)
            html_all_versionname = pattern_all.group(1)
            url_dates_html = re.findall('<th scope="row">(.*?)</th>', html_all_versionname, re.S)
            version_name = ""
            try:
                for url_date_html in url_dates_html:
                    pattern_one = re.search('<a href="(.*?)">', url_date_html, re.S)
                    url_date = pattern_one.group(1)
                    temp_version_name = url_date.split('/')[-1]
                    filetype = temp_version_name.split('.')[-1]
                    if filetype != 'whl':
                        version_name = temp_version_name
                        raise getoutofloop()
            except getoutofloop:
                pass

            if version_name != "":
                pattern_second = re.findall(r'<a (.*?)/a>', date_html2)
                for item in pattern_second:
                    download_url = re.findall('href="../..(.*?)">' + str(version_name) + '<', item, re.S)
                    if download_url != []:
                        url = "https://pypi.tuna.tsinghua.edu.cn" + download_url[0]
                try:
                    update_timeArray = time.localtime(f_date)
                    update_time = time.strftime("%Y-%m-%d %H:%M:%S", update_timeArray)
                    DownloadFile.download_file(url, version_name,path,savepath)
                    DownloadFile.write_versionname(fileid, version_name, filename, f_version,update_time)
                except Exception:
                    print("download error:", version_name)
                    pass
        else:
            print("该文件版本已存在: ","filename:",filename,", version:",f_version)

#查询所有版本
def fileallversion(filename):
    url = 'https://pypi.org/project/' + filename + '/#history'
    date_html = get_page(url)
    versions = []
    re_version = re.findall('<p class="release__version">(.*?)<', date_html, re.S)
    for v in re_version:
        versions.append(v.strip())
    return versions

#查询该版本是否存在，返回查询数量
def find_versionname(filename,version):
    sql = "SELECT COUNT(*) FROM pypi_info_version WHERE version = '%s' and file_name = '%s'" % (version,filename)
    try:
        # 执行sql语句
        db_v = POOL_watchman.connection()
        # db_v = pymysql.connect("localhost","pypi", "12345678", "pypi_all")
        cursor_find_versionname = db_v.cursor()
        cursor_find_versionname.execute(sql)
        nums = cursor_find_versionname.fetchone()
        cursor_find_versionname.close()
        db_v.close()
        num = nums[0]
        return num
    except Exception:
        print("find_versionname() dberror")


#查询文件名，版本及更新时间
def find_fileinfo(filename):
    # print("filename:",filename)
    url = 'https://pypi.org/project/' + filename + '/#history'
    date_html = get_page(url)
    versions = []
    re_version = re.findall('<p class="release__version">(.*?)<.*?<time class="tooltipped tooltipped-s -js-relative-time" datetime="(.*?)"', date_html, re.S)
    for v in re_version:
        templist=[]
        templist.append(v[0].strip())
        timeArray = time.strptime(v[1], "%Y-%m-%dT%H:%M:%S+0000")
        timeStamp = int(time.mktime(timeArray))
        templist.append(timeStamp)
        versions.append(templist)
    # print(versions[0])
    return versions


if __name__ == '__main__':
    start_time = time.time()  # 开始时间
    executor = ThreadPoolExecutor(20)

    path = 'E:\pypi_projects\pypifile_dailyUpdates'
    savepath = "E:\pypi_projects\pypiuncompress_dailyUpdates"

    # allfilename()
    allversionname()

    end_time = time.time()
    print('Total cost time:%s' % (end_time - start_time))