import urllib
import urllib.request
import pymysql
import datetime
import os
import zipfile
import tarfile
from DBUtils.PooledDB import PooledDB

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


def download_file(url,version_name,path, savepath):
    filename = 'E:/pypi_projects/pypifile_dailyUpdates/'+version_name
    filetype = version_name.split('.')[-1]

    if filetype=='whl':
        filename = filename.replace('.whl','.zip')
    elif filetype=='egg':
        filename = filename.replace('.egg', '.zip')
    elif filetype=='exe':
        filename = filename.replace('.exe', '.zip')

    # print('开始下载 : %s' % filename)  # 开始下载
    urllib.request.urlretrieve(url, filename)
    #urllib.request.urlretrieve(url, filename_new)
    # print('下载完成 : %s' % filename)  # 完成下载


    uncompress(path, savepath, version_name)
    # print('解压完成 : %s' % filename)



#解压缩并保留setup,requirement
def uncompress(path, savepath,file_name):
    filetype = file_name.split('.')[-1]
    if filetype == 'whl':
        file_name = file_name.replace('.whl', '.zip')
    elif filetype == 'egg':
        file_name = file_name.replace('.egg', '.zip')
    elif filetype == 'exe':
        file_name = file_name.replace('.exe', '.zip')

    filenamepath = 'E:/pypi_projects/pypifile_dailyUpdates/' + file_name
    # 解压
    try:
    # if threading.acquire(1):
        name = os.path.splitext(file_name)[0]
        filepath = os.path.join(savepath, name)
        if os.path.splitext(file_name)[1] == '.zip':

            dirpath = os.path.join(path, file_name)
            isExists = os.path.exists(filepath)
            # 判断路径是否存在
            if not isExists:
                # 不存在则创建目录
                os.makedirs(filepath)
                file_zip = zipfile.ZipFile(dirpath, 'r')
                for file in file_zip.namelist():
                    file_zip.extract(file, path=filepath)
                file_zip.close()
            os.remove(filenamepath)

        if os.path.splitext(file_name)[1] == '.gz':
            dirpath = os.path.join(path, file_name)
            isExists = os.path.exists(filepath)
            # 判断路径是否存在
            if not isExists:
                t = tarfile.open(dirpath)
                t.extractall(path=savepath)
                t.close()
            # print(filenamepath)
            os.remove(filenamepath)
        # #解压后删除压缩包
        # print("filenamepath:", filenamepath)
        # os.remove(filenamepath)
        # print("删除成功:", filenamepath)
        # threading.lock.release()
    #删除其他文件，只保留setup,requirement文件

        file_name_uncompress=file_name
        filetype_uncompress = file_name_uncompress.split('.')[-1]
        # print("filetype_uncompress:",filetype_uncompress)
        # print("file_name_uncompress1:", file_name_uncompress)
        if filetype_uncompress == 'zip':
            file_name_uncompress = file_name_uncompress.replace('.zip', '')
        elif filetype_uncompress == 'gz':
            file_name_uncompress = file_name_uncompress.replace('.tar.gz', '')
        # print("file_name_uncompress2:", file_name_uncompress)
        path_uncompress = 'E://pypi_projects//pypiuncompress_dailyUpdates//' + file_name_uncompress+"//"
        print("path_uncompress:",path_uncompress)
        list_dir(path_uncompress)

    # R.release()


    except Exception:
        print("解压缩失败：", file_name)

        pass



def list_dir(file_dir):
    match_requirements = "requirements"
    match_setup = "setup"
    match_require = "require"
    dir_list = os.listdir(file_dir)
    for cur_file in dir_list:
        # 获取文件的绝对路径
        path = os.path.join(file_dir, cur_file)
        if os.path.isfile(path): # 判断是否是文件还是目录需要用绝对路径
            if match_setup not in format(cur_file) and match_requirements not in format(cur_file)and match_require not in format(cur_file):
                filePath = os.path.join(path)
                # os.remove(f)
                os.remove(format(filePath))
                # print( "{0} : is file!",format(cur_file))
        if os.path.isdir(path):
            # print( "{0} : is dir!",format(cur_file))
            list_dir(path) # 递归子目录


# 将带版本文件名写入数据库
def write_versionname(fileid, version_name, filename, f_version,update_time):
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")

    sql1 = "INSERT INTO pypi_info_version (version_name,version,file_name,version_date, f_id) VALUES ('%s','%s','%s','%s','%s')" % (version_name,f_version,filename,now,fileid)
    sql2 = "INSERT INTO pypi_info_version_all (file_name, version,update_time,insert_time,f_id) VALUES ('%s','%s','%s','%s','%s')" % (filename, f_version, update_time, now, fileid)
    try:
        # 执行sql语句
        db = POOL_watchman.connection()
        # db = pymysql.connect("localhost", "pypi", "12345678", "pypi_all")
        cursor_write_versionname = db.cursor()
        cursor_write_versionname.execute(sql1)
        cursor_write_versionname.execute(sql2)
        db.commit()
        cursor_write_versionname.close()
        db.close()
    except:
        print("write_versionname() dberror")
        # 发生错误时回滚
        db.rollback()
