#coding=utf-8

import urllib2
import os,sys,time,re
import MySQLdb
import hashlib
import requests
import random
from lxml import etree
from urlparse import urlparse,urljoin
from Queue import Queue


user_agents = [  
                    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',  
                    'Opera/9.25 (Windows NT 5.1; U; en)',  
                    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',  
                    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',  
                    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',  
                    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',  
                    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",  
                    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",  
                    ]

#连接mysql数据库
def connect_mysql():
    conn = MySQLdb.connect("123.56.179.3","fuzzy","fuzzy","fuzzy")
    return conn

#calculate md5
def getFileMD5(filepath):
    if os.path.isfile(filepath):
        f = open(filepath,'rb')
        md5obj = hashlib.md5()
        md5obj.update(f.read())
        hash = md5obj.hexdigest()
        f.close()
        return str(hash).upper()
    return None


#爬取类
class siteCrawler():
    def __init__(self,task_id,job_id,site,depth,ext_list,max_filesize,download_dir):
        self.task_id = task_id
        self.job_id = job_id
        self.site = site
        self.depth = depth
        self.ext_list = ext_list
        self.max_filesize = max_filesize
        self.download_dir = download_dir

    def fileDownloader(self,url):
        task_id = self.task_id
        job_id = self.job_id
        max_filesize = self.max_filesize*1024
        download_dir = self.download_dir
        global user_agents
        #print "detected url: " + url
        conn = connect_mysql()
        cur = conn.cursor()
        sql = "SELECT * FROM tbl_crawl_task_file WHERE source_url = '%s'" % url
        try:
            cur.execute(sql)
            row = cur.fetchone()
            if row == None:
                try:
                    agent = random.choice(user_agents)
                    headers = {"User_Agent":agent}
                    #headers = {"User_Agent":agent,'Accept':'text/html;q=0.9,*/*;q=0.8', 'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding':'gzip', 'Connection':'close', 'Referer':None}
                    #print headers
                    req = urllib2.Request(url,headers=headers)
                    #req.add_header('User_Agent',agent)
                    content = urllib2.urlopen(req,timeout=10)
                    #获取并判断文件大小
                    length = content.info().getheader("content-length")
                    if int(length) < max_filesize:
                        data = content.read()
                        download_file = "%s/%s.%s" % (download_dir,str(int(time.time()*1000)),url.split('.')[-1])
                        print "downloaded url: " + url
                        #下载文件
                        with open(download_file, "wb") as code:
                            code.write(data)
                        #MD5校验
                        md5 = getFileMD5(download_file)
                        #conn = connect_mysql()
                        #cur = conn.cursor()
                        sql = "SELECT * FROM tbl_crawl_task_file WHERE md5 = '%s'" % md5
                        try:
                            cur.execute(sql)
                            rows = cur.fetchone()
                            if rows == None:
                                insert_sql = "INSERT INTO tbl_crawl_task_file(task_id,job_id,file_name,extension,md5,source_url,site,download_time,file_size,fullname) VALUES('%s','%s','%s','%s','%s','%s','%s',NOW(),%d,'%s')" % (task_id,job_id,download_file.split('/')[-1],download_file.split('.')[-1],md5,url,site,int(length),download_file)
                                try:
                                    cur.execute(insert_sql)
                                    conn.commit()
                                except Exception, e:
                                    print e
                            else:
                                os.remove(download_file)
                        except Exception, e:
                            print e
                except Exception, e:
                    print e
        except Exception, e:
            print e
        finally :
            cur.close()
            conn.close()
        return
        


    def crawlSite(self):
        site = self.site
        site_json = {}
        site_json["site"] = site
        site_json["depth"] = 0
        global user_agents
        que = Queue()
        que.put(site_json)
        while(not que.empty()):
            #print que.qsize()
            info = que.get()
            #print info["depth"]
            try:
                agent = random.choice(user_agents)
                headers = {"User_Agent":agent}
                #headers = {"User_Agent":agent,'Accept':'text/html;q=0.9,*/*;q=0.8', 'Accept-Charset':'ISO-8859-1,utf-8;q=0.7,*;q=0.3', 'Accept-Encoding':'gzip', 'Connection':'close', 'Referer':None}
                #print headers
                req = urllib2.Request(info["site"],headers=headers)
                #req.add_header('User_Agent',agent)
                resp = urllib2.urlopen(req,timeout=10)
                html = resp.read()
                content = etree.HTML(html)
                urlList = content.xpath('//a/@href')
                if len(urlList) == 0:
                    hrefs = content.xpath('//script[@type="text/javascript"]/text()')
                    #print hrefs
                    example=re.compile(r"window.location")
                    for href in hrefs:
                        result = re.match(example,href.strip())
                        if result != None:
                            jump_url = result.string.split('\"')[1]
                            if urlparse(jump_url).netloc == "":
                                jump_url = urljoin(info["site"],jump_url)
                            jump_url_info = {"site":jump_url,"depth":info["depth"]}
                            que.put(jump_url_info)
                else:
                    for url in urlList:
                        if urlparse(url).netloc == "":
                            url = urljoin(info["site"],url)
                        #print url
                        if urlparse(url).path.split('.')[-1] in ext_list and urlparse(url).netloc.split('.')[1] == site.split('.')[1]:
                            self.fileDownloader(url)
                        else:
                            if info["depth"] < depth:
                                #print urlparse(url).netloc.split('.')
                                try:
                                    if urlparse(url).netloc.split('.')[1] == site.split('.')[1]:
                                        new_info = {}
                                        new_info["site"] = url
                                        new_info["depth"] = info["depth"] + 1
                                        #print new_info["depth"]
                                        que.put(new_info)
                                except Exception, e:
                                    continue
            except Exception, e:
                print e
        print "Finish"
        return    
                        


if __name__ == '__main__':
    try:
        task_id = sys.argv[1]
        job_id = sys.argv[2]
        site = sys.argv[3]
        depth = int(sys.argv[4])
        ext_list = sys.argv[5].split(',')
        max_filesize = int(sys.argv[6])
        download_dir = sys.argv[7]

        site_info = urlparse(site)
        if site_info.scheme == "":
            site = "http://" + site

    except:
        sys.exit()

    crawler = siteCrawler(task_id,job_id,site,depth,ext_list,max_filesize,download_dir)
    crawler.crawlSite()


