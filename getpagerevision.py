# -*- coding: utf-8 -*-
"""
Created on Thu Jan 10 18:24:22 2019

@author: CCJ
"""

import pymysql
import pandas as pd

#在这里修改需要取的词条的名称,注意数据库中的词条基本上首字母时大写的
page_title = "Welding"
table_name = "revision_" + page_title
conn1 = pymysql.connect(host="localhost",
                        user="root", 
                        password="111111",
                        db="wikiki", 
                        port=3306)
conn2 = pymysql.connect(host="localhost", 
                        user="root", 
                        password="111111",
                        db="wikilinux", 
                        port=3306)
cursor1 = conn1.cursor()
cursor2 = conn2.cursor()
sql_select_pagetitle = "select page_id from page where page_namespace='0' and page_title = '"+page_title+"'"
results = pd.DataFrame(columns=['revid','pageid','textid','userid','username','comment','timestamp','minoredit','deleted','text','flag'])
 
try:
    cursor1.execute(sql_select_pagetitle)
    results_pageid = cursor1.fetchone()
    page_id = results_pageid[0]
    sql_select_revision = "select * from revision where rev_page = '"+str(page_id)+"'"
    cursor1.execute(sql_select_revision)
    results_revision = cursor1.fetchall()
    for row in results_revision:
        results = results.append({'revid':str(row[0]),
                                  'pageid':str(row[1]),
                                  'textid':str(row[2]),
                                  'userid':str(row[4]),
                                  'username':row[5],
                                  'comment':str(row[3],encoding='utf8'),
                                  'timestamp':str(row[6],encoding='utf8'),
                                  'minoredit':str(row[7]),
                                  'deleted':str(row[8])},ignore_index=True)
    sql_drop_table = "drop table if exists "+table_name
    cursor2.execute(sql_drop_table)
    sql_create_table = "create table "+table_name+" (revid int(10) NOT NULL, pageid int(10) unsigned NOT NULL,textid int(10) unsigned NOT NULL,userid int(10) unsigned NOT NULL,username varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,comment varbinary(767) NOT NULL, timestamp binary(14) NOT NULL,minoredit tinyint(3) unsigned NOT NULL,deleted tinyint(3) unsigned NOT NULL,text mediumblob,flag tinyblob)"
    cursor2.execute(sql_create_table) 
    for row in results_revision:
        sql_insert_table = "insert into "+table_name+" (`revid`,`pageid`,`textid`,`userid`,`username`,`comment`,`timestamp`,`minoredit`,`deleted`) values ('"+str(row[0])+"','"+str(row[1])+"','"+str(row[2])+"','"+str(row[4])+"','"+pymysql.escape_string(row[5])+"','"+pymysql.escape_string(str(row[3],encoding='utf8'))+"','"+str(row[6],encoding='utf8')+"','"+str(row[7])+"','"+str(row[8])+"')"
        cursor2.execute(sql_insert_table)
        conn2.commit()
    print("数据库插入成功！")
finally:
    conn1.close()
    conn2.close()    