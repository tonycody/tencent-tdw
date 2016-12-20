#!/usr/bin/env python
#-*- coding: utf-8 -*-
# ******************************************************************************
# 程序名称:     tdw_general
# 功能描述:     
# 输入参数:     
# 目标表名:
# 数据源表:
# 创建人名:     payniexiao
# 创建日期:     2013-9-12
# 版本说明:     v1.0
# 公司名称:     tencent
# 修改人名:
# 修改日期:
# 修改原因:
# ******************************************************************************
import sys
import time
import string
import datetime
import re

targetScripName = ''
cycleDate = ''

def usage():
    print  "Usage:" + sys.argv[0] + " sql_script.sql [yyyymmdd]"

def log(*x):
    cur_time = time.strftime('[%Y-%m-%d %H:%M:%S] ', time.localtime())
    print cur_time,
    for a in x:
        print a,
    print
       
def check_input(tdw,argv):
    global cycleDate
    global targetScripName
    if len(argv) != 2:
        tdw.WriteLog("argv is error.")
        usage()
        sys.exit(1)
    else:
        targetScripName = argv[0]
        print targetScripName
        cycleDate = argv[1]
        tdw.WriteLog("check_input:script name:%s date:%s" % (targetScripName,cycleDate))
        print "check input complete"    
       
def replaceTimeMacro(tdw, sql, timeValue):
    pattern = re.compile(r'\$\{\s*[A-Z]{4,10}\s*[+-]*\s*[0-9]*[MWDH]*\s*\}')
    newSql = ''''''
    startPos = 0
    isMatch = False
    for match in pattern.finditer(sql):    
        isMatch = True   
        newSql += sql[startPos:match.start(0)]        
        timeMarco = sql[match.start(0):match.end(0)]
    
        strlen = len(timeValue)
        year = 0
        month = 0
        
        if strlen <= 10:        
            if strlen == 6:
                timeValue += '0100'
        
            elif strlen == 8:       
                timeValue += '00'
        
            elif strlen == 10:
                pass
            else:
                raise  Exception("Only support YYYYMM, YYYYMMDD, YYYYMMDDHH now, but input is %s" %timeValue)
        
        macroStartPos = -1
        macroEndPos = -1
        opNumStartPos = -1
        opNumEndPos = -1
            
        op = 0
        opLevel = 0
        
        i = 0            
        for c in timeMarco:
            #print c
            if (c == 'Y' or c == 'M' or c == 'W' or c == 'D' or c == 'H'):             
                if macroStartPos == -1:
                    macroStartPos = i
                                      
                if opNumStartPos != -1:                   
                    opNumEndPos = i
                    opLevel = c
                                   
            elif(c == '+' or c == '-'):             
                macroEndPos = i
                op = c
                
            elif(c <= '9' and c >= '0'):           
                if(opNumStartPos == -1):               
                    opNumStartPos = i
                                  
            elif(c == '}'):            
                if(macroStartPos != -1 and opNumStartPos == -1):               
                    macroEndPos = i            
                else:    
                    i+=1           
                    continue
                                
            else:   
                i+=1        
                continue
            i+=1
        
        macroPart = timeMarco[macroStartPos:macroEndPos]
        
        if opNumStartPos > 0 and opNumEndPos > 0 and opNumEndPos > opNumStartPos:
            opNum = string.atoi(timeMarco[opNumStartPos:opNumEndPos])
        else:
            opNum = 0;
                   
        #opNum = string.atoi(timeMarco[opNumStartPos:opNumEndPos])
        replaceValue = '' 
        
        dt = datetime.datetime(string.atoi(timeValue[0:4]),
           string.atoi(timeValue[4:6]),
           string.atoi(timeValue[6:8]),
           string.atoi(timeValue[8:10]))
        
        print dt
        
        if(macroPart == 'YYYYMM'):
            if(op == 0):
                replaceValue = dt.strftime('%Y%m')
            elif(op == '+'):
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    newMonth = (month + opNum -1) % 12 + 1
                    year += (month + opNum -1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m')
                else:
                    raise  Exception("time macro mode is YYYYMM, you can only add or minus use 'M'")
            else:
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    monthTotal = year * 12 + month
                    if(monthTotal < opNum):
                        raise  Exception("month op number is unvalid:%d"  %opNum)                   
                    newMonth = (monthTotal - opNum -1) % 12 + 1
                    year = (monthTotal - opNum - 1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m')
                else:
                    raise  Exception("time macro mode is YYYYMM, you can only add or minus use 'M'")
                
        elif(macroPart == 'YYYYMMDD'):
            if(op == 0):
                replaceValue = dt.strftime('%Y%m%d')
            elif(op == '+'):
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    newMonth = (month + opNum -1) % 12 + 1
                    year += (month + opNum -1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m%d')
                    
                elif(opLevel == 'W'):
                    replaceValue = (dt + datetime.timedelta(weeks=opNum)).strftime('%Y%m%d')
                elif(opLevel == 'D'):
                    replaceValue = (dt + datetime.timedelta(days=opNum)).strftime('%Y%m%d')
                else:
                    raise  Exception("time macro mode is YYYYMMDD, you can only add or minus use 'M' ,'D', 'W'")
            else:
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    monthTotal = year * 12 + month
                    if(monthTotal < opNum):
                        raise  Exception("month op number is unvalid:%d"  %opNum)                   
                    newMonth = (monthTotal - opNum -1) % 12 + 1
                    year = (monthTotal - opNum - 1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m%d')
                   
                elif(opLevel == 'W'):
                    replaceValue = (dt - datetime.timedelta(weeks=opNum)).strftime('%Y%m%d')
                elif(opLevel == 'D'):
                    replaceValue = (dt - datetime.timedelta(days=opNum)).strftime('%Y%m%d')
                else:
                    #print 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
                    raise  Exception("time macro mode is YYYYMMDD, you can only add or minus use 'M' ,'D', 'W'")
        elif (macroPart == 'YYYYMMDDHH'):
            if(op == 0):
                replaceValue = dt.strftime('%Y%m%d%H')
            elif(op == '+'):
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    newMonth = (month + opNum -1) % 12 + 1
                    year += (month + opNum -1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m%d%H')
                    
                elif(opLevel == 'W'):
                    replaceValue = (dt + datetime.timedelta(weeks=opNum)).strftime('%Y%m%d')
                elif(opLevel == 'D'):
                    replaceValue = (dt + datetime.timedelta(days=opNum)).strftime('%Y%m%d%H')
                elif(opLevel == 'H'):
                    replaceValue = (dt + datetime.timedelta(hours=opNum)).strftime('%Y%m%d%H')
                else:
                    raise  Exception("time macro mode is YYYYMM, you can only add or minus use 'M' ,'D' or 'H'")
            else:
                if(opLevel == 'M'):
                    year = string.atoi(timeValue[0:4])
                    month =  string.atoi(timeValue[4:6])
                    monthTotal = year * 12 + month
                    if(monthTotal < opNum):
                        raise  Exception("month op number is unvalid:%d"  %opNum)                  
                    newMonth = (monthTotal - opNum -1) % 12 + 1
                    year = (monthTotal - opNum - 1) / 12
                    dtt = datetime.datetime(year,newMonth,
                        string.atoi(timeValue[6:8]),
                        string.atoi(timeValue[8:10]))                  
                    replaceValue = dtt.strftime('%Y%m%d%H')
                   
                elif(opLevel == 'W'):
                    replaceValue = (dt - datetime.timedelta(weeks=opNum)).strftime('%Y%m%d%H')
                elif(opLevel == 'D'):
                    replaceValue = (dt - datetime.timedelta(days=opNum)).strftime('%Y%m%d%H')
                elif(opLevel == 'H'):
                    replaceValue = (dt - datetime.timedelta(hours=opNum)).strftime('%Y%m%d%H')
                else:
                    raise  Exception("time macro mode is YYYYMM, you can only add or minus use 'M' ,'D' or 'H'") 
        else:
            raise  Exception("unvalid time macro mode:%s" %macroPart)       
        
        #print "%%%%%%%%%%%%%%%%%%%%%%"
        #print replaceValue
        #print "%%%%%%%%%%%%%%%%%%%%%%"
        
        newSql += replaceValue
        
        startPos = match.end(0)
        #print newSql
    
    if isMatch == False:
        newSql = sql
    
    return newSql

def get_sql(tdw):
    try:
        global targetScripName
        filename = targetScripName
        tdw.WriteLog("open file:" + filename)
        f = open(filename,'r')
        
        sql = f.read()
        tdw.WriteLog("raw sql:-----------------------------\n" + sql)
        sql = replaceTimeMacro(tdw, sql, cycleDate)
        f.close()
        return sql
    except Exception, x:
        print x
        tdw.WriteLog("read or format sql error: %s" %str(x))
        #f.close()
        sys.exit(1)

def tdw_execute(tdw,sql):
    #log("Execute PYSQL:",sql)
    tdw.WriteLog(sql)
    
    #split sqls as ";", if the SQL contain ";", please use like "\;"
    sqlArray = sql.split(";")
    combineSql = ''
    for item in sqlArray:
        if item == None:
            continue
        
        itemLen = len(item)
        
        if itemLen < 1:
            continue
        
        if item[itemLen-1] == '\\':
            combineSql += item + ';'
            continue
        else:
            combineSql += item
        
        sqlStr = combineSql.strip()
        tdw.WriteLog("Start to run SQL : %s" %sqlStr)
        res = tdw.execute(sqlStr)
        #for i in res:
        #    print i
        tdw.WriteLog("Run over of SQL : %s" %sqlStr)
        combineSql = ''
    
    return 1
    
def TDW_PL(tdw, argv=[]):
    global targetScripName
    global cycleDate
    check_input(tdw,argv)
    log("tdw start,script name:%s , timemacro:%s" % (targetScripName, cycleDate))
    tdw.execute("show tables")   
    sql = get_sql(tdw)
    
    tdw_execute(tdw,sql)
    
    
    
#if __name__ == "__main__":
#    main()
