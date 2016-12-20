#!/usr/bin/env python
""" Copyright (c) 2010 Ma Can <ml.macana@gmail.com>

This is the core TDW runtime library, enjoy it :)
"""

import sys, re, time

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from array import *

def to_type(line, t):
    '''Type conversion'''
    if t == 'int':
        return int(line)
    elif t == 'bigint':
        return long(line)
    elif t == 'array<int>':
        line = line.strip(" ")
        line = line.strip("[")
        line = line.rstrip(" ")
        line = line.rstrip("]")
        l = line.split(",")
        for i in range(0, len(l)):
            if l[i] == "":
                return array('i')
            else:
                l[i] = int(l[i])
        return array('i', l)
    elif t == 'string':
        return str(line)

class dynamic_type:
    '''Dynamic Type Support'''
    pass

class Configure(object):
    def __init__(self, path):
        self.mark_conf = path
        pass
    
    def getValue(self, name):
        try:
            dom=open(self.mark_conf,'r')
            for l in dom.readlines():
                l=l.strip()
                if l.startswith(name):
                    nametext=l.split('=')[1]
                    dom.close()
                    return nametext.strip()
            return None        
        except Exception:
            return None

import time
import os.path
import socket
import sys
import logging
import shutil
class Mark(object):
    '''
    classdocs
    '''
  
    def __init__(self, parm):
        self.logger=None   
        
        if len(parm) < 2 :
            self.mrk_init('test', 'test', 'test', None)
            return        
        log_path = None
        home = os.getcwd() + os.path.sep + 'mark.conf' 
        if os.path.exists(home):
            cfg = Configure(home)
            isserver = cfg.getValue('server')
            if isserver.upper() == 'Y':
                log_path = cfg.getValue('logdir')
                self.mrk_init(parm[0], parm[1], 1, log_path)
                return
                
        self.mrk_init('test', 'test', 'test', log_path)
        
    def mrk_init(self,task_id,task_name,sub_task_seq,log_file_path):
        #task_id:change to edition of action
        #task_name:change to sub_task_seq of action
        '''paraemt ers needed in dc server'''
        
        self.LogIP=self.GetHostIP()
        self.Seq=task_id
        self.QQUin=task_name
        self.LogType='markusp_guild_'
        self.TradeTime=''
        self.ResultCode=0
        self.ResultInfo=''
        
        '''
        Constructor
        '''
        self.task_id=task_id
        self.task_name=task_name
        self.sub_task_seq=sub_task_seq
        program_name=sys.argv[0].split('\\')[-1]
        self.program_name=program_name
        self.mark_timestamp=''
        self.step_seq=1
        self.step_desc=''
        self.step_status=0
        self.sql_text=''
        self.sql_res=0
        self.sql_rowcnt=0
        self.dic={}
    
        self.param_number=0    
        
        self.log_file_path=log_file_path
        if None == self.log_file_path:
            return 
        if(os.path.exists(self.log_file_path)==False):
            try:
                os.makedirs(self.log_file_path)
            except:
                raise Exception("make mark logdir error while initing mark object")
                
        starttimestr=time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))
        self.file_name = None
        if self.task_name == 'None':
            self.file_name=self.log_file_path+'/'+self.task_id+'.'+starttimestr+'.mark'
        else:
            self.file_name=self.log_file_path+'/'+self.task_id+'.'+self.task_name+'.'+starttimestr+'.mark'
        try:
            self.logger = logging.getLogger("pysql_mark")
            self.logger.setLevel(logging.INFO)
            self.handler = logging.FileHandler(self.file_name)
            self.handler.setLevel(logging.INFO)
            log_formatter = logging.Formatter("%(asctime)s"+ '|' + "%(message)s")
            self.handler.setFormatter(log_formatter)
            self.logger.addHandler(self.handler)
            self.logger.info("begin to execute user script...")
        except Exception,e:
            print e 
            
    def GetHostIP(self):
        ip=''
        try:
            ip=socket.gethostbyname(socket.gethostname())
        except Exception,e:
            ip='127.0.0.1'
        return ip       
    def getId(self):
        return self.task_id
    def getName(self):
        return self.task_name
    def save(self,para_name,para_value):
        self.dic[para_name]=para_value
        self.param_number+=1
    def setMark(self,_mark_desc,_mark_status,_mark_sql,_mark_res,_mark_rowcnt):
        '''paramerters needed in dcserver'''
        self.ResultCode=_mark_status
        self.ResultInfo=_mark_desc
        '''mark content'''
        self.step_desc=_mark_desc
        self.step_status=_mark_status
        self.sql_text=_mark_sql
        self.sql_res=_mark_res
        self.sql_rowcnt=_mark_rowcnt
        self.PrintMark()
        self.dic.clear()
        self.param_number=0
        self.step_seq+=1
        
    def GetParaStr(self):
        str_paras=''
        it=self.dic.iteritems()
        index=1
        if None == self.logger:
            sep = ','
        else:
            sep = chr(0x5)
        for para_name,para_value in it:
            str_paras=str_paras+sep+para_name+sep+str(para_value)
            index+=1
        while self.param_number<=4:
            str_paras=str_paras+sep+sep
            self.param_number+=1
        return str_paras
    
    def PrintMark(self):
        str_paras=self.GetParaStr()
        time_second='%.6f' % time.time()
        timestr=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(float(time_second)))
        self.TradeTime=time_second.replace('.',':')
        if None == self.logger:
            sep = ','
        else:
            sep = '|'
        '''logstr=str(self.task_id)\
        +sep+str(self.task_name)\
        +sep+str(self.sub_task_seq)\
        +sep+str(timestr)\
        +sep+str(self.program_name)\
        +sep+str(self.step_seq)\
        +sep+str(self.step_desc)\
        +sep+str(self.step_status)\
        +sep+str(self.sql_text)\
        +sep+str(self.sql_res)\
        +sep+str(self.sql_rowcnt)\
        +str(str_paras)+'\n'
        '''
        logstr=str(self.step_seq)\
        +sep+str(self.step_desc)
        if(self.logger==None):
            #print 'Open File Error'
            print logstr+'\n'
            return
        else:
            try:
                self.logger.info(logstr)
            except Exception,e:
                print 'Wrie Log Error'
                pass
          
    def finishMark(self):
        if(self.logger!=None):
            try:
                self.logger.info("user script execute end...")
                self.logger.removeHandler(self.handler)
                self.handler.close()
                #os.rename(self.file_name,self.file_name+'.bak')
                #shutil.copy(self.file_name, self.file_name + ".bak")
            except Exception,e:
                print 'finishMark Error'


class TDW:
    '''This is the runtime TDW class.'''
    running = True
    mark=None
    server=None
    ip=None
    port=None
    retrytime=None
    transport=None
    protocol=None
    usrname=None
    passwd=None
    dbname=None
    session=None
    authid=None
    waittime=None
    useInfo=None
    setInfo=[]
    
    def __init__(self, client=None):
        '''do init job'''
        self.cli = client
    
    def init_mark(self,argv):
        self.mark=Mark(argv)
    
    def init_connect(self, server, ip, port, retrytime, transport, protocol,usrname,passwd, dbname, waittime):
        self.server = server
        self.ip = ip
        self.port = port
        self.retrytime = retrytime
        self.transport = transport
        self.protocol = protocol
        self.usrname = usrname
        self.passwd = passwd
        self.dbname = dbname
        self.waittime = waittime
        
    def init_session(self,session,authid):
        self.session = session
        self.authid = authid 
    
    def init_set_use_Info(self,useInfo,setInfo):
        self.useInfo = useInfo
        self.setInfo = setInfo    
         
    def get_use_Info(self):
        return self.useInfo
    
    def get_set_Info(self):
        return self.setInfo
        
    def get_session(self):
        return self.session
    
    def get_authid(self):
        return self.authid    
        
    def saveparam(self,para_name,para_value):
        self.mark.save(para_name,para_value)
            
    def WriteLog(self,_mark_desc,_mark_status=0,_mark_sql='',_mark_res=0,_mark_rowcnt=0):
        self.mark.setMark(_mark_desc,_mark_status,_mark_sql,_mark_res,_mark_rowcnt)
    
    def finishMark(self):
        self.mark.finishMark()
        
    def do_exit(self):
        '''begin to exit'''
        self.running = False

    def execute(self, query):
        '''Execute a SQL query and return the result LIST[0:]'''
        res = None
        
        #added by michealxu
        #print query
        query = query.strip()
        tmpResult = query.split(" ")
        #print tmpResult[0].lower()
        if tmpResult[0].lower() == "use" :
            self.useInfo = query
            #print query
        elif tmpResult[0].lower() == "set" :
            self.setInfo.append(query)
            #print query
        tmpResult = query.split("\t")
        #print tmpResult[0].lower()
        if tmpResult[0].lower() == "use" :
            self.useInfo = query
            #print query
        elif tmpResult[0].lower() == "set" :
            self.setInfo.append(query)
            #print query
        
        
        indexOfRetrytime = 0
        if self.retrytime == 0:
            res = self.cli.execute(query)
        else:
            try:
                res = self.cli.execute(query)
                indexOfRetrytime = self.retrytime
            except Thrift.TException, tx:
                #try:
                #    self.cli.dropSession(self.session, self.authid)
                #except Exception, ee:
                #    print "dropSession failed: %s"  %(self.session)
                #print "drop: %s" %(self.session)
                self.WriteLog("first time execute failed: %s" %(query))
                self.WriteLog(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                print "first time execute failed: %s"  %(tx.message)                                     
                
        #add by cherry start
        try:
            resultlist = socket.getaddrinfo(self.server, None)
            ipcounter = 0
            iplist = [i[4][0] for i in resultlist]
            iplist = list(set(iplist))
            #for i in iplist:
            #    print "%s"%(i)
            ipcounter = len(iplist)
        except socket.error , gaie:
            print "connect to server %s error: %s"%(self.server, gaie[1])
            self.do_exit()
        historyip = list()
        historyip.append(self.ip)		
        #add by cherry end
        
        while indexOfRetrytime < self.retrytime-1:
            try:
                self.WriteLog("start retry: %s" %(query))
                self.transport.close()
                time.sleep(self.waittime)
                #add by cherry start
                num = random.randint(0, ipcounter-1)
                self.ip = iplist[num]
                while indexOfRetrytime+1 < ipcounter and historyip.count(self.ip) > 0:
                    num = random.randint(0, ipcounter-1)
                    self.ip = iplist[num]									
                print "%d time retry execute connect to hive ip:%s"%(indexOfRetrytime+1,self.ip)
                self.WriteLog("%d time retry execute connect to hive ip:%s"%(indexOfRetrytime+1,self.ip))
                historyip.append(self.ip)
                self.transport = TSocket.TSocket(self.ip, self.port)
                #add by cherry end
                #self.transport = TSocket.TSocket(self.server, self.port)
                self.transport = TTransport.TBufferedTransport(self.transport)
                self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
                self.cli = ThriftHive.Client(self.protocol)
                self.transport.open()
                self.cli.audit(self.usrname, self.passwd, self.dbname)
                sname = self.cli.createSession("")
                self.session = sname[0]
                #print "create: %s" %(self.session)
                self.authid = sname[1]      
                res = self.cli.execute("set plcretry=%d" %(indexOfRetrytime+1))  
                self.WriteLog("plcretry: %d" %(indexOfRetrytime+1))  
                self.WriteLog("new session: "+self.session)
                self.WriteLog("new session server: "+self.server) 				
                self.WriteLog("new session ip: "+self.ip) 
                self.WriteLog(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
                res = self.cli.execute("set tdw.username="+self.usrname)
                if self.useInfo != None:
                    #print "self.useInfo"
                    #print self.useInfo
                    res = self.cli.execute(self.useInfo)
                if len(self.setInfo) > 0:
                    for oneset in self.setInfo:
                        #print "oneset"
                        #print oneset
                        res = self.cli.execute(oneset)
                res = self.cli.execute(query)
                indexOfRetrytime = self.retrytime
                #self.cli.dropSession(self.session, self.authid)
                #self.session=None
                #self.authid=None                 
                #print "drop: %s" %(self.session)           
            except Thrift.TException, tx:
                #try:
                #    self.cli.dropSession(self.session, self.authid)
                #except Exception, ee:
                #    print "dropSession failed: %s"  %(self.session) 
                #print "drop: %s" %(self.session)
                indexOfRetrytime = indexOfRetrytime + 1;
                info = sys.exc_info()
                print "%d time retry execute failed: %s"  %(indexOfRetrytime,tx.message) 
                          
        
        if (indexOfRetrytime < self.retrytime):
            self.WriteLog("start retry: %s" %(query))
            self.transport.close()
            time.sleep(self.waittime)
			#add by cherry start
            num = random.randint(0, ipcounter-1)
            self.ip = iplist[num]
            while indexOfRetrytime+1 < ipcounter and historyip.count(self.ip) > 0:
                num = random.randint(0, ipcounter-1)
                self.ip = iplist[num]									
            print "%d time retry execute connect to hive ip:%s"%(indexOfRetrytime+1,self.ip)
            self.WriteLog("%d time retry execute connect to hive ip:%s"%(indexOfRetrytime+1,self.ip))
            self.transport = TSocket.TSocket(self.ip, self.port)
            #add by cherry end
            #self.transport = TSocket.TSocket(self.server, self.port)
            self.transport = TTransport.TBufferedTransport(self.transport)
            self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
            self.cli = ThriftHive.Client(self.protocol)
            self.transport.open()
            self.cli.audit(self.usrname, self.passwd, self.dbname)
            sname = self.cli.createSession("")
            self.session = sname[0]
            #print "create: %s" %(self.session)
            self.authid = sname[1]    
            res = self.cli.execute("set plcretry=%d" %(indexOfRetrytime+1))  
            self.WriteLog("plcretry: %d" %(indexOfRetrytime+1))  
            self.WriteLog("new session: "+self.session)
            self.WriteLog("new session server: "+self.server)   			
            self.WriteLog("new session ip: "+self.ip)       
            self.WriteLog(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))               
            res = self.cli.execute("set tdw.username="+self.usrname)
            if self.useInfo != None:
                #print "self.useInfo"
                #print self.useInfo
                res = self.cli.execute(self.useInfo)
            if len(self.setInfo) > 0:
                for oneset in self.setInfo:
                    #print "oneset"
                    #print oneset
                    res = self.cli.execute(oneset)            
            res = self.cli.execute(query)
        #add end
        
        #try:
        #    if self.running:
        #        res = self.cli.execute(query)
        #    else:
        #        raise IOError (-2, "Interrupted by the caller or fored exit.")
        #except KeyboardInterrupt:
        #    print ("Recv KeyboardInterrupt, please wait for this SQL "
        #           "execution (hint: %s)" % res)
        #    try: 
        #        if res == None: self.cli.recv_execute()
        #    except KeyboardInterrupt:
        #        self.do_exit()
        #        raise IOError (4, "Interrupted by the caller or fored exit.")
        #    except Exception, e:
        #        print "Exception: %s" % str(e)
        #        self.do_exit()
        #        raise IOError (4, "Interrupted by the caller or fored exit.")
        #    self.do_exit()

        try:
            res = self.cli.fetchAll()
            return res
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, your client should be reset."
            self.do_exit()
            raise IOError (4, "Interrupted by the caller or fored exit.")

    def execute2int(self, query):
        '''Execute a SQL query and return a INT result'''
        try:
            if self.running:
                res = self.cli.execute(query)
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, please wait for this SQL execution"
            try: 
                if res == None: self.cli.recv_execute()
            except KeyboardInterrupt:
                self.do_exit()
                raise IOError (4, "Interrupted by the caller or fored exit.")
            except Exception, e:
                print "Exception: %s" % str(e)
                self.do_exit()
                raise IOError (4, "Interrupted by the caller or fored exit.")
            self.do_exit()

        try:
            res = self.cli.fetchAll()
            return int(res[0])
        except TypeError, e :
            print "Result '%s' to INT failed %s" % (res[0], e.message)
            raise IOError (-3, "Result type transform to INT failed, "
                            "TypeError.")
        except ValueError :
            print "Result '%s' to INT failed, ValueError" % res[0]
            raise IOError (-3, "Result type transform to INT failed, "
                            "ValueError.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, your client should be reset."
            self.do_exit()
            raise IOError (4, "Interrupted by the caller or fored exit.")

    def execute2str(self, query):
        '''Execute a SQL query and return a STRING (LIST[0])'''
        try:
            if self.running:
                res = self.cli.execute(query)
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, please wait for this SQL execution"
            try: 
                if res == None: self.cli.recv_execute()
            except KeyboardInterrupt:
                self.do_exit()
                raise IOError (4, "Interrupted by the caller or fored exit.")
            except Exception, e:
                print "Exception: %s" % str(e)
                self.do_exit()
                raise IOError (4, "Interrupted by the caller or fored exit.")
            self.do_exit()

        try:
            res = self.cli.fetchAll()
            return str(res[0])
        except TypeError, e:
            print "Result '%s' to STRING failed %s" % (res[0], e.message)
            raise IOError (-3, "Result type transform to STRING failed, "
                            "TypeError.")
        except ValueError :
            print "Result '%s' to STRING failed" % res[0]
            raise IOError (-3, "Result type transform to STRING failed, "
                            "ValueError.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, your client should be reset."
            self.do_exit()
            raise IOError (4, "Interrupted by the caller or fored exit.")

    def printf(self, string, res):
        '''Print the result in a formated manner'''
        try:
            if (type(res) == type(str())):
                print string + " " + res
            elif (type(res) == type(list())):
                print "Q: '" + string + "' |=> '" + " ".join(res) + "'"
            elif (type(res) == type(int())):
                print string + " " + str(res)
            elif (type(res) == type(None)):
                print "Argument is Not A Type."
        except ValueError :
            print "printf convert STRING failed"
            raise IOError (-3, "printf convert STRING failed, ValueError.")

    def result_str(self, res):
        '''Convert a SQL result to a STRING'''
        try:
            __res = str(res)
        except:
            return None
        return __res

    def result_list(self, res):
        '''Convert a SQL result to a LIST'''
        try:
            __res = list(res)
        except:
            return None
        return __res

    def result_int(self, res):
        '''Convert a SQL result to a INT'''
        try:
            __res = int(res)
        except:
            return None
        return __res

    def uploadJob(self, fname):
        '''Upload a job file to server'''
        if not self.running:
            raise IOError (-2, "Interrupted by the caller or fored exit.")

        print "Tring to upload Job '%s' ..." % (fname)
        read_data = ""
        try:
            f = open(fname, "r")
            read_data = f.read()
            f.close()
        except IOError, ie:
            print "IOError %s" % ie
            return

        if read_data == "":
            print "[WARN] Read file '" + fname + "' w/ ZERO content."
        # ok to upload the file to hiveserver
        self.cli.uploadJob(read_data)

    def getschema(self):
        '''Get the Hive Schema from the server'''
        try:
            if self.running:
                return str(self.cli.getSchema())
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, please wait for get schema"
            self.do_exit()
            self.cli.recv_getSchema()

    def getschemax(self):
        '''Get the SQL schema info the the Hive Scheme'''
        try:
            if self.running:
                schema_string = str(self.cli.getSchema())
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, please wait for get schema"
            self.do_exit()
            self.cli.recv_getSchema()
            return

        # Parse the schema_string
        m = re.findall("name='[^']*", schema_string)
        for i in range(0, len(m)):
            m[i] = m[i].replace("name='", "")
        return m

    def gettypex(self):
        '''Get the SQL type info from the Hive Schema'''
        try:
            if self.running:
                schema_string = str(self.cli.getSchema())
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except KeyboardInterrupt:
            print "Recv KeyboardInterrupt, please wait for get schema"
            self.do_exit()
            self.cli.recv_getSchema()
            return

        # Parse the schema_string
        m = re.findall("type='[^']*", schema_string)
        for i in range(0, len(m)):
            m[i] = m[i].replace("type='", "")
        return m

    def parseschema(self, dict, sql_result):
        '''Parse the SQL result list to a list of lists based on the 'type'
        and 'schema' info in DICT'''
        if sql_result == "" or sql_result == None:
            return list()
        if type(sql_result) != type(list()):
            return list()

        result_list = list()
        column_list = dict['schema']
        type_list = dict['type']
        for i in range(0, len(sql_result)):
            if sql_result[i] == "":
                continue
            new_object = dynamic_type()
            sql_line = sql_result[i].split("\t")
            for j in range(0, len(column_list)):
                setattr(new_object, column_list[j], to_type(sql_line[j], 
                                                            type_list[j]))
            result_list.append(new_object)

        return result_list
        
    def tdw_getrowcount(self):
        '''Get the rowcount from server. The same as %rowcount'''
        try:
            if self.running:
                rc = self.cli.getRowCount()
            else:
                raise IOError (-2, "Interrupted by the caller or fored exit.")
        except TypeError, e:
            print "TypeError %s" % e
            raise IOError (-2, "%s" % e)
        except KeyboardInterrupt, e:
            print "Recv KeyboardInterrupt, your client should be reset."
            self.do_exit()
            raise IOError (-2, "%s" % e)

        return long(rc)

    def tdw_raise(self, errno=None, strerror=None):
        '''Raise a TDW/PL exception (IOError actually)'''
        if type(errno) != type(int()):
            errno = -1
        if type(strerror) != type(str()):
            strerror = "TDW/PL default exception."
        raise IOError(errno, strerror)
