tdw邮件列表：[https://groups.google.com/d/forum/tdw-user](https://groups.google.com/d/forum/tdw-user)
# 环境依赖 #
- [CentOS-x86-64 6.x](http://www.centos.org/download/)
- [Oracle JDK 1.6.x及以上版本](http://www.oracle.com/technetwork/java/javasebusiness/downloads/java-archive-downloads-javase6-419409.html)
- [Apache-ant 1.7.x及以上版本](http://ant.apache.org/bindownload.cgi)
- [PostgreSQL 9.2.x及以上版本](http://www.postgresql.org/download/)
- [Hadoop 2.2.x版本](http://archive.apache.org)
- [Protobuf 2.5.0版本](https://code.google.com/p/protobuf/)
- [hadoop-gpl-compression](https://code.google.com/a/apache-extras.org/p/hadoop-gpl-compression/)

# 编译 #
## 安装c/c++开发环境 ##

    sudo yum groupinstall "Development Tools"
    sudo yum install glibc-static wget unzip

## 安装java开发环境 ##
下载JDK 1.6.x和Apache-ant并安装，设置JAVA_HOME和PATH环境变量，如添加以下到~/.bashrc中（这里将jdk和ant都安装在home中，请根据自己的情况替换相应的路径）： 
 
    echo 'export JAVA_HOME=~/jdk_1.6.0_26' >> ~/.bashrc  
    echo 'export PATH=$JAVA_HOME/bin:~/apache-ant-1.7.1/bin:$PATH' >> ~/.bashrc
    . ~/.bashrc

检查java与ant环境及版本是否正确安装，运行如下命令检查：
> allison@tdw:~$ ant -version  
> Apache Ant version 1.7.1 compiled on June 27 2008  
> allison@tdw:~$ javac -version  
> javac 1.6.0_26

如果返回"-bash: xxx: command not found"，或者版本号低于TDW要求，请确认依赖软件是否安装正确，相应的环境变量是否设置生效。

## 安装protobuf ##

    wget https://protobuf.googlecode.com/files/protobuf-2.5.0.zip
    unzip ./protobuf-2.5.0.zip
    cd ./protobuf-2.5.0
    ./configure
    sudo make install

## 编译TDW QE二进制 ##
	
	下载编译依赖的LZO压缩库：
	wget hadoop-gpl-compression-0.1.0-rc0.tar.gz
	tar xf hadoop-gpl-compression-0.1.0-rc0.tar.gz
    下载qe.zip到当前目录
    unzip ./qe.zip
	cp hadoop-gpl-compression-0.1.0/hadoop-gpl-compression-0.1.0.jar qe/lib/
	进行编译：
    cd qe && ant package

看到
> BUILD SUCCESSFUL  
> Total time: ...  

表示编译成功，生成的二进制包在qe/build/dist目录中，有以下几个目录：
- auxlib:运行时辅助lib  
- bin：启动、重启脚本  
- conf：配置文件所在的目录  
- examples：SQL示例  
- lib：库目录  
- pl：过程语言库及脚本  
- PLClient：交互式客户端  
- protobuf：运行时protobuf临时目录

qe/build/dist目录用来打包部署生产环境。

编译后dist的代码命名为QE_HOME环境变量，后本QE_HOME都指向这个目录：  

    export QE_HOME=/home/allison/qe/build/dist  

# 单机环境搭建 #
这个章节，我们快速搭建一个单机版的TDW QE环境，并运行简单的SQL进行体验。这个环境中，Hive，元数据，Hadoop都在一台机器上，并且HDFS和MapReduce都是本地运行模式，因此不宜处理较大数据量。

TDW QE安装包中自带了一个hadoop-2.2.0的包在qe/hadoopcore文件夹中，在单机环境中，我们使用这个hadoop在单机环境中运行SQL和MR。解压hadoop-2.2.0的压缩包，然后设置HADOOP_HOME指向这个目录。

export HADOOP_HOME=/home/allison/qe/hadoopcore/hadoop-2.2.0

## 默认元数据配置 ##
首先在本机上要有PostgreSQL服务，使它监听127.0.0.1的5432端口（默认安装和初始化的PostgreSQL即监听127.0.0.1的5432端口），然后使用PG的管理员身份(一般是初始化PG数据库的linux账号,这里是postgres账户)，运行qe/script/tdw_meta_init.sql脚本，初始化元数据：

    psql -h 127.0.0.1 -p 5432 -U postgres postgres -f qe/script/tdw_meta_init.sql
如果之前初始化过，可以使用PG管理员身份运行qe/script/reset_meta.sql脚本重置元数据，再运行上面的命令重新初始化元数据：

	psql -h 127.0.0.1 -p 5432 -U postgres postgres -f qe/script/reset_meta.sql
	psql -h 127.0.0.1 -p 5432 -U postgres postgres -f qe/script/tdw_meta_init.sql

## 启动Hive命令行（CLI模式） ##
CLI模式区别于hvie server模式，在客户端进行SQL的解析和MR任务的提交。在开发和调试时，使用CLI模式比较方便。

启动hive的命令行：
    cd $QE_HOME/bin && ./hive -u root -p tdwroot
进入如下命令行界面(TDW QE默认初始化账号root以及它的密码为tdwroot)：
> allison@tdw:~/qe/bin$ ./hive -u root -p tdwroot  
> session : allison_201404021650_0.4582392182039239 start!  
> Connect to TDW successfully!  
> hive>

然后就可以运行TDW SQL：

    hive> create table tdw_table(key int,value string);
    OK
    Time taken: 0.574 seconds
    hive> show create table tdw_table;
    OK
    CREATE TABLE tdw_table(
    key INT,
    value STRING
    );
    Time taken: 0.113 seconds
    hive> insert into tdw_table values(1,'11'),(2,'22'),(3,'haha');
	 --...省略日志信息
    hive> select * from tdw_table; 
    OK
    1   11
    2   22
    3   haha
    Time taken: 0.019 seconds

## 启动Hive Server模式 ##
Hive Server模式是生产环境推荐的使用模式。它的启动方式是： 
 
    cd $QE_HOME/bin && ./start-server.sh  
这个命令将hive server默认绑定在50000端口。如果希望指定端口，可以将端口号作为第一个参数传入，如：
  
    cd $QE_HOME/bin && ./start-server.sh 50001

然后可以通过PLClient进行连接。PLClient的使用方式，请参考下文。

# Hadoop分布式环境配置 #

分布式环境与单机环境的最大区别是Hadoop使用local模式，还是分布式模式。在Hadoop生产环境中，TDW QE作为Hadoop的客户端，提交MR任务。因此，需要在TDW QE的机器上部署Hadoop客户端。

然后进行以下初始化：  
以hadoop管理员运行以下命令(这里假设TDW QE将以tdwadmin这个hadoop用户提交mr job和做hdfs操作)：

    hadoop fs -mkdir /tdwqe/warehouse
    hadoop fs -chown -R tdwadmin:tdwadmin /tdwqe/warehouse
    hadoop fs -chmod -R 777 /tmp 

修改TDW QE配置，将$QE_HOME/conf/hive-default.xml中的配置进行修改：
将以下配置项删除：

    hadoop.tmp.dir
    fs.default.name
    mapred.job.tracker
将以下配置项设置为冒号后的值：

    hive.exec.scratchdir:/tmp/tdw-${user.name}
    hive.metastore.warehouse.dir:/tdwqe/warehouse

将HADOOP_HOME环境变量指向Hadoop客户端，如：

    export HADOOP_HOME=/home/allison/hadoop-0.20.1

然后启动TDW QE：

    cd $QE_HOME/bin && ./start-server.sh

然后就可以通过PLClient连接使用。

# 配置元数据分散化 #
TDW的元数据支持分散化。元数据database由一个globa database和若干个segment database组成。在使用元数据时，TDW QE对表的qe db名计算一个hash值，将hash值映射在0~9999范围内，通过global元数据database中的一张路由配置信息，确定qe db内的表存储在哪个segment元数据database中。

TDW QE是通过global database中的seg_split表中的内容，选择将元数据放到对应的segment database中的。seg_split的定义是（可以在qe/script/tdw_meta_init.sql中找到元数据的定义信息）：

    CREATE TABLE seg_split (
    	seg_addr character varying NOT NULL,
    	seg_accept_range int4range
    );
默认TDW QE的配置如下：

    insert into seg_split values('jdbc:postgresql://127.0.0.1:5432/seg_1','[0,10000)');

表示将所有qe db hash值为0~9999的表，都放在seg_1这个元数据database中，seg_1的连接地址是jdbc:postgresql://127.0.0.1:5432/seg_1。

如果要将元数据分散在两个segment database中，这两个segment database位于不同的主机上，则可以将上面的insert语句改为：

    insert into seg_split values('jdbc:postgresql://192.168.1.2:5432/seg_1','[0,4999)');
    insert into seg_split values('jdbc:postgresql://192.168.1.3:5432/seg_2','[5000,10000)');

除了修改global database中的路由配置信息，还要初始化对应的seg_1和seg_2数据库，seg_1和seg_2两个database内的表结构都是一样的，如果他们在不同的PostgreSQL实例中，database名甚至可以相同（如都叫seg_1,但是不推荐）。可以通过pg_dump -s命令获得一个segment database的schema信息，用这个信息创建其它segment。

# 配置多HDFS #
如果想使指定的tdw database数据存储在指定的hdfs中，可以在TDW QE中执行一下操作：
  
    create database abc with(hdfsschema = 'hdfs://tdw-hdfs.tencent-distribute.com:5353');  

之后，这个db中的所有表的数据，都会存放在hdfsschema指定的hdfs集群中。
如果create database不带with参数，创建的db的存放hdfs是TDW QE运行的机器上hadoop hdfs默认的配置。

# 配置pgdata存储引擎 #
pgdata存储引擎是将PostgreSQL作为TDW的一个存储引擎，用户在TDW中建表时，如果指定stored as pgdata，那么这张表就会在配置项hive.pgdata.storage.url所指定的PostgreSQL实例中创建，同时在TDW中可见。在PG端，建表所用的用户名和密码，与TDW端的用户名和密码一致，因此在执行建表前，要确保该用户在PG侧账户和密码的存在和正确性。

用户也可以自己指定一个PG实例，以覆盖TDW QE默认的设置。如：

    set hive.pgdata.storage.url = jdbc:postgresql://192.168.1.3:5432/mypgdata;
    create table test(key int,value string) stored as pgdata;

pgdata存储引擎的表，在TDW端和PG端都可以操作，数据值保存在PG端。相对于普通的TDW表，pgdata存储引擎表可以有索引，可以快速update、delete，可以使用PG的jdbc、odbc、php等接口进行操作等。而且，对于pgdata存储引擎表，TDW会尽量将过滤条件下推到PG侧，使它在TDW中的访问效率更高。

# PLClient使用 #
PLClient提供交互式的查询接口，一下为登陆和运行SQL的演示：

> allison@tdw$ **$QE_HOME/bin/plclient/tdwpl**  
> Python version is OK.  
> No arguments, we fall back to shell mode now ...  
> Entering shell mode now ...  
> Got config from cmd line: mode m connect to None:None@None:None  
> Username: **root**  
> Password: **tdwroot**  
> Use 'connect/c' to connect the server!  
> TDW/PL Client Version 0.2.3  
> Auto save config: False  
> Auto save passwd: False  
> User home library: '~/.tdwpl'  
> User: 'root'  
> Database name 'default_db'  
> Server 'None:None' not connected  
> Welcome to TDW PL Shell, for help please input ? or help  
> root@TDW-PL$ **server 127.0.0.1**  
> You have changed server to '127.0.0.1'  
> root@TDW-PL$ **port 50000**  
> You have changed port to '50000'  
> root@TDW-PL$ **c**  
> connect to hive ip:127.0.0.1  
> Connect to server '127.0.0.1:50000' success.  
> root@TDW-PL$ **new**  
> New Session 6957574802551219 -1485586152  
> root@TDW-PL$ **create table allison_test(key int);**  
> TDW-00000 SUCCEED ( session: 6957574802551219 query: create table allison_test(key int) )  
> root@TDW-PL$ **desc allison_test**  
> key     int  
> TDW-00000 SUCCEED ( session: 6957574802551219 query: desc allison_test )  
> root@TDW-PL$ **help**  
> <pre>
> Documented commands (type help <topic>):  
> ========================================  
> EOF            color       ed            kill     put           sleep    
> a              compile     edit          ls       q             start    
> attach         config      editor        lsl      quit          status  
> autoed         connect     exec          lsm      reset         test    
> autosaveconf   d           exit          lsu      run           upload  
> c              dbname      get           makejar  save          uploadm 
> cat            detach      gethistory    n        savepasswd    user    
> ci             discard     getqp         new      server      
> cl             disconnect  getqueryplan  passwd   serverstatus
> clusterstatus  doc         getschema     plc      sethome     
> co             downloadm   host          port     shows       
</pre>

# 常用配置说明 #

TDW QE的配置文件在$QE_HOME/conf目录中，默认有两个,hive-default.xml和hive-log4j.properties,前者是TDW QE的主要配置，后者配置日志行为。hive-default.xml中的配置可以包含Hadoop的配置项，如果包含，会覆盖$HADOOP_HOME/conf中的相关配置。

- hive.default.fileformat  
指定新建表的默认存储格式，当create table没有stored as子句时，将使用改配置指定的存储格式。可选为sequencefile，textfile，rcfile，formatfile.默认为textfile，推荐使用rcfile。

- hive.default.formatcompress  
使用formatfile为默认存储引擎是是否启用LZO压缩，为true时启用LZO压缩，false则不启用。默认为true。


- hive.service.newlogpath  
hive server session log存放路径，每个session会产生一个文件。默认为/tmp/newlog目录。注意要定期清理里面的历史session日志。


- fetch.execinfo.mode  
PLClient在执行完每条SQL时，打印执行过程的统计信息。可选的为no，part，all，打印信息依次增加。默认为part。

- hive.query.info.log.url
- hive.query.info.log.user
- hive.query.info.log.passwd
TDW QE在Hive Server模式下执行信息上报配置。分别配置为PG的jdbc连接url，连接用户名和密码。url格式如：jdbc:postgresql://192.168.1.3:5432/query_info。在源码的script中tdw_meta_init.sql中的tdw数据库下的tdw schema可以查看执行信息上报的表结构。注意要定期清理这些表的历史数据。

- hive.metastore.global.url
- hive.metastore.pbjar.url
- hive.metastore.user
- hive.metastore.passwd
hive.metastore.global.url指定元数据global database的jdbc url，格式如：jdbc:postgresql://192.168.1.3:5432/global。hive.metastore.pbjar.url指定protobuf jar包等信息的database usr，格式如：jdbc:postgresql://192.168.1.4:5432/pbjar。
hive.metastore.user和hive.metastore.passwd设置hive.metastore.global.url、hive.metastore.pbjar.url所指定的PostgreSQL元数据库连接的用户名和密码。这个用户名密码，也是TDW QE连接所有segment database的用户名和密码。

- hive.metastore.warehouse.dir
TDW QE database数据在HDFS上的存放目录。如：/tdwqe/warehouse。

- hive.exec.scratchdir
TDW QE SQL执行过程中的中间临时数据在HDFS上的存放目录。如/tmp/tdw-${user.name}。

- hive.pgdata.storage.url
默认的pgdata存储引擎的的PG实例地址，格式如jdbc:postgresql://192.168.1.4:5432/pbdata。

- hive.pb.badfile.skip
- hive.pb.badfile.limit
hive.pb.badfile.skip设置是否开启SQL处理的Protobuf文件算坏时，跳过这个文件，而不是立即报错。true表示开启，false表示不开启。默认不开启。hive.pb.badfile.limit设置如果开启跳过PB损坏文件，当损坏文件超过多少时，SQL执行失败。默认是10。

- hive.max.sql.length
设置TDW QE接受的SQL的最大长度，到超过这个长度时，SQL将报错。这个值大于100时生效。默认是0。

- hive.session.timeout
设置session多长时间没有活动，Hive Server将其断掉。该值在[0,7200]范围内时，被设置为7200s，否则被设置为60s。默认是0。

- hive.client.connection.limit
- hive.client.connection.limit.number
hive.client.connection.limit设置是否开启Hive Server最大连接数限制，true开启，false关闭。默认是true。hive.client.connection.limit.number设置开启Hive Server最大连接数设置时，最大连接数限制值。连接数大于或等于这个值时，Hive Server将决绝连接。默认是50。

- hive.inputfiles.splitbylinenum
- hive.inputfiles.line_num_per_split
设置是否开启按行split，以及按照多上行记录，产生一个split。hive.inputfiles.splitbylinenum设置是否开启，默认为false。hive.inputfiles.line_num_per_split设置单个split的行数。只对format和rcfile存储格式生效。

- hive.exec.parallel
- hive.exec.parallel.threads
设置是否开启单个SQL无依赖MR并行运行，以及并行的线程数。hive.exec.parallel为true时开启，默认为false。hive.exec.parallel.threads默认是8。

- hive.multi.rctmpfile.path
SQL在做JOIN等操作时，如果内存不够用，会将数据写入到MR task本地磁盘。这个配置指定多个路径，他们可以分散到多个磁盘，以提高IO效率。配置路径以逗号分隔，且必须是绝对路径。默认是/tmp/data{1..12}。

# ProtoBuf存储格式支持 #

TDW支持Protobuf存储格式，用户可以通过proto定义文件，创建TDW表。

下面介绍创建Protobuf表的步骤。

## 拷贝proto定义文件 ##

将proto文件拷贝到$QE_HOME/protobuf/upload/${UserName}/中，UserName为TDW建表用户名。如果一张表对应有多个proto文件，确保将所有的都拷贝到目标目录。

## 预处理proto文件 ##

运行以下命令：

    $QE_HOME/bin/makejar.sh pgurl user passwd dbname tablename username filename protoversion 
参数说明：

- pgurl，user，passwd：保存jar报的元数据库信息，应该分别与$QE_HOME/conf中的hive.metastore.pbjar.url，hive.metastore.user，hive.metastore.passwd一致。  
- dbname，tablename：要创建表在TDW中的db名和表名，注意表名要与主message名相同。 
- username：建表用户名
- filename：主proto文件名，这个文件及其包含的文件（如果有的话）应该已经拷贝到指定的目录中。
- protoversion：protobuf版本号，目前只支持2.3.0

## 执行建表语句  ##

    create table tablename partition by list(part_time) (partition default) stored as pb

tablename：要创建的表名，支持分区表。创建表的dbname，tablename要与预处理中的信息一致。

## 注意事项 ##
- proto文件名一定要是小写，并且不能包含空格等特殊字符;
- proto文件中用到import其他proto文件的，不要写路径，只指明文件名即可，例如import "text.proto";
- 主proto文件的message名字一定要与表名相同，根据proto文件生成jar包的时候会进行检查，不相同会报错
- 自定义的类型名和变量名不能相同（新版本支持区分大小写，即message A类型的变量名可以为a），否则生成jar包会失败
- 不能包含空的message，否则建表的时候会出错

# 回归测试 #
首先按照“默认元数据配置”章节所描述的方法，初始化元数据，然后执行以下操作：
  
    cd qe
    ant test
    等待...
    ant testreport  

用浏览器打开qe/build/test/junit-noframes.html可以查看junit测试用例运行情况。  
推荐在有4GB以上内存配置的机器上运行回归测试。
# 已知问题 #
1. formatfile存储格式单行记录不能超过32KB，rcfile存储格式默认单行记录不能超过4MB，文本存储格式没有这个限制。因此在使用前，请提前做好评估，选择合适的存储格式建表。

2. 目前tdw的用户名和密码在元数据库中使明文存储的，没有复杂读要求，并且在传输过程中也是明文传输；TDW默认的密码如tdwroot、元数据默认密码tdwmeta、tdw等请及时修改；hadoop的日志和job配置信息，日志可能带有敏感信息，需要控制访问权限。因此，请在安全可信的环境中（使用防火墙隔离或者物理隔离）使用。在TDW未来版本中，将对安全这块进行改进。

3. 在向一个分区表中insert数据时，如果insert的目标分区过大（如数据将insert到超过100个分区），在MR执行过程中可能产生OOM错误，这时请修改SQL逻辑，或者增大MR task的内存配置。

# FAQ #
1. TDW是什么？  
   TDW是腾讯分布式数据仓库（Tencent Distributed Data Warehouse）的简称，是腾讯基于Hive、Hadoop、PostgreSQL等开源软件构建的数据仓库。TDW目前将核心模块--TDW查询引擎（TDW QE）块以apache license 2.0协议开源，这个模块基于apache hive 0.4.1版本进行改造，可以配合社区Hadoop版本快速构建数据仓库系统。

2. TDW QE现在的应用情况是怎样的？  
   TDW正式运营已有4年多，目前腾讯内超过90%以上的大数据分析是通过TDW QE的SQL来做的。我们的生产环境有70多个QE（hive server模式）实例在运行，他们共用一套元数据，管理着30万+张表，数千万个分区，近百PB的HDFS数据，每天接受近900万条SQL命令，并发session峰值超过2000。

3. TDW QE相比官方apache hive有什么优势？  
   首先TDW QE经过腾讯大规模压力、稳定性、功能上的验证，更稳定；其次，我们有一些社区并不具备的特性：如元数据分散化，多HDFS支持，基于角色的权限管理，range/list分区等；最后我们的文档都是中文，对国内研发人员来说更友好。

4. TDW QE的SQL语法与与apache hive兼容吗？  
   我们基于apache hive 0.4.1版本，添加了大量SQL语法和函数，在添加过程中，我们尽量参考已有的SQL语法（如MySQL、PostgreSQL），但是很少参考社区hive 0.4.1后的版本的语法。因此TDW QE的SQL语法与apache hive有差异，不完全兼容。对于SQL语法，TDW QE的往往比apache hive的更标准（如cube和rollup的语法）。

5. 与TDW QE配合使用的hadoop版本有哪些？  
   在腾讯的TDW生产环境中，我们使用的hadoop版本基于hadoop-2.2.x，目前我们只测试了TDW QE配合hadoop-2.2.x使用，更新版本的hadoop版本我们没有验证。

# TDW相关链接 #
[腾讯TDW项目：开源的分布式数据仓库](http://code.csdn.net/news/2818988)  
[Hive在腾讯分布式数据仓库的实践](http://www.csdn.net/article/2012-11-19/2811864-tecent_ZhaoWei_interview)  
[TDW在Hadoop上的实践分享](http://www.csdn.net/article/a/2012-05-23/2805878)  
[大规模Hadoop集群实践：腾讯分布式数据仓库（TDW）](http://codecloud.net/hadoop-tdw-823.html)  