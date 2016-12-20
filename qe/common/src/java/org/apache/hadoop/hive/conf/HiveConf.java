/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.conf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.commons.lang.StringUtils;

public class HiveConf extends Configuration {

  protected String hiveJar;
  protected Properties origProp;
  protected String auxJars;
  private static final Log l4j = LogFactory.getLog(HiveConf.class);
  private static URL confVarURL = null;

  private static String hiveServerVersion = "TDWQEV2.0R022D001";

  public static String getHiveServerVersion() {
    return hiveServerVersion;
  }

  public final static HiveConf.ConfVars[] metaVars = {
      HiveConf.ConfVars.METASTOREWAREHOUSE,
      HiveConf.ConfVars.METASTOREURIS };

  public static enum ConfVars {
	  	DEBUG("hive.debug", true),
	  	SCRIPTWRAPPER("hive.exec.script.wrapper", null), 
	  	PLAN("hive.exec.plan", null), 
        SCRATCHDIR("hive.exec.scratchdir", "/tmp/"+ System.getProperty("user.name") + "/hive"), 
        SUBMITVIACHILD("hive.exec.submitviachild", false), 
        SCRIPTERRORLIMIT("hive.exec.script.maxerrsize", 100000), 
        COMPRESSRESULT("hive.exec.compress.output", false), 
        COMPRESSINTERMEDIATE("hive.exec.compress.intermediate", false), 
        BYTESPERREDUCER("hive.exec.reducers.bytes.per.reducer", (long) (500 * 1000 * 1000)), 
        BYTESPERINSERTREDUCER("hive.insert.reducers.bytes.per.reducer",(long) (10 * 1000 * 1000 * 1000)), 
        FILESPERINSERTREDUCER("hive.insert.reducers.files.per.reducer", 1), 
        MAXREDUCERS("hive.exec.reducers.max", 999), 
        PREEXECHOOKS("hive.exec.pre.hooks", ""),

        MAXLIMITCOUNT("hive.sortby.limit.maxcount", 1024),

        HIVEFETCHOUTPUTSERDE("hive.fetch.output.serde", ""),

        EXECPARALLEL("hive.exec.parallel", false), 
        EXECPARALLELNUM("hive.exec.parallel.threads", 8),

        HADOOPBIN("hadoop.bin.path", System.getenv("HADOOP_HOME") + "/bin/hadoop"), 
        HADOOPCONF("hadoop.config.dir", System.getenv("HADOOP_HOME") + "/conf"), 
        HADOOPFS("fs.default.name", "file:///"), 
        HADOOPMAPFILENAME("map.input.file",null),
        HADOOPJT("mapred.job.tracker", "local"), 
        HADOOPNUMREDUCERS("mapred.reduce.tasks", 1), 
        HADOOPJOBNAME("mapred.job.name", null),

        METASTOREWAREHOUSE("hive.metastore.warehouse.dir", ""), 
        METASTOREURIS("hive.metastore.uris", ""), 

        CLIIGNOREERRORS("hive.cli.errors.ignore", false),

        HIVESESSIONID("hive.session.id", ""),

        HIVEQUERYSTRING("hive.query.string", ""),

        HIVEQUERYID("hive.query.id", ""),

        HIVEPLANID("hive.query.planid", ""), 
        HIVEJOBNAMELENGTH("hive.jobname.length", 50),

        HIVEJAR("hive.jar.path", ""), 
        HIVEAUXJARS("hive.aux.jars.path", ""),

        HIVEADDEDFILES("hive.added.files.path", ""), 
        HIVEADDEDJARS("hive.added.jars.path", ""),

        HIVETABLENAME("hive.table.name", ""), 
        HIVEPARTITIONNAME("hive.partition.name", ""), 
        HIVESCRIPTAUTOPROGRESS("hive.script.auto.progress", false), 
        HIVEMAPREDMODE("hive.mapred.mode","nonstrict"),
        HIVEALIAS("hive.alias", ""), 
        HIVEMAPSIDEAGGREGATE("hive.map.aggr", "true"), 
        HIVEGROUPBYSKEW("hive.groupby.skewindata","false"), 
        HIVEOPTIMIZECUBEROLLUP("hive.optimize.cuberollup", "true"), 
        HIVECUBEROLLUPREDUCE("hive.cuberollup.reduce", "false"), 
        HIVECUBEROLLUPREDUCESKEW("hive.cuberollup.reduce.skew", "false"), 
        HIVECUBEROLLUPREDUCETHRESHOULD("hive.cuberollup.reduce.threshould", "1"), 
        HIVEJOINEMITINTERVAL("hive.join.emit.interval", 1000), 
        HIVEJOINCACHESIZE("hive.join.cache.size", 25000), 
        HIVEMAPJOINBUCKETCACHESIZE("hive.mapjoin.bucket.cache.size", 100), 
        HIVEMAPJOINROWSIZE("hive.mapjoin.size.key", 10000), 
        HIVEMAPJOINCACHEROWS("hive.mapjoin.cache.numrows", 25000), 
        HIVEGROUPBYMAPINTERVAL("hive.groupby.mapaggr.checkinterval", 100000), 
        HIVEMAPAGGRHASHMEMORY("hive.map.aggr.hash.percentmemory", (float) 0.5), 
        HIVEMAPAGGRHASHMINREDUCTION("hive.map.aggr.hash.min.reduction", (float) 0.5), 
        HIVEMAPAGGRHASHMEMORY_MAX("hive.map.aggr.hash.max.memory", 1l * 64 * 1024 * 1024),

        HIVEUDTFAUTOPROGRESS("hive.udtf.auto.progress", false),

        HIVEDEFAULTFILEFORMAT("hive.default.fileformat", "TextFile"), 
        HIVEDEFAULTFORMATCOMPRESS("hive.default.formatcompress", true),

        HIVEHISTORYFILELOC("hive.querylog.location", "/tmp/" + System.getProperty("user.name")),

        HIVESCRIPTSERDE("hive.script.serde","org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"), 
        HIVESCRIPTRECORDREADER("hive.script.recordreader","org.apache.hadoop.hive.ql.exec.TextRecordReader"),

        HIVEHADOOPMAXMEM("hive.mapred.local.mem", 0),

        HIVETESTMODE("hive.test.mode", false), 
        HIVETESTMODEPREFIX("hive.test.mode.prefix", "test_"), 
        HIVETESTMODESAMPLEFREQ("hive.test.mode.samplefreq", 32), 
        HIVETESTMODENOSAMPLE("hive.test.mode.nosamplelist", ""),

        HIVEMERGEMAPFILES("hive.merge.mapfiles", true), 
        HIVEMERGEMAPREDFILES("hive.merge.mapredfiles", false), 
        HIVEMERGEINPUTFILES("hive.merge.inputfiles", true), 
        HIVEMERGEMAPFILESSIZE("hive.merge.size.per.task", (long) (512l * 1024 * 1024)),

        HIVESENDHEARTBEAT("hive.heartbeat.interval", 1000),

        HIVEOPTCP("hive.optimize.cp", true), 
        HIVEOPTPPD("hive.optimize.ppd", true), 
        HIVEOPTPPR("hive.optimize.pruner", true), 
        HIVEOPTHASHMAPJOIN("hive.optimize.hashmapjoin", false),

        NEWLOGPATH("hive.service.newlogpath", "/tmp/newlog"), 
        USERGROUPNAME("tdw.ugi.groupname", ""), 
        LIMITOP("hive.ql.parse.limitop", 1000), 
        OPENSHOWTABLESIZE("hive.ql.showtblsize", true), 
        OPENSHOWDBSIZE("hive.ql.showdbsize", true), 
        JOBCLIENTNPERETRYTIME("hive.isSuccessful.retrytime", 5), 
        JOBCLIENTNPEWAITTIME("hive.isSuccessful.waittime", 5000), 
        OPENWITH("hive.ql.openwith", false), 
        OPENWITHOPTIMIZE("hive.ql.openwith.optimize", true),
        
        HIVE_QUERY_INFO_LOG_URL("hive.query.info.log.url",""),
        HIVE_QUERY_INFO_LOG_USER("hive.query.info.log.user",""),
        HIVE_QUERY_INFO_LOG_PASSWD("hive.query.info.log.passwd",""),
        
        //DO NOT USE BISTORExx,USE HIVE_QUERY_INFO_LOG_XX
        //these configurations will be removed in the futere
        BISTOREIP("hive.bi.ip","172.25.38.163"), 
        BISTOREPORT("hive.bi.port", 5432), 
        BISTOREDB("hive.bi.db", "xx"), 
        BISTOREUSER("hive.bi.user", "xx"), 
        BISTOREPWD("hive.bi.pwd", "xx"), 

        ALTERSCHEMAACTIVATEEXTTABLE("hive.alterschema.activate.exttable", false), 
        ALTERSCHEMAACTIVATENOTYPELIMIT("hive.alterschema.activate.notypelimit", false), 
        ALTERSCHEMAACTIVATEREPLACE("hive.alterschema.activate.replace", true), 
        ALTERSCHEMAACTIVATEFIRSTAFTER("hive.alterschema.activate.firstandafter", false), 
        ALTERSCHEMAACTIVATETXTTABLE("hive.alterschema.activate.texttable", false),

        MAXNUM_ROWDELETED_PRINTLOG_PERTASK("maxnum.rowdeleted.printlog.pertask",100),

        TOLERATE_DATAERROR_READEXT("tolerate.dataerror.readext", "delete"),

        HIVEOUTERJOINSUPPORTSFILTERS("hive.outerjoin.supports.filters", true),

        SHOW_JOB_FAIL_DEBUG_INFO("hive.exec.show.job.failure.debug.info", true),

        ANAFUNCTMPDIR("analysisbuffer.tmp.addr", "/data/tdwadmin/tdwenv/tdw_tmp"),
  
        //do not use PGURL,use TDW_PGDATA_STORAGE_URL
        PGURL("pg_url","jdbc:postgresql://10.136.130.94:5432/tdwdata"),
        HIVE_PGDATA_STORAGE_URL("hive.pgdata.storage.url",""),

        FORMATSTORAGELIMITFIX("hive.formatstorage.limit.fix", false), 
        FORMATSTORAGELIMITNUM("hive.formatstorage.limit.num", 100000000), 
        FORMATSTORAGELIMITMIN("hive.formatstorage.limit.min", 100000000), 
        FORMATSTORAGELIMITMAX("hive.formatstorage.limit.max", 2000000000),

        JOINNULLBUGFIX("hive.join.nullbug.fix", true),

        HADOOPMAPREDINPUTDIR("mapred.input.dir", null), 
        HIVESKEWJOIN("hive.optimize.skewjoin", false), 
        HIVESKEWJOINKEY("hive.skewjoin.key",500000), 
        HIVEINPUTFORMAT("hive.input.format", ""), 
        HIVERCTMPFILEPATH("hive.rctmpfile.path", "/tmp/"),

        HIVE_GET_MULTI_RCTMP_PATH("hive.rowcontainer.get.multi.rctmp.path", true),
        HIVEMULTIRCTMPPATH(
        "hive.multi.rctmpfile.path",
        "/tmp/data1/, /tmp/data2/, /tmp/data3/, "
            + "/tmp/data4/, /tmp/data5/, /tmp/data6/, /tmp/data7/, /tmp/data8/,"
            + "/tmp/data9/, /tmp/data10/, /tmp/data11/, /tmp/data12/"), 

        HIVEMAXSQLLENGTH("hive.max.sql.length", 0),
        HIVESESSIONTIMEOUT("hive.session.timeout", 0),
        HIVECLIENTCONNECTIONLIMIT("hive.client.connection.limit", true), 
        HIVECLIENTCONNECTIONSLIMITNUMBER("hive.client.connection.limit.number", 50),

        HIVERCTMPFILESPLITTHRESHOLD("hive.rctmpfile.split.threshold", 800000), 
        HIVERCTMPFILESPLITSIZE("hive.rctmpfile.split.size", 400000),

        HIVEINSERTPARTSUPPORT("hive.insert.part.support", false), 
        HIVEINSERTPARTMAPERRORLIMIT("hive.insert.part.error.limit", 500),
        
        HIVEMETASTOREMASTERURL("hive.metastore.global.url","jdbc:postgresql://10.136.130.102:5432/global_metastore_db"),  
        HIVEMETASTOREUSER("hive.metastore.user", "tdw"), 
        HIVEMETASTOREPASSWD("hive.metastore.passwd", "tdw"), 
        HIVEMETASTOREROUTERBUFFERENABLE("hive.metastore.router.buffer.enable", true), 
        HIVEMETASTOREROUTERBUFFERSYNCTIME("hive.metastore.router.buffer.sync.time", 10), 
        HIVEMETASTOREUSEDISTRIBUTETRANSACTION("hive.metadata.usedistributetransaction", false), 
        HIVEMETASTORECONNPOOLACTIVESIZE("hive.metadata.conn.pool.activesize", 10), 
        HIVEMETASTORECONNPOOLENABLE("hive.metadata.conn.pool.enable", true),

        HIVEMETASTOREPBJARURL("hive.metastore.pbjar.url","jdbc:postgresql://127.0.0.1:5432/pbjar"),

        HIVEBIROWNUMPERINSERT("hive.bi.rownum.per.insert", 10000),

        HIVEERRORDATAPROCESSLEVEL("hive.errordata.process.level", 0),

        HIVEUSEEXPLICITRCFILEHEADER("hive.exec.rcfile.use.explicit.header", true), 
        HIVEUSERCFILESYNCCACHE("hive.exec.rcfile.use.sync.cache", true), 
        HIVEDEFAULTRCFILECOMPRESS("hive.default.rcfilecompress", true),

        MULTIHDFSENABLE("multihdfs.function.enable", true),

        BIINSERTNEW("bi.insert.new", true), 
        BIINSERTTOLERENT("bi.insert.tolerent",false), 
        BIINSERTTOLERENTNUM("bi.insert.tolerent.num", 1000),
    
        HIVEPBBADFILESKIP("hive.pb.badfile.skip", false), 
        HIVEPBBADFILELIMIT("hive.pb.badfile.limit", 10),
    
        HIVESUPPORTEXTERNALPARTITION("hive.support.external.partition", false),
        HIVE_PG_TIMEOUT("hive.pg.timeout", 10),
    
        HIVEEXECESTDISTINCTBUCKETSIZE("hive.exec.estdistinct.bucketsize.log", 15),
        HIVEEXECESTDISTINCTBUFFSIZE("hive.exec.estdistinct.buffsize.log", 8),
        
        HIVE_DELETE_DIR_ERROR_THROW("hive.delete.dir.error.throw",true),
        HIVE_OPTIMIZER_LEVEL("hive.semantic.analyzer.optimizer.level",2),
        
        HIVECUBENULLFILTERENABLE("hive.cube.null.filter.enable", false),
        
        LHOTSE_IMPORT_EXPORT_FLAG("lz.etl.flag", "test"),
        LHOTSE_IMPORT_EXPORT_FLAG_SERVER("lz.etl.flag_server", "test"),
        HIVEPROTOBUFVERSION("hive.protobuf.version", "2.5.0"),
        HIVE_SELECT_STAR_CHECK_PARTITION_MAX("hive.selectstar.check.partition.maxnumber", 10),
	  	
        HIVE_ADJUST_RESOURCE_ENABLE("hive.adjust.resource.enbale", true),
        HIVE_MAP_CHILD_JAVA_OPTS("mapred.map.child.java.opts", "-Xmx2048M"),
        HIVE_REDUCE_CHILD_JAVA_OPTS("mapred.reduce.child.java.opts", "-Xmx2048M"),
        HIVE_ADJUST_MEMORY_VARIATION_INMB("hive.adjust.memory.variation.mb", 256),
        HIVE_SIMPLE_MAP_MEMORY_INMB("hive.simple.map.memory.mb", 1024),
        HIVE_SIMPLE_MAP_CONTAINER_MEMORY_INMB("hive.simple.map.container.memory.mb", 1280),
	  	
	  	HIVE_DIVIDE_ZERO_RETURN_NULL("hive.dividezero.returnnull",false),
	  	
	  	//can be no,part,all
	  	FETCH_EXEC_INFO_MODE("fetch.execinfo.mode","part"),
	  	
	  	HIVE_INPUTFILES_SPLIT_BY_LINE("hive.inputfiles.splitbylinenum",false),
	  	HIVE_INPUTFILES_LINE_NUM_PER_SPLIT("hive.inputfiles.line_num_per_split",1000000),
	  	
	  	HIVE_OPT_PPR_IN("hive.optimize.ppr.in.enable", false),
	  	HIVE_OPT_PPR_IN_RANGEPART_FUNC_SUPPORT("hive.optimize.ppr.in.rangepart.func.enable", false),
	  	HIVE_OPT_PPR_RANGEPART_NEW("hive.optimize.ppr.rangepart.new.enable", false),
	  	HIVE_EXEC_RANGEPART_CACHE_MAXSIZE("hive.exec.rangepart.cache.maxsize", 10000),
	  	HIVE_CREATE_EXTTABLE_DIR_IFNOTEXIST("hive.exttable.createdir.ifnotexist", true),
	  	HIVE_CREATE_EXTTABLE_LOCATION_AUTH("hive.exttable.location.auth", false),
	  	HIVE_OPT_PPR_RANGEPART_NEW_ADDFIRSTPART_ALWAYS("hive.optimize.ppr.rangepart.new.addfirstpart.always", false),
	  	HIVE_OPT_PPR_RANGEPART_NEW_CHECK_DATEFORMAT("hive.optimize.ppr.rangepart.new.check.dateformat", false),
	  	
	  	HIVE_UDTF_EXPLODE_CHANGE_ZERO_SIZE_2_NULL("hive.udtf.explode.change0size2null", false),
	  	HIVE_UDTF_EXPLODE_CHANGE_NULL_2_NULL("hive.udtf.explode.change_null2null", false);

    public final String varname;
    public final String defaultVal;
    public final int defaultIntVal;
    public final long defaultLongVal;
    public final float defaultFloatVal;
    public final Class<?> valClass;
    public final boolean defaultBoolVal;

    ConfVars(String varname, String defaultVal) {
      this.varname = varname;
      this.valClass = String.class;
      this.defaultVal = defaultVal;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, int defaultIntVal) {
      this.varname = varname;
      this.valClass = Integer.class;
      this.defaultVal = null;
      this.defaultIntVal = defaultIntVal;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, long defaultLongVal) {
      this.varname = varname;
      this.valClass = Long.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = defaultLongVal;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, float defaultFloatVal) {
      this.varname = varname;
      this.valClass = Float.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = defaultFloatVal;
      this.defaultBoolVal = false;
    }

    ConfVars(String varname, boolean defaultBoolVal) {
      this.varname = varname;
      this.valClass = Boolean.class;
      this.defaultVal = null;
      this.defaultIntVal = -1;
      this.defaultLongVal = -1;
      this.defaultFloatVal = -1;
      this.defaultBoolVal = defaultBoolVal;
    }

    public String toString() {
      return varname;
    }
  }

  public static int getIntVar(Configuration conf, ConfVars var) {
    ConfVars.values();
    assert (var.valClass == Integer.class);
    return conf.getInt(var.varname, var.defaultIntVal);
  }

  public int getIntVar(ConfVars var) {
    return getIntVar(this, var);
  }

  public static long getLongVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Long.class);
    return conf.getLong(var.varname, var.defaultLongVal);
  }

  public long getLongVar(ConfVars var) {
    return getLongVar(this, var);
  }

  public static float getFloatVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Float.class);
    return conf.getFloat(var.varname, var.defaultFloatVal);
  }

  public float getFloatVar(ConfVars var) {
    return getFloatVar(this, var);
  }

  public static boolean getBoolVar(Configuration conf, ConfVars var) {
    assert (var.valClass == Boolean.class);
    return conf.getBoolean(var.varname, var.defaultBoolVal);
  }

  public boolean getBoolVar(ConfVars var) {
    return getBoolVar(this, var);
  }

  public static String getVar(Configuration conf, ConfVars var) {
    return conf.get(var.varname, var.defaultVal);
  }

  public static void setVar(Configuration conf, ConfVars var, String val) {
    assert (var.valClass == String.class);
    conf.set(var.varname, val);
  }

  public String getVar(ConfVars var) {
    return getVar(this, var);
  }

  public void setVar(ConfVars var, String val) {
    setVar(this, var, val);
  }

  public void logVars(PrintStream ps) {
    for (ConfVars one : ConfVars.values()) {
      ps.println(one.varname + "="
          + ((get(one.varname) != null) ? get(one.varname) : ""));
    }
  }

  public HiveConf() {
    super();
  }

  public HiveConf(Class<?> cls) {
    super();
    initialize(cls);
  }

  public HiveConf(Configuration other, Class<?> cls) {
    super(other);
    initialize(cls);
  }

  private Properties getUnderlyingProps() {
    Iterator<Map.Entry<String, String>> iter = this.iterator();
    Properties p = new Properties();
    while (iter.hasNext()) {
      Map.Entry<String, String> e = iter.next();
      p.setProperty(e.getKey(), e.getValue());
    }
    return p;
  }

  private void initialize(Class<?> cls) {
    hiveJar = (new JobConf(cls)).getJar();

    origProp = getUnderlyingProps();

    URL hconfurl = getClassLoader().getResource("hive-default.xml");
    if (hconfurl == null) {
      l4j.debug("hive-default.xml not found.");
    } else {
      addResource(hconfurl);
    }
    URL hsiteurl = getClassLoader().getResource("hive-site.xml");
    if (hsiteurl == null) {
      l4j.debug("hive-site.xml not found.");
    } else {
      addResource(hsiteurl);
    }

    URL hadoopconfurl = getClassLoader().getResource("hadoop-default.xml");
    if (hadoopconfurl == null)
      hadoopconfurl = getClassLoader().getResource("hadoop-site.xml");
    if (hadoopconfurl != null) {
      String conffile = hadoopconfurl.getPath();
      this.setVar(ConfVars.HADOOPCONF,
          conffile.substring(0, conffile.lastIndexOf('/')));
    }

    applySystemProperties();

    if (hiveJar == null) {
      hiveJar = this.get(ConfVars.HIVEJAR.varname);
    }

    if (auxJars == null) {
      auxJars = this.get(ConfVars.HIVEAUXJARS.varname);
    }

  }

  private static synchronized URL getConfVarURL() {
    if (confVarURL == null) {
      try {
        Configuration conf = new Configuration();
        File confVarFile = File.createTempFile("hive-default-", ".xml");
        confVarFile.deleteOnExit();

        applyDefaultNonNullConfVars(conf);

        FileOutputStream fout = new FileOutputStream(confVarFile);
        conf.writeXml(fout);
        fout.close();
        confVarURL = confVarFile.toURI().toURL();
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to initialize default Hive configuration variables!", e);
      }
    }
    return confVarURL;
  }

  private static void applyDefaultNonNullConfVars(Configuration conf) {
    for (ConfVars var : ConfVars.values()) {
      if (var.defaultVal == null) {
        continue;
      }
      if (conf.get(var.varname) != null) {
        l4j.debug("Overriding Hadoop conf property " + var.varname + "='"
            + conf.get(var.varname) + "' with Hive default value '"
            + var.defaultVal + "'");
      }
      conf.set(var.varname, var.defaultVal);
    }
  }

  public void applySystemProperties() {
    for (ConfVars oneVar : ConfVars.values()) {
      if (System.getProperty(oneVar.varname) != null) {
        if (System.getProperty(oneVar.varname).length() > 0)
          this.set(oneVar.varname, System.getProperty(oneVar.varname));
      }
    }
  }

  public Properties getChangedProperties() {
    Properties ret = new Properties();
    Properties newProp = getUnderlyingProps();

    for (Object one : newProp.keySet()) {
      String oneProp = (String) one;
      String oldValue = origProp.getProperty(oneProp);
      if (!StringUtils.equals(oldValue, newProp.getProperty(oneProp))) {
        ret.setProperty(oneProp, newProp.getProperty(oneProp));
      }
    }
    return (ret);
  }

  public Properties getAllProperties() {
    return getUnderlyingProps();
  }

  public String getJar() {
    return hiveJar;
  }

  public String getAuxJars() {
    return auxJars;
  }

  public void setAuxJars(String auxJars) {
    this.auxJars = auxJars;
    setVar(this, ConfVars.HIVEAUXJARS, auxJars);
  }

  public String getUser() throws IOException {
    try {
      UserGroupInformation ugi = ShimLoader.getHadoopShims()
          .getUGIForConf(this);
      return ugi.getUserName();
    } catch (LoginException e) {
      throw (IOException) new IOException().initCause(e);
    }
  }

  public static String getColumnInternalName(int pos) {
    return "_col" + pos;
  }
}
