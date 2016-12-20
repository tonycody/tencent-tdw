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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.beans.*;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.PlanUtils.ExpressionTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import StorageEngineClient.MyTextInputFormat;

@SuppressWarnings("nls")
public class Utilities {

  public static String HADOOP_LOCAL_FS = "file:///";

  public static enum ReduceField {
    KEY, VALUE, ALIAS
  };

  private static volatile Map<String, mapredWork> mrWorkMap = Collections
      .synchronizedMap(new HashMap<String, mapredWork>());
  static final private Log LOG = LogFactory.getLog(Utilities.class.getName());

  public static void clearMapRedWork(Configuration job) {
    try {
      Path planPath = new Path(HiveConf.getVar(job, HiveConf.ConfVars.PLAN));
      FileSystem fs = FileSystem.get(job);
      if (fs.exists(planPath)) {
        try {
          fs.delete(planPath, true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
    } finally {

      String planPath = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
      String jobID = (new Path(planPath)).getName();

      if (jobID != null) {
        synchronized (mrWorkMap) {
          mrWorkMap.remove(jobID);
        }
      }
    }
  }

  public static mapredWork getMapRedWork(Configuration job) {
    mapredWork gWork = null;
    try {

      String planPath = HiveConf.getVar(job, HiveConf.ConfVars.PLAN);
      String jobID = (new Path(planPath)).getName();

      synchronized (mrWorkMap) {
        gWork = mrWorkMap.get(jobID);
        LOG.debug("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
        for (Entry<String, mapredWork> en : mrWorkMap.entrySet()) {
          LOG.debug("jobID: " + en.getKey());
          LOG.debug("mapredWork: " + en.getValue().toString());
        }
        LOG.debug("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
      }

      if (gWork == null) {
        //String jtConf = HiveConf.getVar(job, HiveConf.ConfVars.HADOOPJT);        
        String jtConf = ShimLoader.getHadoopShims().getJobLauncherRpcAddress(job);
        
        String path;
        if (jtConf.equals("local")) {
          path = new Path(planPath).toUri().getPath();
        } else {
          path = "HIVE_PLAN" + sanitizedJobId(job);
        }

        InputStream in = new FileInputStream(path);
        mapredWork ret = deserializeMapRedWork(in, job);
        gWork = ret;
        gWork.initialize();
        mrWorkMap.put(getJobName(job), gWork);
      }
      return (gWork);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String getJobName(Configuration job) {
    String s = HiveConf.getVar(job, HiveConf.ConfVars.HADOOPJOBNAME);
    if (s == null) {
      s = "JOB" + randGen.nextInt();
      HiveConf.setVar(job, HiveConf.ConfVars.HADOOPJOBNAME, s);
    }
    return s;
  }

  public static int sanitizedJobId(Configuration job) {
    String s = getJobName(job);
    return s.hashCode();
  }

  public static List<String> getFieldSchemaString(List<FieldSchema> fl) {
    if (fl == null) {
      return null;
    }

    ArrayList<String> ret = new ArrayList<String>();
    for (FieldSchema f : fl) {
      ret.add(f.getName() + " " + f.getType()
          + (f.getComment() != null ? (" " + f.getComment()) : ""));
    }
    return ret;
  }

  public static class EnumDelegate extends DefaultPersistenceDelegate {
    @Override
    protected Expression instantiate(Object oldInstance, Encoder out) {
      return new Expression(Enum.class, "valueOf", new Object[] {
          oldInstance.getClass(), ((Enum<?>) oldInstance).name() });
    }

    protected boolean mutatesTo(Object oldInstance, Object newInstance) {
      return oldInstance == newInstance;
    }
  }

  public static void setMapRedWork(Configuration job, mapredWork w) {
    try {
      String jobID = UUID.randomUUID().toString();
      FileSystem fs = FileSystem.get(job);
      Path planPath = new Path(HiveConf.getVar(job,
    		  HiveConf.ConfVars.SCRATCHDIR), jobID);

      FSDataOutputStream out = fs.create(planPath);
      serializeMapRedWork(w, out);

      // Path tmpPath=new Path("file:///tmp/hive-plan_" + jobID);

      HiveConf.setVar(job, HiveConf.ConfVars.PLAN, planPath.toString());

      if (!HiveConf.getVar(job, HiveConf.ConfVars.HADOOPJT).equals("local") ||
          !job.get("mapreduce.framework.name", "local").equals("local")) {
          LOG.info("jobid : " + ((JobConf)job).getJobName() + " setMapRedWord : planPath :" + planPath.toString());
      //if (!HiveConf.getVar(job, HiveConf.ConfVars.HADOOPJT).equals("local")) {

        DistributedCache.createSymlink(job);
        String uriWithLink = planPath.toUri().toString() + "#HIVE_PLAN"
            + sanitizedJobId(job);
        DistributedCache.addCacheFile(new URI(uriWithLink), job);

        short replication = (short) job.getInt("mapred.submit.replication", 10);
        fs.setReplication(planPath, replication);

      }

      w.initialize();

      mrWorkMap.put(jobID, w);

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static void serializeTasks(Task<? extends Serializable> t,
      OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
    e.setPersistenceDelegate(groupByDesc.Mode.class, new EnumDelegate());
    e.writeObject(t);
    e.close();
  }

  public static void serializeMapRedWork(mapredWork w, OutputStream out) {
    XMLEncoder e = new XMLEncoder(out);
    e.setPersistenceDelegate(ExpressionTypes.class, new EnumDelegate());
    e.setPersistenceDelegate(groupByDesc.Mode.class, new EnumDelegate());
    e.writeObject(w);
    e.close();
  }

  public static mapredWork deserializeMapRedWork(InputStream in,
      Configuration conf) {
    XMLDecoder d = new XMLDecoder(in, null, null, conf.getClassLoader());
    mapredWork ret = (mapredWork) d.readObject();
    d.close();
    return (ret);
  }

  public static class Tuple<T, V> {
    private T one;
    private V two;

    public Tuple(T one, V two) {
      this.one = one;
      this.two = two;
    }

    public T getOne() {
      return this.one;
    }

    public V getTwo() {
      return this.two;
    }
  }

  public static tableDesc defaultTd;
  static {
    defaultTd = PlanUtils.getDefaultTableDesc("" + Utilities.ctrlaCode);
  }

  public final static int newLineCode = 10;
  public final static int tabCode = 9;
  public final static int ctrlaCode = 1;

  public static String nullStringStorage = "\\N";
  public static String nullStringOutput = "NULL";

  public static Random randGen = new Random();

  public static String getTaskId(Configuration hconf) {
    String taskid = (hconf == null) ? null : hconf.get("mapred.task.id");

    if ((taskid == null) || taskid.equals("")) {
      taskid = ("" + randGen.nextInt());
    } else {
      taskid = taskid.replaceAll("task_[0-9]+_", "");
    }
    taskid += ("." + System.currentTimeMillis());
    return taskid;
  }

  public static HashMap makeMap(Object... olist) {
    HashMap ret = new HashMap();
    for (int i = 0; i < olist.length; i += 2) {
      ret.put(olist[i], olist[i + 1]);
    }
    return (ret);
  }

  public static Properties makeProperties(String... olist) {
    Properties ret = new Properties();
    for (int i = 0; i < olist.length; i += 2) {
      ret.setProperty(olist[i], olist[i + 1]);
    }
    return (ret);
  }

  public static ArrayList makeList(Object... olist) {
    ArrayList ret = new ArrayList();
    for (int i = 0; i < olist.length; i++) {
      ret.add(olist[i]);
    }
    return (ret);
  }

  public static class StreamPrinter extends Thread {
    InputStream is;
    String type;
    PrintStream os;

    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }

    public void run() {
      try {
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        if (type != null) {
          while ((line = br.readLine()) != null)
            os.println(type + ">" + line);
        } else {
          while ((line = br.readLine()) != null)
            os.println(line);
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }

  public static tableDesc getTableDesc(Table tbl) {
    return (new tableDesc(tbl.getDeserializer().getClass(),
        tbl.getInputFormatClass(), tbl.getOutputFormatClass(), tbl.getSchema()));
  }

  public static tableDesc getTableDescFetch(Table tbl) {
    Class<? extends InputFormat> inputclass = tbl.getInputFormatClass();
    if (inputclass == TextInputFormat.class) {
      inputclass = MyTextInputFormat.class;
    }
    return (new tableDesc(tbl.getDeserializer().getClass(), inputclass,
        tbl.getOutputFormatClass(), tbl.getSchema()));
  }

  public static tableDesc getTableDesc(String cols, String colTypes) {
    return (new tableDesc(LazySimpleSerDe.class, SequenceFileInputFormat.class,
        HiveSequenceFileOutputFormat.class, Utilities.makeProperties(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                + Utilities.ctrlaCode,
            org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS, cols,
            org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES, colTypes)));
  }

  public static partitionDesc getPartitionDesc(Partition part) {
    return (new partitionDesc(getTableDesc(part.getTable()), part.getSpec()));
  }

  public static void addMapWork(mapredWork mr, Table tbl, String alias,
      Operator<?> work) {
    mr.addMapWork(tbl.getDataLocation().getPath(), alias, work,
        new partitionDesc(getTableDesc(tbl), null));
  }

  private static String getOpTreeSkel_helper(Operator<?> op, String indent) {
    if (op == null)
      return "";

    StringBuffer sb = new StringBuffer();
    sb.append(indent);
    sb.append(op.toString());
    sb.append("\n");
    if (op.getChildOperators() != null)
      for (Object child : op.getChildOperators()) {
        sb.append(getOpTreeSkel_helper((Operator<?>) child, indent + "  "));
      }

    return sb.toString();
  }

  public static int getDefaultNotificationInterval(Configuration hconf) {
    int notificationInterval;
    Integer expInterval = Integer.decode(hconf
        .get("mapred.tasktracker.expiry.interval"));

    if (expInterval != null) {
      notificationInterval = expInterval.intValue() / 2;
    } else {
      notificationInterval = 5 * 60 * 1000;
    }
    return notificationInterval;
  }

  public static String getOpTreeSkel(Operator<?> op) {
    return getOpTreeSkel_helper(op, "");
  }

  private static boolean isWhitespace(int c) {
    if (c == -1) {
      return false;
    }
    return Character.isWhitespace((char) c);
  }

  public static boolean contentsEqual(InputStream is1, InputStream is2,
      boolean ignoreWhitespace) throws IOException {
    try {
      if ((is1 == is2) || (is1 == null && is2 == null))
        return true;

      if (is1 == null || is2 == null)
        return false;

      while (true) {
        int c1 = is1.read();
        while (ignoreWhitespace && isWhitespace(c1))
          c1 = is1.read();
        int c2 = is2.read();
        while (ignoreWhitespace && isWhitespace(c2))
          c2 = is2.read();
        if (c1 == -1 && c2 == -1)
          return true;
        if (c1 != c2)
          break;
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    return false;
  }

  public static String abbreviate(String str, int max) {
    str = str.trim();

    int len = str.length();
    int suffixlength = 20;

    if (len <= max)
      return str;

    suffixlength = Math.min(suffixlength, (max - 3) / 2);
    String rev = StringUtils.reverse(str);

    String suffix = WordUtils.abbreviate(rev, 0, suffixlength, "");
    suffix = StringUtils.reverse(suffix);

    String prefix = StringUtils.abbreviate(str, max - suffix.length());

    return prefix + suffix;
  }

  public final static String NSTR = "";

  public static enum streamStatus {
    EOF, TERMINATED
  }

  public static streamStatus readColumn(DataInput in, OutputStream out)
      throws IOException {

    while (true) {
      int b;
      try {
        b = (int) in.readByte();
      } catch (EOFException e) {
        return streamStatus.EOF;
      }

      if (b == Utilities.newLineCode) {
        return streamStatus.TERMINATED;
      }

      out.write(b);
    }
  }

  public static OutputStream createCompressedStream(JobConf jc, OutputStream out)
      throws IOException {
    boolean isCompressed = FileOutputFormat.getCompressOutput(jc);
    return createCompressedStream(jc, out, isCompressed);
  }

  public static OutputStream createCompressedStream(JobConf jc,
      OutputStream out, boolean isCompressed) throws IOException {
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat
          .getOutputCompressorClass(jc, DefaultCodec.class);
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(
          codecClass, jc);
      return codec.createOutputStream(out);
    } else {
      return (out);
    }
  }

  public static String getFileExtension(JobConf jc, boolean isCompressed) {
    if (!isCompressed) {
      return "";
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat
          .getOutputCompressorClass(jc, DefaultCodec.class);
      CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(
          codecClass, jc);
      return codec.getDefaultExtension();
    }
  }

  public static SequenceFile.Writer createSequenceWriter(JobConf jc,
      FileSystem fs, Path file, Class<?> keyClass, Class<?> valClass)
      throws IOException {
    boolean isCompressed = SequenceFileOutputFormat.getCompressOutput(jc);
    return createSequenceWriter(jc, fs, file, keyClass, valClass, isCompressed);
  }

  public static SequenceFile.Writer createSequenceWriter(JobConf jc,
      FileSystem fs, Path file, Class<?> keyClass, Class<?> valClass,
      boolean isCompressed) throws IOException {
    CompressionCodec codec = null;
    CompressionType compressionType = CompressionType.NONE;
    Class codecClass = null;
    if (isCompressed) {
      compressionType = SequenceFileOutputFormat.getOutputCompressionType(jc);
      codecClass = SequenceFileOutputFormat.getOutputCompressorClass(jc,
          DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
    }
    return (SequenceFile.createWriter(fs, jc, file, keyClass, valClass,
        compressionType, codec));

  }

  public static RCFile.Writer createRCFileWriter(JobConf jc, FileSystem fs,
      Path file, boolean isCompressed) throws IOException {
    CompressionCodec codec = null;
    Class<?> codecClass = null;
    if (isCompressed) {
      codecClass = FileOutputFormat.getOutputCompressorClass(jc,
          DefaultCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
    }
    return new RCFile.Writer(fs, jc, file, null, codec);
  }

  public static String realFile(String newFile, Configuration conf)
      throws IOException {
    Path path = new Path(newFile);
    URI pathURI = path.toUri();
    FileSystem fs;

    if (pathURI.getScheme() == null) {
      fs = FileSystem.getLocal(conf);
    } else {
      fs = path.getFileSystem(conf);
    }

    if (!fs.exists(path)) {
      return null;
    }

    try {
      fs.close();
    } catch (IOException e) {
    }
    ;

    String file = path.makeQualified(fs).toString();
    if (StringUtils.startsWith(file, "file:/")
        && !StringUtils.startsWith(file, "file:///")) {
      file = "file:///" + file.substring("file:/".length());
    }
    return file;
  }

  public static List<String> mergeUniqElems(List<String> src, List<String> dest) {
    if (dest == null)
      return src;
    if (src == null)
      return dest;
    int pos = 0;

    while (pos < dest.size()) {
      if (!src.contains(dest.get(pos)))
        src.add(dest.get(pos));
      pos++;
    }

    return src;
  }

  private static final String tmpPrefix = "_tmp.";

  public static Path toTempPath(Path orig) {
    if (orig.getName().indexOf(tmpPrefix) == 0)
      return orig;
    return new Path(orig.getParent(), tmpPrefix + orig.getName());
  }

  public static Path toTempPath(String orig) {
    return toTempPath(new Path(orig));
  }

  public static String getPartNameFromTempPath(String orig) {
    if(orig.startsWith(tmpPrefix) && orig.length() > tmpPrefix.length()){
      return orig.split(tmpPrefix)[1];
    }
    else{
      return null;
    }
    
   //return toTempPath(new Path(orig));
  }

  public static Path toFilePath(Path orig, int filenum) {
    return new Path(orig.getParent(), orig.getName() + "_file" + filenum);
  }

  public static boolean isTempPath(FileStatus file) {
    String name = file.getPath().getName();
    return (name.startsWith("_task") || name.startsWith(tmpPrefix));
  }

  static public void rename(FileSystem fs, Path src, Path dst)
      throws IOException, HiveException {
    if (!fs.rename(src, dst)) {
      throw new HiveException("Unable to move: " + src + " to: " + dst);
    }
  }

  static public void renameOrMoveFiles(FileSystem fs, Path src, Path dst)
      throws IOException, HiveException {
    if (!fs.exists(dst)) {
      if (!fs.rename(src, dst)) {
        throw new HiveException("Unable to move: " + src + " to: " + dst);
      }
    } else {
      FileStatus[] files = fs.listStatus(src);
      for (int i = 0; i < files.length; i++) {
        Path srcFilePath = files[i].getPath();
        String fileName = srcFilePath.getName();
        Path dstFilePath = new Path(dst, fileName);
        if (fs.exists(dstFilePath)) {
          int suffix = 0;
          do {
            suffix++;
            dstFilePath = new Path(dst, fileName + "_" + suffix);
          } while (fs.exists(dstFilePath));
        }
        if (!fs.rename(srcFilePath, dstFilePath)) {
          throw new HiveException("Unable to move: " + src + " to: " + dst);
        }
      }
    }
  }

  static Pattern fileNameTaskIdRegexrow = Pattern
      .compile("^.*_([0-9]*)_[0-9]+(\\..*)?$");

  static Pattern fileNameTaskIdRegexcolumn = Pattern
      .compile("^.*_([0-9]*)_[0-9,.]*_idx([0-9]*)[\\w]*(\\..*)?$");

  static Pattern fileNameTaskIdRegexformat = Pattern
      .compile("^.*_([0-9]*)_([0-9]+)\\..*_file([0-9]*)[\\w]*(\\..*)?$");

  public static String getTaskIdFromFilename(String filename) {
    Matcher mrow = fileNameTaskIdRegexrow.matcher(filename);
    Matcher mcolumn = fileNameTaskIdRegexcolumn.matcher(filename);
    Matcher mformat = fileNameTaskIdRegexformat.matcher(filename);

    if (!mrow.matches() && !mcolumn.matches() && !mformat.matches()) {
      LOG.warn("Unable to get task id from file name: " + filename
          + ". Using full filename as task id.");
      return filename;
    } else {

      String taskId;
      if (mformat.matches()) {
        taskId = "format#" + mformat.group(1) + "_" + mformat.group(2);
        LOG.debug("TaskId for " + filename + " = " + taskId);
        return taskId;
      }

      if (mcolumn.matches()) {
        taskId = mcolumn.group(1) + mcolumn.group(2);
        LOG.debug("TaskId for " + filename + " = " + taskId);
        return taskId;
      }

      if (mrow.matches()) {
        taskId = mrow.group(1);
        LOG.debug("TaskId for " + filename + " = " + taskId);
        return taskId;
      }
    }
    return filename;
  }

  public static void removeTempOrDuplicateFiles(FileSystem fs, Path path)
      throws IOException {
    if (path == null)
      return;

    FileStatus items[] = fs.listStatus(path);
    if (items == null)
      return;

    HashMap<String, FileStatus> taskIdToFile = new HashMap<String, FileStatus>();
    HashMap<String, ArrayList<FileStatus>> taskIdToFiles = new HashMap<String, ArrayList<FileStatus>>();
    ArrayList<FileStatus> files = null;
    for (FileStatus one : items) {
      if (isTempPath(one)) {
        if (!fs.delete(one.getPath(), true)) {
          throw new IOException("Unable to delete tmp file: " + one.getPath());
        }
      } else {
        String taskId = getTaskIdFromFilename(one.getPath().getName());

        if (taskId.startsWith("format")) {
          taskId = taskId.split("#")[1];
          files = taskIdToFiles.get(taskId);
          if (files == null) {
            files = new ArrayList<FileStatus>();
          }
          files.add(one);
          taskIdToFiles.put(taskId, files);
          continue;
        }

        FileStatus otherFile = taskIdToFile.get(taskId);
        if (otherFile == null) {
          taskIdToFile.put(taskId, one);
        } else {
          FileStatus toDelete = null;
          if (otherFile.getLen() >= one.getLen()) {
            toDelete = one;
          } else {
            toDelete = otherFile;
            taskIdToFile.put(taskId, one);
          }
          long len1 = toDelete.getLen();
          long len2 = taskIdToFile.get(taskId).getLen();
          if (!fs.delete(toDelete.getPath(), true)) {
            throw new IOException("Unable to delete duplicate file: "
                + toDelete.getPath() + ". Existing file: "
                + taskIdToFile.get(taskId).getPath());
          } else {
            LOG.warn("Duplicate taskid file removed: " + toDelete.getPath()
                + " with length " + len1 + ". Existing file: "
                + taskIdToFile.get(taskId).getPath() + " with length " + len2);
          }
        }
      }
    }

    HashMap<String, Integer> taskIdToTotalSize = new HashMap<String, Integer>();
    HashMap<String, String> taskIdToTaskMulti = new HashMap<String, String>();
    for (String taskMulti : taskIdToFiles.keySet()) {
      for (String taskOne : taskIdToFile.keySet()) {
        if (taskMulti.startsWith(taskOne)) {
          FileStatus toDelete = taskIdToFile.get(taskOne);

          if (!fs.delete(toDelete.getPath(), true)) {
            throw new IOException("Unable to delete duplicate file: "
                + toDelete.getPath() + ". Existing file: "
                + taskIdToFile.get(taskOne).getPath());
          } else {
            LOG.warn("Duplicate taskid file removed: " + toDelete.getPath()
                + ". Existing file: " + taskMulti);
          }
        }
      }

      int totalSize = 0;
      files = taskIdToFiles.get(taskMulti);
      for (FileStatus one : files) {
        totalSize += one.getLen();
      }

      String taskId = taskMulti.split("_")[0];
      Integer recordsize = taskIdToTotalSize.get(taskId);
      if (recordsize == null) {
        taskIdToTotalSize.put(taskId, new Integer(totalSize));
        taskIdToTaskMulti.put(taskId, taskMulti);
      } else {
        if (recordsize.intValue() < totalSize) {
          String recordTaskId = taskIdToTaskMulti.get(taskId);
          taskIdToTaskMulti.put(taskId, taskMulti);
          files = taskIdToFiles.get(recordTaskId);
        }

        for (FileStatus toDelete : files) {
          if (!fs.delete(toDelete.getPath(), true)) {
            throw new IOException("Unable to delete duplicate file: "
                + toDelete.getPath());
          } else {
            LOG.warn("Duplicate taskid file removed: " + toDelete.getPath()
                + ". Existing file: " + taskMulti);
          }
        }
      }
    }
  }

  public static String getNameMessage(Exception e) {
    return e.getClass().getName() + "(" + e.getMessage() + ")";
  }

  public static ClassLoader addToClassPath(ClassLoader cloader,
      String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>();

    for (URL onePath : curPath) {
      newPath.add(onePath);
    }
    curPath = newPath;

    for (String onestr : newPaths) {
      if (StringUtils.indexOf(onestr, "file://") == 0)
        onestr = StringUtils.substring(onestr, 7);

      URL oneurl = (new File(onestr)).toURL();
      if (!curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }

    return new URLClassLoader(curPath.toArray(new URL[0]), loader);
  }

  public static void removeFromClassPath(String[] pathsToRemove)
      throws Exception {
    URLClassLoader loader = (URLClassLoader) JavaUtils.getpbClassLoader();
    Set<URL> newPath = new HashSet<URL>(Arrays.asList(loader.getURLs()));

    for (String onestr : pathsToRemove) {
      if (StringUtils.indexOf(onestr, "file://") == 0)
        onestr = StringUtils.substring(onestr, 7);

      URL oneurl = (new File(onestr)).toURL();
      newPath.remove(oneurl);
    }

    loader = new URLClassLoader(newPath.toArray(new URL[0]));
    JavaUtils.setpbClassLoader(loader);
  }

  public static List<String> getColumnNames(Properties props) {
    List<String> names = new ArrayList<String>();
    String colNames = props.getProperty(Constants.LIST_COLUMNS);
    String[] cols = colNames.trim().split(",");
    if (cols != null) {
      for (String col : cols) {
        if (col != null && !col.trim().equals("")) {
          names.add(col);
        }
      }
    }
    return names;
  }

  public static List<String> getColumnTypes(Properties props) {
    List<String> types = new ArrayList<String>();
    String colNames = props.getProperty(Constants.LIST_COLUMN_TYPES);
    String[] cols = colNames.trim().split(",");
    if (cols != null) {
      for (String col : cols) {
        if (col != null && !col.trim().equals(""))
          types.add(col);
      }
    }
    return types;
  }
  
	public static String getRandomRctmpDir(Configuration conf) {
		String RCtmppath = "";
		//thers codes should be refactor
		if (HiveConf.getBoolVar(conf,
				HiveConf.ConfVars.HIVE_GET_MULTI_RCTMP_PATH)) {
			LOG.info("get rctmp path from multi path: ");
			String tmpFileArray = HiveConf.getVar(conf,
					HiveConf.ConfVars.HIVEMULTIRCTMPPATH);
			String tdwTmpRcPath[] = tmpFileArray.split(",");
			int pathNum = tdwTmpRcPath.length;
			if (pathNum == 0) {
				pathNum = 2;
			}
			int randNum = Utilities.randGen.nextInt();
			int index = Math.abs(randNum % pathNum);

			String dirName = tdwTmpRcPath[index].trim() + "rctmpfile/";
			LOG.info("dirName: " + dirName);

			if (index >= 0 && index < pathNum) {
				File file = new File(dirName);
				if (file.exists()) {
					RCtmppath = dirName;
					LOG.info("RCtmppath: " + RCtmppath);
				} else {
					LOG.info("rctmpfile path is not exist: " + dirName
							+ " create it!");
					if (file.mkdirs()) {
						RCtmppath = dirName;
					} else {
						LOG.info("mkdir error: " + dirName);
						RCtmppath = HiveConf.getVar(conf,
								HiveConf.ConfVars.HIVERCTMPFILEPATH);
					}
				}
			} else {
				RCtmppath = HiveConf.getVar(conf,
						HiveConf.ConfVars.HIVERCTMPFILEPATH);
			}
		} else {
			RCtmppath = HiveConf.getVar(conf,
					HiveConf.ConfVars.HIVERCTMPFILEPATH);
		}
		
		return RCtmppath;
	}
}
