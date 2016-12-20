/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package IndexService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

import FormatStorage1.IFormatDataFile;

public class IndexServer_Old extends Thread {
  public static final Log LOG = LogFactory.getLog(IndexServer_Old.class);

  public String TMPDIR = "/se/tmp/";
  private Configuration conf;
  private FileSystem fs;
  private HiveConf hiveConf;
  private Hive db;
  private Warehouse wh;

  class SingleStatus {

    private String datadir;
    private String database;
    private String table;
    private String indexname;
    private String field;
    private String location;
    private int type;
    private int status;

    String tabletype;

    public String getDatabase() {
      return database;
    }

    public String getTable() {
      return table;
    }

    public String getIndexname() {
      return indexname;
    }

    public String getField() {
      return field;
    }

    public String getLocation() {
      return location;
    }

    public int getType() {
      return type;
    }

    public int getStatus() {
      return status;
    }

    public void setDatabase(String database) {
      this.database = database;
    }

    public void setTable(String table) {
      this.table = table;
    }

    public void setIndexname(String indexname) {
      this.indexname = indexname;
    }

    public void setField(String field) {
      this.field = field;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public void setType(int type) {
      this.type = type;
    }

    public void setStatus(int status) {
      this.status = status;
    }

    public String getDatadir() {
      return datadir;
    }

    public void setDatadir(String datadir) {
      this.datadir = datadir;
    }

    public String gettablelocation() {
      return datadir + "/" + database + "/" + table + "/";
    }

    public String getTabletype() {
      return tabletype;
    }

    public void setTabletype(String tabletype) {
      this.tabletype = tabletype;
    }

  }

  class IndexMetaStatus {

    HashMap<String, HashMap<String, SingleStatus>> tablestatuss = new HashMap<String, HashMap<String, SingleStatus>>();
    HashMap<String, Integer> indextasknums = new HashMap<String, Integer>();

    public IndexMetaStatus() {
    }

    synchronized public ArrayList<String> gettablelocations() {
      ArrayList<String> tablelocations = new ArrayList<String>();
      for (String tablelocation : tablestatuss.keySet()) {
        tablelocations.add(tablelocation);
      }
      return tablelocations;
    }

    synchronized public ArrayList<String> getindexlocations() {
      ArrayList<String> indexlocations = new ArrayList<String>();
      for (HashMap<String, SingleStatus> indexs : tablestatuss.values()) {
        indexlocations.addAll(indexs.keySet());
      }
      return indexlocations;
    }

    synchronized public void updatestatus(ArrayList<SingleStatus> statuss) {
      for (SingleStatus status : statuss) {
        if (!tablestatuss.containsKey(status.gettablelocation())) {
          HashMap<String, SingleStatus> table = new HashMap<String, SingleStatus>();
          tablestatuss.put(status.gettablelocation(), table);
        }

        HashMap<String, SingleStatus> table = tablestatuss.get(status
            .gettablelocation());
        try {
          Table t = db.getTable(status.database, status.table);
          String tt = t.getProperty("type");
          if (tt == null) {
            tt = "unknown";
          }
          status.setTabletype(tt);
        } catch (HiveException e) {
          e.printStackTrace();
        }
        table.put(status.getIndexname(), status);
      }
    }

    synchronized void increasetasknum(String indexlocation) {
      if (indextasknums.containsKey(indexlocation)) {
        int num = indextasknums.get(indexlocation);
        indextasknums.put(indexlocation, num + 1);
      } else {
        indextasknums.put(indexlocation, 1);
      }
      Path indexpath = new Path(indexlocation);
      try {
        db.setIndexStatus(indexpath.getParent().getParent().getName(),
            indexpath.getParent().getName(), indexpath.getName(), 1);
      } catch (HiveException e) {
        e.printStackTrace();
      }
    }

    synchronized void decreasetasknum(String indexlocation) {
      if (indextasknums.containsKey(indexlocation)) {
        int num = indextasknums.get(indexlocation);
        if (num > 0)
          indextasknums.put(indexlocation, num - 1);
      }
      if (indextasknums.get(indexlocation) == 0) {
        Path indexpath = new Path(indexlocation);
        try {
          db.setIndexStatus(indexpath.getParent().getParent().getName(),
              indexpath.getParent().getName(), indexpath.getName(), 2);
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }
    }
  }

  class IndexTaskQueue {

    private class PartEntity {

      private ConcurrentLinkedQueue<IndexTask> tasks = new ConcurrentLinkedQueue<IndexTask>();

      synchronized public boolean havetask() {
        for (int i = 0; i < tasks.size(); i++) {
          IndexTask task = tasks.peek();
          if (task.status != 2)
            break;
          tasks.remove(task);
        }
        return tasks.size() > 0;
      }

      synchronized public IndexTask gettask() {
        if (havetask()) {
          IndexTask task = tasks.peek();
          if (task.status == 0) {
            task.status = 1;
            return task;
          }
        }
        return null;
      }

      synchronized public void puttask(IndexTask task) {
        tasks.add(task);
      }
    }

    HashMap<String, PartEntity> partEntities = new HashMap<String, PartEntity>();

    ConcurrentLinkedQueue<String> partids = new ConcurrentLinkedQueue<String>();

    private IndexMetaStatus indexMetaStatus;

    public IndexTaskQueue(IndexMetaStatus indexMetaStatus) {
      this.indexMetaStatus = indexMetaStatus;
    }

    synchronized public void addCreateTasks(String tablelocation,
        String indexname, SingleStatus ss) throws IOException {
      System.out.println("IndexTaskQueue:addCreateTasks:\t" + tablelocation
          + "\t" + indexname);
      FileStatus[] parts = fs.listStatus(new Path(tablelocation));
      if (parts == null || parts.length == 0)
        return;
      if (ss.getTabletype() == null) {

      }

      String tabletype = ss.getTabletype();
      if (tabletype.equals("unknown")) {
        System.out.println("tabletype is unknown, create task not build...");
        return;
      }
      boolean column = tabletype.equals("column") ? true : false;
      if (!parts[0].isDir()) {
        String partid = (ss.getLocation().endsWith("/") ? ss.getLocation()
            : (ss.getLocation() + "/")) + "nopart";
        if (!partEntities.containsKey(partid)) {
          partEntities.put(partid, new PartEntity());
        }

        CreateIndexTask task = new CreateIndexTask(indexMetaStatus,
            ss.getLocation(), tablelocation, partid, ss.getIndexname(),
            ss.getField(), column);
        partEntities.get(partid).puttask(task);
        if (!partids.contains(partid)) {
          partids.add(partid);
        }
        System.out.println("add a new create index task nopart");

      } else {
        for (FileStatus part : parts) {
          String partid = (ss.getLocation().endsWith("/") ? ss.getLocation()
              : (ss.getLocation() + "/")) + part.getPath().getName();
          if (!partEntities.containsKey(partid)) {
            partEntities.put(partid, new PartEntity());
          }
          CreateIndexTask task = new CreateIndexTask(indexMetaStatus,
              ss.getLocation(), part.getPath().toString(), partid,
              ss.getIndexname(), ss.getField(), column);
          partEntities.get(partid).puttask(task);
          if (!partids.contains(partid)) {
            partids.add(partid);
          }
          System.out.println("add a new create index task part:\t" + partid);
        }
      }
    }

    synchronized public void addUpdateTasks(String tablelocation,
        String partname) {
      Collection<SingleStatus> ss = indexMetaStatus.tablestatuss.get(
          tablelocation).values();
      for (SingleStatus status : ss) {
        System.out.println("++++++++++++++++++++++++++++add a new update task");
        String partid = (status.getLocation().endsWith("/") ? status
            .getLocation() : (status.getLocation() + "/")) + partname;
        partid = partid.endsWith("/") ? partid
            .substring(0, partid.length() - 1) : partid;
        String inputdir = (tablelocation.endsWith("/") ? tablelocation
            : (tablelocation + "/")) + partname;
        if (!partEntities.containsKey(partid)) {
          partEntities.put(partid, new PartEntity());
        }
        UpdateIndexTask task = new UpdateIndexTask(indexMetaStatus,
            status.getLocation(), inputdir, partid, status.getIndexname(),
            status.getField());
        partEntities.get(partid).puttask(task);
        if (!partids.contains(partid)) {
          partids.add(partid);
        }
      }
    }

    synchronized public void addMergeTask(String indexlocation, String partname) {
      System.out.println("+++++++++++++++++++a new merge task");
      String partid = (indexlocation.endsWith("/") ? indexlocation
          : (indexlocation + "/")) + partname;
      if (!partEntities.containsKey(partid)) {
        partEntities.put(partid, new PartEntity());
      }
      MergeIndexTask task = new MergeIndexTask(indexMetaStatus, indexlocation,
          partid, partid);
      partEntities.get(partid).puttask(task);
      if (!partids.contains(partid)) {
        partids.add(partid);
      }
    }

    synchronized public IndexTask getatask() {
      for (int i = 0; i < partids.size(); i++) {
        String partid = partids.peek();
        PartEntity pn = partEntities.get(partid);
        IndexTask task;
        if (pn.havetask() && (task = pn.gettask()) != null) {
          return task;
        } else if (pn.havetask()) {
          partid = partids.poll();
          partids.add(partid);
        } else {
          partids.remove(partid);
        }
      }
      return null;
    }

  }

  abstract class IndexTask extends Thread {
    boolean last = false;
    int status = 0;
    String indexlocation;
    IndexMetaStatus indexMetaStatus;

    public IndexTask(IndexMetaStatus indexMetaStatus, String indexlocation) {
      this.indexMetaStatus = indexMetaStatus;
      this.indexlocation = indexlocation;
      this.starttask();
    }

    private void starttask() {
      indexMetaStatus.increasetasknum(indexlocation);
    }

    protected void finishtask() {
      status = 2;
      indexMetaStatus.decreasetasknum(indexlocation);
    }

    protected void checkindexdir(Path indexdirpath) {

    }

    protected String getdatafiles(Path part, boolean column, String indexids) {
      try {
        StringBuffer sb = new StringBuffer();
        FileStatus[] part2s = fs.listStatus(part);
        for (FileStatus p2ss : part2s) {
          if (fs.isFile(p2ss.getPath())) {
            String filename = p2ss.getPath().makeQualified(fs).toString();
            if (!column || checkidx(filename, indexids)) {
              sb.append(filename + ",");
            }
          } else {
            FileStatus[] filess = fs.listStatus(p2ss.getPath());
            for (FileStatus fstts : filess) {
              String filename = fstts.getPath().makeQualified(fs).toString();
              if (!column || checkidx(filename, indexids)) {
                sb.append(filename + ",");
              }
            }
          }
        }
        return sb.substring(0, sb.length() - 1);
      } catch (IOException e) {
        e.printStackTrace();
        return new String();
      }
    }

    private boolean checkidx(String filename, String indexids) {
      if (!filename.contains("_idx"))
        return false;
      String[] idx1s = filename.substring(filename.indexOf("_idx") + 4).split(
          "_");
      String[] idx2s = indexids.split(",");
      for (String idx2 : idx2s) {
        for (String idx1 : idx1s) {
          if (idx2.trim().equals(idx1.trim()))
            return true;
        }
      }
      return false;
    }

    protected ArrayList<String> getcurrentdatafiles(Path datadir,
        boolean column, String indexids) {
      ArrayList<String> currentfiles = new ArrayList<String>();
      try {

        FileStatus[] fss = fs.listStatus(datadir);
        for (FileStatus status : fss) {
          if (fs.isFile(status.getPath())) {
            currentfiles.add(status.getPath().makeQualified(fs).toString());
          } else {
            FileStatus[] fss1 = fs.listStatus(status.getPath());
            for (FileStatus status2 : fss1) {
              String filename = status2.getPath().makeQualified(fs).toString();
              if (!column || filename.contains(indexids))
                currentfiles.add(filename);
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return currentfiles;
    }

    protected HashSet<String> getcurrentdatafilesfromindex(Path indexpart) {
      HashSet<String> files = new HashSet<String>();
      try {
        FileStatus[] fss = fs.listStatus(indexpart);
        for (FileStatus status : fss) {
          Path indexfile = status.getPath();
          if (indexfile.getName().startsWith("index")) {
            IFormatDataFile ifdf = new IFormatDataFile(conf);
            ifdf.open(indexfile.toString());
            Collection<String> datafiles = ifdf.fileInfo().head().getUdi()
                .infos().values();
            files.addAll(datafiles);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return files;
    }

    protected Path getnewindexfile(Path indexdir) {
      String file = "indexfile";
      int i = 0;
      try {
        if (!fs.exists(indexdir)) {
        } else {
          i = fs.listStatus(indexdir).length;
          while (fs.exists(new Path(indexdir, file + i))) {
            i++;
          }
        }
        return new Path(indexdir, file + i);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    protected String getindexfiles(Path part) {
      StringBuffer sb = new StringBuffer();
      try {
        FileStatus[] parts = fs.listStatus(part);
        for (FileStatus p : parts) {
          sb.append(p.getPath().makeQualified(fs).toString() + ",");
        }
        return sb.substring(0, sb.length() - 1);
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }

    protected Path gettmpoutputdir() {
      int x = (int) (Math.random() * Integer.MAX_VALUE);
      Path tmp = new Path(TMPDIR + x);
      try {
        while (fs.exists(tmp)) {
          x = (int) (Math.random() * Integer.MAX_VALUE);
          tmp = new Path(TMPDIR + x);
        }
        fs.mkdirs(tmp);
        return tmp;
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  class CreateIndexTask extends IndexTask {
    String inputdir;
    String outputdir;
    String indexids;
    boolean column;

    private CreateIndexTask(IndexMetaStatus indexMetaStatus,
        String indexlocation) {
      super(indexMetaStatus, indexlocation);
    }

    public CreateIndexTask(IndexMetaStatus indexMetaStatus,
        String indexlocation, String inputdir, String outputdir,
        String indexname, String indexids, boolean column) {
      this(indexMetaStatus, indexlocation);
      this.column = column;
      this.inputdir = inputdir;
      this.outputdir = outputdir;
      this.indexids = indexids;
    }

    @Override
    public void run() {
      try {
        Path tmpoutputdir = gettmpoutputdir();
        String inputfiles = getdatafiles(new Path(inputdir), column, indexids);

        System.out.println("start create index");
        IndexMR
            .run(conf, inputfiles, column, indexids, tmpoutputdir.toString());

        Path indexdirpath = new Path(outputdir);

        checkindexdir(indexdirpath);

        Path tmpoutputdir1 = gettmpoutputdir();
        fs.rename(indexdirpath, tmpoutputdir1);
        fs.mkdirs(indexdirpath);

        Path indexfile = new Path(indexdirpath, "indexfile0");
        fs.rename(new Path(tmpoutputdir, "part-00000"), indexfile);
        fs.delete(tmpoutputdir, true);
        fs.delete(tmpoutputdir1, true);
      } catch (IOException e) {
        e.printStackTrace();
      }
      LOG.info("a create task finished");
      finishtask();
    }
  }

  class UpdateIndexTask extends IndexTask {
    String inputdir;
    String outputdir;
    String indexids;
    boolean column;

    private UpdateIndexTask(IndexMetaStatus indexMetaStatus,
        String indexlocation) {
      super(indexMetaStatus, indexlocation);
    }

    public UpdateIndexTask(IndexMetaStatus indexMetaStatus,
        String indexlocation, String inputdir, String outputdir,
        String indexname, String indexids) {
      this(indexMetaStatus, indexlocation);
      column = indexMetaStatus.tablestatuss.get(inputdir).get(indexname).tabletype
          .equals("column") ? true : false;
      this.inputdir = inputdir;
      this.outputdir = outputdir;
      this.indexids = indexids;
    }

    @Override
    public void run() {
      try {
        HashSet<String> indexedfiles = getcurrentdatafilesfromindex(new Path(
            outputdir));
        ArrayList<String> currentdatafiles = getcurrentdatafiles(new Path(
            inputdir), column, indexids);

        StringBuffer inputfiles = new StringBuffer();
        for (String file : currentdatafiles) {
          if (!indexedfiles.contains(file)) {
            inputfiles.append(new Path(file).makeQualified(fs).toString())
                .append(",");
          }
        }

        Path tmpoutputdir = gettmpoutputdir();
        IndexMR.run(conf, inputfiles.substring(0, inputfiles.length() - 1),
            false, indexids, tmpoutputdir.toString());

        Path indexdirpath = new Path(outputdir);

        checkindexdir(indexdirpath);

        Path indexfile = getnewindexfile(indexdirpath);

        if (!fs.exists(indexfile.getParent())) {
          fs.mkdirs(indexfile.getParent());
        }
        fs.rename(new Path(tmpoutputdir, "part-00000"), indexfile);
        fs.delete(tmpoutputdir, true);

      } catch (IOException e) {
        e.printStackTrace();
      }
      finishtask();
      LOG.info("a update task finished");
    }
  }

  class MergeIndexTask extends IndexTask {

    String inputdir;
    String outputdir;

    public MergeIndexTask(IndexMetaStatus indexMetaStatus, String indexlocation) {
      super(indexMetaStatus, indexlocation);
    }

    public MergeIndexTask(IndexMetaStatus indexMetaStatus,
        String indexlocation, String inputdir, String outputdir) {
      this(indexMetaStatus, indexlocation);
      this.inputdir = inputdir;
      this.outputdir = outputdir;
    }

    @Override
    public void run() {
      try {
        Path indexpart = new Path(inputdir);
        String inputfiles = getindexfiles(indexpart);
        Path tmpoutputdir = gettmpoutputdir();

        IndexMergeMR.run(inputfiles, tmpoutputdir.toString(),
            new Configuration());

        Path tmpoutputdir1 = gettmpoutputdir();
        fs.rename(indexpart, tmpoutputdir1);

        fs.delete(indexpart, true);
        fs.mkdirs(indexpart);
        fs.rename(new Path(tmpoutputdir, "part-00000"), new Path(indexpart,
            "indexfile0"));
        fs.delete(tmpoutputdir, true);
        fs.delete(tmpoutputdir1, true);

      } catch (IOException e) {
        e.printStackTrace();
      }
      finishtask();
      LOG.info("a merge task finished");
    }
  }

  IndexMetaStatus indexMetaStatus = new IndexMetaStatus();
  IndexTaskQueue indexTaskQueue = new IndexTaskQueue(indexMetaStatus);

  MetaDataChecker metaDataChecker = new MetaDataChecker();
  DataDirChecker dataDirChecker = new DataDirChecker();
  IndexDirChecker indexDirChecker = new IndexDirChecker();

  private AtomicBoolean run = new AtomicBoolean(true);
  ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(3, 6, 1,
      TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());

  public IndexServer_Old() {
    try {
      conf = new Configuration();
      conf.addResource("hive-default.xml");
      hiveConf = new HiveConf(conf, IndexServer_Old.class);
      fs = FileSystem.get(conf);
      db = Hive.get(hiveConf);
      wh = new Warehouse(hiveConf);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (HiveException e) {
      e.printStackTrace();
    } catch (MetaException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    metaDataChecker.start();
    dataDirChecker.start();
    indexDirChecker.start();
  }

  @Override
  public void run() {
    while (run.get()) {
      try {
        synchronized (indexTaskQueue) {
          indexTaskQueue.wait(5000);
        }
      } catch (InterruptedException e) {
      }
      IndexTask task = indexTaskQueue.getatask();
      if (task == null)
        continue;
      System.out.println("+++++++++++++++++++++get a task and run\t"
          + task.indexlocation);
      poolExecutor.execute(task);
    }
  }

  public void close() {
    run.set(false);
    this.interrupt();
    System.exit(0);
  }

  public static void main(String[] args) throws IOException, HiveException {
    IndexServer_Old server = new IndexServer_Old();
    server.start();
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String str;
    while (true) {
      try {
        str = br.readLine();
        String[] strs = str.split(" ");
        if (strs[0].equals("setindexstatus")) {
          Path indexpath = new Path(strs[1]);
          server.db.setIndexStatus(indexpath.getParent().getParent().getName(),
              indexpath.getParent().getName(), indexpath.getName(),
              Integer.valueOf(strs[2]));
        } else if (str.equals("exit")) {
          server.close();
          System.exit(0);
          break;
        }
      } catch (NumberFormatException e) {
        e.printStackTrace();
      } catch (HiveException e) {
        e.printStackTrace();
      }
    }
  }

  class MetaDataChecker extends Thread {

    public MetaDataChecker() {
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      while (true) {
        if (System.currentTimeMillis() - time > 20000) {
          System.err.println("MetaDataChecker is running....");
          time = System.currentTimeMillis();
        }
        ArrayList<SingleStatus> singleStatuss = getmetastatus();
        if (singleStatuss != null) {
          indexMetaStatus.updatestatus(singleStatuss);
          for (SingleStatus status : singleStatuss) {
            if (status.getStatus() == 0) {
              try {
                synchronized (indexTaskQueue) {
                  indexTaskQueue.addCreateTasks(status.gettablelocation(),
                      status.getIndexname(), status);
                  indexTaskQueue.notifyAll();
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
            }
          }
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    private ArrayList<SingleStatus> getmetastatus() {
      ArrayList<SingleStatus> statuss = new ArrayList<SingleStatus>();
      try {
        List<IndexItem> indexInfo = db.getAllIndexSys();
        for (IndexItem indexItem : indexInfo) {
          SingleStatus status = new SingleStatus();
          status.setDatabase(indexItem.getDb());
          status.setTable(indexItem.getTbl());
          status.setIndexname(indexItem.getName());
          status.setLocation(indexItem.getLocation());
          status.setStatus(indexItem.getStatus());
          status.setType(indexItem.getType());
          String datadir = wh.getDefaultDatabasePath(indexItem.getDb())
              .getParent().toString();
          datadir = datadir.startsWith("file:") ? datadir.substring(5)
              : datadir;
          status.setDatadir(new Path(datadir).makeQualified(fs).toString());
          status.setField(indexItem.getFieldList());
          statuss.add(status);

        }

      } catch (HiveException e) {
        e.printStackTrace();
      } catch (MetaException e) {
        e.printStackTrace();
      }
      return statuss;
    }
  }

  class DataDirChecker extends Thread {

    public DataDirChecker() {
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      while (true) {
        if (System.currentTimeMillis() - time > 20000) {
          System.err.println("DataDirChecker is running....");
          time = System.currentTimeMillis();
        }
        ArrayList<String> tablelocations = indexMetaStatus.gettablelocations();
        for (String location : tablelocations) {
          try {
            checktablelocation(location);
            Thread.sleep(10000);
          } catch (InterruptedException e) {
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }

    private void checktablelocation(String location) throws IOException {
      Path tablepath = new Path(location);
      FileStatus[] partslocations = fs.listStatus(tablepath);
      if (partslocations == null || partslocations.length <= 0)
        return;
      if (!partslocations[0].isDir()) {
        for (FileStatus file : partslocations) {
          if (file.getPath().getName().equals("ok")) {
            synchronized (indexTaskQueue) {
              fs.rename(file.getPath(), new Path(file.getPath().getParent(),
                  "creating"));
              indexTaskQueue.addUpdateTasks(tablepath.toString(), null);
            }
          }
        }
      } else {
        for (FileStatus partss : partslocations) {
          FileStatus[] files = fs.listStatus(partss.getPath());
          if (files != null && files.length > 0) {
            for (FileStatus file : files) {
              if (file.getPath().getName().equals("ok")) {
                synchronized (indexTaskQueue) {
                  fs.rename(file.getPath(), new Path(
                      file.getPath().getParent(), "creating"));
                  indexTaskQueue.addUpdateTasks(tablepath.toString(), partss
                      .getPath().getName());
                }
              }
            }
          }
        }
      }
    }
  }

  class IndexDirChecker extends Thread {

    public IndexDirChecker() {
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      while (true) {
        if (System.currentTimeMillis() - time > 20000) {
          System.err.println("IndexDirChecker is running....");
          time = System.currentTimeMillis();
        }
        ArrayList<String> indexlocations = indexMetaStatus.getindexlocations();
        for (String indexlocation : indexlocations) {
          try {
            checkmergetask(indexlocation);
            Thread.sleep(10000);
          } catch (InterruptedException e) {
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
    }

    private void checkmergetask(String indexlocation) throws IOException {
      FileStatus[] statuss = fs.listStatus(new Path(indexlocation));
      if (statuss != null && statuss.length > 0) {
        for (FileStatus status : statuss) {
          FileStatus[] ss = fs.listStatus(status.getPath());
          if (ss != null && ss.length > 10) {
            synchronized (indexTaskQueue) {
              indexTaskQueue.addMergeTask(indexlocation, status.getPath()
                  .getName());
            }
          }
        }
      }
    }
  }
}
