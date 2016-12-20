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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hadoop.mapred.RunningJob;

import FormatStorage1.IFormatDataFile;

public class IndexServer extends Thread {

  public static final Log LOG = LogFactory.getLog(IndexServer.class);

  public boolean testmode = false;

  public final int MDC_check_interval;
  public final int DPM_check_interval;
  public final int TQM_check_interval;
  public final int IPM_check_merge_interval;
  public final int mergeindexfilelimit;
  public int Indexer_max_jobnum;

  Random r = new Random();

  public String TMPDIR = "/user/tdw/tmp/";
  private Configuration conf;
  private FileSystem fs;
  private HiveConf hiveConf;
  private Hive db;
  private Warehouse wh;

  public IndexServer(Configuration conf2) {
    initialize(conf2);
    MDC_check_interval = conf.getInt("se.indexer.MDC_interval", 10000);
    DPM_check_interval = conf.getInt("se.indexer.DPM_interval", 10000);
    TQM_check_interval = conf.getInt("se.indexer.TQM_interval", 5000);
    IPM_check_merge_interval = conf.getInt("se.indexer.IPM_merge_interval",
        3600 * 1000);
    mergeindexfilelimit = conf.getInt("mergeindexfilelimit", 200);

    Indexer_max_jobnum = conf.getInt("indexer.max.jobnum", 10);
    poolExecutor = new ThreadPoolExecutor(Indexer_max_jobnum,
        Indexer_max_jobnum * 2, 30, TimeUnit.SECONDS,
        new LinkedBlockingQueue<Runnable>());
  }

  private void initialize(Configuration conf2) {
    try {
      if (conf2 == null)
        conf2 = new Configuration();

      this.conf = conf2;
      fs = FileSystem.get(conf);
      System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\t"
          + fs.getUri());
      hiveConf = new HiveConf(conf, IndexServer.class);
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

  ConcurrentHashMap<String, String> tabletype = new ConcurrentHashMap<String, String>();

  public ConcurrentHashMap<String, ConcurrentHashMap<String, IndexItemStatus>> indexitems = new ConcurrentHashMap<String, ConcurrentHashMap<String, IndexItemStatus>>();
  public MetaDataChecker metaDataChecker = new MetaDataChecker();

  public ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>> tablefiles = new ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>>();
  public DataPartMoniter dataPartMoniter = new DataPartMoniter();

  public ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>>> indexfiles = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>>>();
  public IndexPartMoniter indexPartMoniter = new IndexPartMoniter();

  public ConcurrentLinkedQueue<IndexTask> taskQueue = new ConcurrentLinkedQueue<IndexTask>();
  public TaskQueueManager taskQueueManager = new TaskQueueManager();

  private AtomicBoolean run = new AtomicBoolean(true);

  ThreadPoolExecutor poolExecutor;

  @Override
  public synchronized void start() {
    this.setDaemon(true);
    try {
      metaDataChecker.start();
      Thread.sleep(1000);
      dataPartMoniter.start();
      Thread.sleep(1000);
      indexPartMoniter.start();
      Thread.sleep(1000);
      taskQueueManager.start();
    } catch (InterruptedException e) {
    }
    super.start();
  }

  @Override
  public void run() {
    long time = 0;
    while (run.get()) {
      synchronized (taskQueue) {
        while (taskQueue.size() <= 0) {
          try {
            if (System.currentTimeMillis() - time > 20000) {
              System.err.println("QueueRunner is running....");
              time = System.currentTimeMillis();
            }
            taskQueue.wait(10000 + r.nextInt(5000));
          } catch (InterruptedException e) {
          }
        }
        IndexTask task = taskQueue.poll();
        System.err.println("MAIN: find a task and run it:\t"
            + task.indexpart.indexlocaiton);
        poolExecutor.execute(task);
      }
    }
  }

  public void close() {
    System.err.println("MAIN: close the server...");
    run.set(false);
    taskQueueManager.close();
    indexPartMoniter.close();
    dataPartMoniter.close();
    metaDataChecker.close();
    poolExecutor.shutdownNow();
    this.interrupt();
  }

  public static void main(String[] args) {
    File stop = new File("/tmp/.indexstop");
    File running = new File("/tmp/.indexrunning");
    if (args != null && args.length > 0 && args[0].equals("stop")) {
      try {
        stop.createNewFile();
        running.delete();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return;
    }

    if (running.exists()
        && (System.currentTimeMillis() - running.lastModified() < 15000)) {
      long time = running.lastModified();
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (running.lastModified() == time) {
        running.delete();
      } else {
        return;
      }
    }

    if (stop.exists()) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (stop.exists())
        stop.delete();
    }

    Configuration conf = new Configuration();
    IndexServer server = new IndexServer(conf);
    if (args != null && args.length > 0 && args[0].equals("test")) {
      server.testmode = true;
    }
    server.start();
    try {
      running.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
    }
    new UserCmdProc(server).start();

    while (true) {
      stop = new File("/tmp/.indexstop");
      if (stop.exists()) {
        server.close();
        running.delete();
        stop.delete();
        break;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e1) {
        e1.printStackTrace();
      }

      running.setLastModified(System.currentTimeMillis());

    }
  }

  static class UserCmdProc extends Thread {
    IndexServer server;
    File stop = new File("/tmp/.indexstop");

    public UserCmdProc(IndexServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String str;
      try {
        while (true) {
          str = br.readLine();
          String[] strs = str.split(" ");
          if (strs[0].equals("setindexstatus")) {
            server.setindexstatus(strs[1], Integer.valueOf(strs[2]));
          } else if (strs[0].equals("settest")) {
            server.testmode = strs[1].equals("true");
          } else if (str.equals("exit")) {
            stop.createNewFile();
            break;
          }
        }
      } catch (NumberFormatException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }
  }

  public static class IndexItemStatus {
    public String datadir;
    public String database;
    public String table;
    public String indexname;
    public String field;
    public String indexlocation;
    public int type;
    public int status;

    public String tabletype;

    public String gettablelocation() {
      return datadir + "/"
          + (database.equals("default_db") ? database : (database + ".db"))
          + "/" + table + "/";
    }

    public void read(DataInputStream dis) throws IOException {
      this.datadir = dis.readUTF();
      this.database = dis.readUTF();
      this.table = dis.readUTF();
      this.indexname = dis.readUTF();
      this.field = dis.readUTF();
      this.indexlocation = dis.readUTF();
      this.status = dis.readInt();
      this.type = dis.readInt();
      this.tabletype = dis.readUTF();

    }

    public void write(DataOutputStream dos) throws IOException {
      dos.writeUTF(datadir);
      dos.writeUTF(database);
      dos.writeUTF(table);
      dos.writeUTF(indexname);
      dos.writeUTF(field);
      dos.writeUTF(indexlocation);
      dos.writeInt(status);
      dos.writeInt(type);
      dos.writeUTF(tabletype);
    }
  }

  class Message {
    IndexItemStatus itemStatus;
    int cmd;

    public Message(IndexItemStatus itemStatus, int cmd) {
      this.itemStatus = itemStatus;
      this.cmd = cmd;
    }
  }

  public class MetaDataChecker extends Thread {

    public MetaDataChecker() {
    }

    @Override
    public void run() {
      long time = 0;
      while (run.get()) {
        try {
          if (System.currentTimeMillis() - time > 20000) {
            System.err.println("MetaDataChecker is running....");
            time = System.currentTimeMillis();
          }
          ArrayList<IndexItemStatus> indexItemStatuss = getmetastatus();

          ConcurrentHashMap<String, ConcurrentHashMap<String, IndexItemStatus>> indexitemsnew = new ConcurrentHashMap<String, ConcurrentHashMap<String, IndexItemStatus>>();
          for (IndexItemStatus itemStatus : indexItemStatuss) {
            String tableloc = itemStatus.gettablelocation();
            if (!indexitemsnew.containsKey(tableloc)) {
              indexitemsnew.put(tableloc,
                  new ConcurrentHashMap<String, IndexItemStatus>());
            }

            ConcurrentHashMap<String, IndexItemStatus> indexStatuss = indexitemsnew
                .get(tableloc);
            String indexname = itemStatus.indexname;
            if (!indexStatuss.containsKey(indexname)) {
              indexStatuss.put(indexname, itemStatus);
            }
            tabletype.put(tableloc, itemStatus.tabletype);
            if (itemStatus.status == 0) {
              System.err
                  .println("MDC: find a new rebuild index item and update it ...\t"
                      + itemStatus.indexlocation);
              msg(new Message(itemStatus, 0));
            } else if (itemStatus.status == -1) {
              System.err.println("MDC: find a bad index status=-1:\t"
                  + itemStatus.indexlocation);
            }
          }
          synchronized (indexitems) {
            indexitems = indexitemsnew;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        try {
          Thread.sleep(MDC_check_interval + r.nextInt(5000));
        } catch (Exception e) {
        }
      }
    }

    private void msg(Message message) {
      taskQueueManager.msg(message);
    }

    private ArrayList<IndexItemStatus> getmetastatus() {
      ArrayList<IndexItemStatus> statuss = new ArrayList<IndexItemStatus>();
      if (!testmode) {
        try {
          List<IndexItem> indexInfo = db.getAllIndexSys();
          for (IndexItem indexItem : indexInfo) {
            IndexItemStatus status = new IndexItemStatus();
            status.database = indexItem.getDb();
            status.table = indexItem.getTbl();
            status.indexname = indexItem.getName();
            status.indexlocation = indexItem.getLocation();
            status.status = indexItem.getStatus();
            status.type = indexItem.getType();
            String datadir = wh.getDefaultDatabasePath(indexItem.getDb())
                .getParent().toString();
            datadir = datadir.startsWith("file:") ? datadir.substring(5)
                : datadir;
            status.datadir = new Path(datadir).makeQualified(fs).toString();
            status.field = indexItem.getFieldList();
            try {
              Table t = db.getTable(status.database, status.table);
              String tt = t.getProperty("type");
              if (tt == null) {
                tt = "unknown";
              }
              status.tabletype = tt;
            } catch (HiveException e) {
              e.printStackTrace();
              continue;
            }
            statuss.add(status);
          }
        } catch (HiveException e) {
          e.printStackTrace();
        } catch (MetaException e) {
          e.printStackTrace();
        }
      } else {
        try {
          File file = new File("indexconf");
          if (file.exists()) {
            DataInputStream dis = new DataInputStream(new FileInputStream(file));
            int num = dis.readInt();
            for (int i = 0; i < num; i++) {
              IndexItemStatus status = new IndexItemStatus();
              status.read(dis);
              statuss.add(status);
            }
            dis.close();
          }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return statuss;
    }

    public void close() {
      this.interrupt();
    }
  }

  public class DataPartMoniter extends Thread {

    public DataPartMoniter() {
    }

    @Override
    public void run() {
      long time = 0;
      while (run.get()) {
        try {
          if (System.currentTimeMillis() - time > 20000) {
            System.err.println("DataPartMoniter is running....");
            time = System.currentTimeMillis();
          }
          Set<String> tablelocs;
          tablelocs = indexitems.keySet();
          ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>> tablefilesnew = new ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>>();
          for (String tableloc : tablelocs) {
            boolean column = tabletype.get(tableloc).equals("column");
            FileStatus[] partsstatus = fs.listStatus(new Path(tableloc));
            if (partsstatus == null || partsstatus.length <= 0) {
              continue;
            }
            ConcurrentHashMap<String, HashSet<String>> parts = new ConcurrentHashMap<String, HashSet<String>>();
            if (!partsstatus[0].isDir()) {
              parts.put("nopart",
                  getdatafiles(partsstatus[0].getPath().getParent(), column));
              tablefilesnew.put(tableloc, parts);
            } else {
              for (FileStatus status : partsstatus) {
                parts.put(status.getPath().getName(),
                    getdatafiles(status.getPath(), column));
              }
              tablefilesnew.put(tableloc, parts);
            }
          }

          synchronized (tablefiles) {
            tablefiles = tablefilesnew;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        try {
          Thread.sleep(DPM_check_interval + r.nextInt(5000));
        } catch (Exception e) {
        }
      }
    }

    private HashSet<String> getdatafiles(Path partloc, boolean column) {
      HashSet<String> files = new HashSet<String>();
      try {
        FileStatus[] part2s = fs.listStatus(partloc);
        for (FileStatus p2ss : part2s) {
          if (fs.isFile(p2ss.getPath())) {
            String filename = p2ss.getPath().toString();
            if (column && filename.contains("_idx")) {
              filename = filename.substring(0, filename.lastIndexOf("_idx"));
            }
            files.add(filename);
          } else {
            FileStatus[] filess = fs.listStatus(p2ss.getPath());
            for (FileStatus fstts : filess) {
              String filename = fstts.getPath().makeQualified(fs).toString();
              if (column && filename.contains("_idx")) {
                filename = filename.substring(0, filename.lastIndexOf("_idx"));
              }
              files.add(filename);
            }
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      return files;
    }

    public void close() {
      this.interrupt();
    }
  }

  class IndexPart {
    String tablelocation;
    String indexname;
    String partname;
    String indexlocaiton;

    public IndexPart(String tablelocation, String indexname, String partname,
        String indexlocaiton) {
      this.tablelocation = tablelocation;
      this.indexname = indexname;
      this.partname = partname;
      this.indexlocaiton = indexlocaiton;
    }
  }

  public class IndexPartMoniter extends Thread {

    ConcurrentLinkedQueue<IndexPart> indexparts = new ConcurrentLinkedQueue<IndexPart>();

    @Override
    public synchronized void start() {
      synchronized (tablefiles) {
        Set<String> tablelocs = tablefiles.keySet();
        for (String tableloc : tablelocs) {
          synchronized (indexitems) {
            Set<String> indexs = indexitems.get(tableloc).keySet();
            for (String index : indexs) {
              IndexPart indexpart = new IndexPart(tableloc, index, null,
                  indexitems.get(tableloc).get(index).indexlocation);
              this.update(indexpart);
            }
          }
        }
      }
      super.start();
    }

    @Override
    public void run() {
      long time = 0;
      label: while (run.get()) {
        try {
          long lastmergechecktime = System.currentTimeMillis();
          synchronized (indexparts) {
            while (indexparts.size() <= 0) {
              if (System.currentTimeMillis() - time > 20000) {
                System.err.println("IndexPartMoniter is running....");
                time = System.currentTimeMillis();
              }
              indexparts.wait(5000);
              if (System.currentTimeMillis() - lastmergechecktime > IPM_check_merge_interval) {
                System.err
                    .println("IPM: its time to check the merge tasks ...");
                checkmergetask();
                lastmergechecktime = System.currentTimeMillis();
              }
            }
          }
        } catch (Exception e) {
          continue label;
        }
        try {
          IndexPart indexpart = indexparts.poll();
          System.err
              .println("IPM: find a new indexpart and going to update indexdata ...\t"
                  + indexpart.indexlocaiton);
          ArrayList<String> parts = new ArrayList<String>();
          if (indexpart.partname != null && indexpart.partname.length() > 0
              && !indexpart.partname.equals("null")) {
            parts.add(indexpart.partname);
          } else {
            FileStatus[] fss = fs.listStatus(new Path(indexpart.tablelocation));
            if (fss == null || fss.length <= 0) {
              continue;
            }
            if (!fss[0].isDir()) {
              parts.add("nopart");
            } else {
              for (FileStatus status : fss) {
                parts.add(status.getPath().getName());
              }
            }
          }

          synchronized (indexfiles) {
            if (!indexfiles.containsKey(indexpart.tablelocation)) {
              indexfiles
                  .put(
                      indexpart.tablelocation,
                      new ConcurrentHashMap<String, ConcurrentHashMap<String, HashSet<String>>>());
            }
            if (!indexfiles.get(indexpart.tablelocation).containsKey(
                indexpart.indexname)) {
              indexfiles.get(indexpart.tablelocation).put(indexpart.indexname,
                  new ConcurrentHashMap<String, HashSet<String>>());
            }
            for (String part : parts) {
              HashSet<String> files = new HashSet<String>();
              FileStatus[] indexfilestatuss = fs.listStatus(new Path(
                  indexpart.indexlocaiton + "/" + part));
              if (indexfilestatuss != null && indexfilestatuss.length > 0) {
                for (FileStatus status : indexfilestatuss) {
                  IFormatDataFile ifdf = new IFormatDataFile(conf);
                  ifdf.open(status.getPath().toString());
                  files
                      .addAll(ifdf.fileInfo().head().getUdi().infos().values());
                  ifdf.close();
                }
              }

              indexfiles.get(indexpart.tablelocation).get(indexpart.indexname)
                  .put(part, files);
              System.err.println("IPM: a new part indexfiles updated : "
                  + indexpart.indexlocaiton + "/" + part);
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private void checkmergetask() {
      synchronized (indexfiles) {
        for (String tableloc : indexfiles.keySet()) {
          for (String indexname : indexfiles.get(tableloc).keySet()) {
            for (String partname : indexfiles.get(tableloc).get(indexname)
                .keySet()) {
              Path indexpartpath = new Path(indexitems.get(tableloc).get(
                  indexname).indexlocation
                  + "/" + partname);
              FileStatus[] fss = null;
              try {
                fss = fs.listStatus(indexpartpath);
              } catch (IOException e) {
                e.printStackTrace();
              }
              if (fss != null && fss.length >= mergeindexfilelimit) {
                IndexPart indexpart = new IndexPart(tableloc, indexname,
                    partname, indexpartpath.getParent().toString());
                MergeIndexTask task = new MergeIndexTask(indexpart);
                taskQueueManager.addmergetask(task);
              }
            }
          }
        }
      }
    }

    public void update(IndexPart indexpart) {
      System.err.println("IPM: receive a update cmd: "
          + indexpart.indexlocaiton);
      synchronized (indexparts) {
        indexparts.add(indexpart);
        indexparts.notifyAll();
      }
    }

    public void close() {
      this.interrupt();
    }

  }

  public class TaskQueueManager extends Thread {

    ConcurrentHashMap<String, ConcurrentHashMap<String, IndexTask>> runningtasks = new ConcurrentHashMap<String, ConcurrentHashMap<String, IndexTask>>();
    ConcurrentLinkedQueue<IndexTask> pendingmergetasks = new ConcurrentLinkedQueue<IndexTask>();

    public TaskQueueManager() {
    }

    @Override
    public void run() {
      long time = 0;
      while (run.get()) {
        try {
          if (System.currentTimeMillis() - time > 20000) {
            System.err.println("TaskQueueManager is running....");
            time = System.currentTimeMillis();
          }
          label: for (String indexloc : runningtasks.keySet()) {
            ArrayList<String> finishedparttasks = new ArrayList<String>();
            synchronized (runningtasks) {
              if (runningtasks.get(indexloc).size() <= 0)
                continue;
              for (String part : runningtasks.get(indexloc).keySet()) {
                System.err.println("TQM: check a running task ...");
                switch (runningtasks.get(indexloc).get(part).status) {
                case 2:
                  finishedparttasks.add(part);
                  System.err
                      .println("TQM: check a running task: a task finished and delete it...");
                  break;
                case -1:
                  killtasks(indexloc);
                  System.err
                      .println("TQM: check a running task : some task failed and kill all tasks in the same index ...");
                  continue label;
                }
              }
            }
            for (String part : finishedparttasks) {
              indexPartMoniter
                  .update(runningtasks.get(indexloc).get(part).indexpart);
            }
            try {
              Thread.sleep(3000);
            } catch (InterruptedException e) {
            }
            synchronized (runningtasks) {
              for (String part : finishedparttasks) {
                runningtasks.get(indexloc).remove(part);
              }
              if (runningtasks.get(indexloc).size() <= 0) {
                setindexstatus(indexloc, 2);
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        try {
          synchronized (tablefiles) {
            Set<String> tablelocs = tablefiles.keySet();
            for (String tableloc : tablelocs) {
              Set<String> indexnames = indexitems.get(tableloc).keySet();
              if (!indexfiles.containsKey(tableloc)) {
                for (String indexname : indexnames) {
                  indexPartMoniter.update(new IndexPart(tableloc, indexname,
                      null,
                      indexitems.get(tableloc).get(indexname).indexlocation));
                }
                continue;
              }
              for (String partname : tablefiles.get(tableloc).keySet()) {
                HashSet<String> partfiles = tablefiles.get(tableloc).get(
                    partname);
                for (String indexname : indexnames) {
                  if (indexitems.get(tableloc).get(indexname).status == -1) {
                    continue;
                  }
                  IndexPart indexpart = new IndexPart(tableloc, indexname,
                      partname,
                      indexitems.get(tableloc).get(indexname).indexlocation);
                  if (!indexfiles.get(tableloc).containsKey(indexname)
                      || !indexfiles.get(tableloc).get(indexname)
                          .containsKey(partname)) {
                    indexPartMoniter.update(indexpart);
                    continue;
                  }
                  if (isrunning(indexpart.indexlocaiton, partname)) {
                    continue;
                  }

                  synchronized (indexfiles) {
                    HashSet<String> indexpartfiles = indexfiles.get(tableloc)
                        .get(indexname).get(partname);
                    for (String partfile : partfiles) {
                      if (!indexpartfiles.contains(partfile)) {
                        IndexTask task = new UpdateIndexTask(indexpart,
                            indexitems.get(tableloc).get(indexname).field);
                        if (addruntask(task))
                          System.err.println("TQM: generage a new task :\t"
                              + indexpart.indexlocaiton + "/" + partname);
                        break;
                      }
                    }
                  }
                }
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

        try {
          synchronized (pendingmergetasks) {
            if (pendingmergetasks.size() > 0) {
              for (int i = 0; i < pendingmergetasks.size(); i++) {
                IndexTask task = pendingmergetasks.poll();
                if (task.status == 2)
                  continue;
                if (task.status == 0
                    && !isrunning(task.indexpart.indexlocaiton,
                        task.indexpart.partname)) {
                  addruntask(task);
                }
                pendingmergetasks.add(task);
              }
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
        try {
          Thread.sleep(TQM_check_interval);
        } catch (Exception e) {
        }
      }
    }

    public void close() {
      this.interrupt();
      for (String indexloc : runningtasks.keySet()) {
        for (IndexTask task : runningtasks.get(indexloc).values()) {
          task.kill();
        }
      }
    }

    private void killtasks(String indexloc) {
      IndexPart indexpart = null;
      for (IndexTask task : runningtasks.get(indexloc).values()) {
        task.kill();
        indexpart = task.indexpart;
      }
      if (indexpart != null) {
        indexitems.get(indexpart.tablelocation).get(indexpart.indexname).status = -1;
      }
      runningtasks.get(indexloc).clear();
      setindexstatus(indexloc, -1);
    }

    public void addmergetask(MergeIndexTask task) {
      synchronized (pendingmergetasks) {
        boolean add = true;
        for (int i = 0; i < pendingmergetasks.size(); i++) {
          IndexTask task1 = pendingmergetasks.poll();
          pendingmergetasks.add(task1);
          if (task1.indexpart.indexlocaiton
              .equals(task.indexpart.indexlocaiton)
              && task1.indexpart.partname.equals(task.indexpart.partname)) {
            add = false;
            break;
          }
        }
        if (add) {
          this.pendingmergetasks.add(task);
          System.err.println("TQM: add a new mergetask:\t"
              + task.indexpart.indexlocaiton + "/" + task.indexpart.partname);
        }
      }
    }

    public void msg(Message message) {
      System.err.println("TQM: receive a message: " + message.cmd);
      try {
        if (message.cmd == 0) {
          if (runningtasks.containsKey(message.itemStatus.indexlocation)) {
            for (IndexTask task : runningtasks.get(
                message.itemStatus.indexlocation).values()) {
              task.kill();
            }
            runningtasks.get(message.itemStatus.indexlocation).clear();
          }
          fs.delete(new Path(message.itemStatus.indexlocation), true);
        }

        IndexPart indexpart = new IndexPart(
            message.itemStatus.gettablelocation(),
            message.itemStatus.indexname, null,
            message.itemStatus.indexlocation);
        indexPartMoniter.update(indexpart);

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private boolean addruntask(IndexTask task) {
      if (isrunning(task.indexpart.indexlocaiton, task.indexpart.partname)) {
        return false;
      }
      if (!runningtasks.containsKey(task.indexpart.indexlocaiton)) {
        runningtasks.put(task.indexpart.indexlocaiton,
            new ConcurrentHashMap<String, IndexTask>());
      }
      runningtasks.get(task.indexpart.indexlocaiton).put(
          task.indexpart.partname, task);
      if (!setindexstatus(task.indexpart.indexlocaiton, 1)) {
        System.err.println("TQM: add inidexstatus fail...");
        return false;
      }
      synchronized (taskQueue) {
        taskQueue.add(task);
        taskQueue.notifyAll();
      }
      return true;
    }

    private boolean isrunning(String indexlocaiton, String partname) {
      if (!runningtasks.containsKey(indexlocaiton)) {
        return false;
      }
      if (!runningtasks.get(indexlocaiton).containsKey(partname)) {
        return false;
      }
      return true;
    }
  }

  abstract class IndexTask extends Thread {
    int status = 0;
    IndexPart indexpart;
    boolean kill = false;

    public IndexTask(IndexPart indexpart) {
      this.indexpart = indexpart;
    }

    public void kill() {
      kill = true;
    }

    protected void starttask() {
      this.status = 1;
    }

    protected void finishtask(int status) {
      this.status = status;
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
      String[] idx1s = filename.substring(filename.lastIndexOf("_idx") + 4)
          .split("_");
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
          fs.mkdirs(indexdir);
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
      Path tmp = new Path(TMPDIR + x).makeQualified(fs);
      try {
        while (fs.exists(tmp)) {
          x = (int) (Math.random() * Integer.MAX_VALUE);
          tmp = new Path(TMPDIR + x).makeQualified(fs);
        }
        fs.mkdirs(tmp);
        return tmp;
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  class UpdateIndexTask extends IndexTask {
    String inputdir;
    String outputdir;
    String indexids;
    boolean column;

    public UpdateIndexTask(IndexPart indexpart, String indexids) {
      super(indexpart);
      this.indexids = indexids;
      this.column = tabletype.get(indexpart.tablelocation).equals("column");
      inputdir = indexpart.tablelocation + "/"
          + (indexpart.partname.equals("nopart") ? "" : indexpart.partname);
      outputdir = indexpart.indexlocaiton + "/" + indexpart.partname;
    }

    @Override
    public void run() {
      long starttime = System.currentTimeMillis();
      starttask();
      try {
        HashSet<String> indexedpartfiles = new HashSet<String>();
        FileStatus[] indexfilestatuss = fs.listStatus(new Path(
            indexpart.indexlocaiton + "/" + indexpart.partname));
        if (indexfilestatuss != null && indexfilestatuss.length > 0) {
          for (FileStatus status : indexfilestatuss) {
            IFormatDataFile ifdf = new IFormatDataFile(conf);
            ifdf.open(status.getPath().toString());
            indexedpartfiles.addAll(ifdf.fileInfo().head().getUdi().infos()
                .values());
            ifdf.close();
          }
        }

        HashSet<String> currentdatafiles = tablefiles.get(
            indexpart.tablelocation).get(indexpart.partname);

        StringBuffer inputfiles = new StringBuffer();
        for (String file : currentdatafiles) {
          if (!indexedpartfiles.contains(file)) {
            inputfiles.append(file).append(",");
          }
        }
        if (inputfiles.length() <= 0) {
          finishtask(2);
          return;
        }

        Path tmpoutputdir = gettmpoutputdir();
        RunningJob job = IndexMR.run(conf,
            inputfiles.substring(0, inputfiles.length() - 1), column, indexids,
            tmpoutputdir.toString());
        if (job == null) {
          finishtask(-1);
          return;
        }
        String lastReport = "";
        SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd hh:mm:ss,SSS");
        long reportTime = System.currentTimeMillis();
        long maxReportInterval = 3 * 1000;
        while (!job.isComplete()) {
          if (kill) {
            job.killJob();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }

          int mapProgress = Math.round(job.mapProgress() * 100);
          int reduceProgress = Math.round(job.reduceProgress() * 100);

          String report = " map = " + mapProgress + "%,  reduce = "
              + reduceProgress + "%";

          if (!report.equals(lastReport)
              || System.currentTimeMillis() >= reportTime + maxReportInterval) {

            String output = dateFormat.format(Calendar.getInstance().getTime())
                + report;
            System.err.println(job.getID() + ":\t" + output);
            lastReport = report;
            reportTime = System.currentTimeMillis();
          }
        }

        if (!kill && job.isSuccessful()) {
          Path indexdirpath = new Path(outputdir);

          checkindexdir(indexdirpath);

          FileStatus[] fss = fs.listStatus(tmpoutputdir);
          if (fss != null && fss.length > 0)
            for (FileStatus status : fss) {
              if (status.isDir())
                continue;
              Path indexfile = getnewindexfile(indexdirpath);
              fs.rename(status.getPath(), indexfile);
            }
          fs.delete(tmpoutputdir, true);
          finishtask(2);
        } else if (kill) {
          finishtask(-2);
        } else {
          finishtask(-1);
        }
        LOG.info("Create index task finished:\t" + job.getID() + "\t"
            + indexpart.indexlocaiton);
        LOG.info("used time :\t" + (System.currentTimeMillis() - starttime)
            + "ms");

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  class MergeIndexTask extends IndexTask {

    String inputdir;
    String outputdir;

    public MergeIndexTask(IndexPart indexpart) {
      super(indexpart);
      inputdir = indexpart.indexlocaiton + "/" + indexpart.partname;
    }

    @Override
    public void run() {
      starttask();
      try {
        Path indexpartpath = new Path(inputdir);

        StringBuffer sb = new StringBuffer();
        FileStatus[] parts = fs.listStatus(indexpartpath);
        if (parts == null || parts.length < mergeindexfilelimit) {
          finishtask(2);
          return;
        }
        for (FileStatus p : parts) {
          sb.append(p.getPath().toString() + ",");
        }
        String inputfiles = sb.substring(0, sb.length() - 1);

        Path tmpoutputdir = gettmpoutputdir();

        RunningJob job = IndexMergeMR.run(inputfiles, tmpoutputdir.toString(),
            new Configuration());

        if (job == null) {
          finishtask(-1);
          return;
        }
        String lastReport = "";
        SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd hh:mm:ss,SSS");
        long reportTime = System.currentTimeMillis();
        long maxReportInterval = 3 * 1000;
        while (!job.isComplete()) {
          if (kill) {
            job.killJob();
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }

          int mapProgress = Math.round(job.mapProgress() * 100);
          int reduceProgress = Math.round(job.reduceProgress() * 100);

          String report = " map = " + mapProgress + "%,  reduce = "
              + reduceProgress + "%";

          if (!report.equals(lastReport)
              || System.currentTimeMillis() >= reportTime + maxReportInterval) {
            String output = dateFormat.format(Calendar.getInstance().getTime())
                + report;
            System.err.println(output);
            lastReport = report;
            reportTime = System.currentTimeMillis();
          }
        }

        if (!kill && job.isSuccessful()) {
          Path tmpoutputdir1 = gettmpoutputdir();
          fs.rename(indexpartpath, tmpoutputdir1);

          fs.delete(indexpartpath, true);
          fs.mkdirs(indexpartpath);
          fs.rename(new Path(tmpoutputdir, "part-00000"), new Path(
              indexpartpath, "indexfile0"));
          fs.delete(tmpoutputdir, true);
          fs.delete(tmpoutputdir1, true);
          finishtask(2);
        } else if (kill) {
          finishtask(-2);
        } else {
          finishtask(-1);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      LOG.info("a merge task finished");
    }
  }

  public boolean setindexstatus(String indexloc, int status) {
    if (!testmode) {
      Path indexpath = new Path(indexloc);
      try {
        String dbname = indexpath.getParent().getParent().getName();
        if (dbname.endsWith(".db")) {
          dbname = dbname.substring(0, dbname.lastIndexOf(".db"));
        }
        return db.setIndexStatus(dbname, indexpath.getParent().getName(),
            indexpath.getName(), status);
      } catch (HiveException e1) {
        e1.printStackTrace();
        return false;
      }
    } else {
      try {
        ArrayList<IndexItemStatus> statuss = new ArrayList<IndexItemStatus>();
        File file = new File("indexconf");
        boolean exist = false;
        if (file.exists()) {
          DataInputStream dis = new DataInputStream(new FileInputStream(file));
          int num = dis.readInt();
          for (int i = 0; i < num; i++) {
            IndexItemStatus itemstatus = new IndexItemStatus();
            itemstatus.read(dis);
            if (itemstatus.indexlocation.equals(indexloc)) {
              itemstatus.status = status;
              exist = true;
            }
            statuss.add(itemstatus);
          }
          dis.close();

          DataOutputStream dos = new DataOutputStream(
              new FileOutputStream(file));
          dos.writeInt(statuss.size());
          for (IndexItemStatus indexItemStatus : statuss) {
            indexItemStatus.write(dos);
          }
          dos.close();
        }
        if (exist)
          return true;
        return false;
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }
  }
}
