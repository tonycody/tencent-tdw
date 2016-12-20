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
package org.apache.hadoop.hive.ql.exec;

import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ACLWork;
import org.apache.hadoop.hive.ql.plan.createGroupDesc;
import org.apache.hadoop.hive.ql.plan.createRoleDesc;
import org.apache.hadoop.hive.ql.plan.createUserDesc;
import org.apache.hadoop.hive.ql.plan.dropGroupDesc;
import org.apache.hadoop.hive.ql.plan.dropRoleDesc;
import org.apache.hadoop.hive.ql.plan.dropUserDesc;
import org.apache.hadoop.hive.ql.plan.grantGroupDesc;
import org.apache.hadoop.hive.ql.plan.grantPrisDesc;
import org.apache.hadoop.hive.ql.plan.grantRoleDesc;
import org.apache.hadoop.hive.ql.plan.revokeGroupDesc;
import org.apache.hadoop.hive.ql.plan.revokePriDesc;
import org.apache.hadoop.hive.ql.plan.revokeRoleDesc;
import org.apache.hadoop.hive.ql.plan.setPwdDesc;
import org.apache.hadoop.hive.ql.plan.showGrantsDesc;
import org.apache.hadoop.hive.ql.plan.showGroupsDesc;
import org.apache.hadoop.hive.ql.plan.showRolesDesc;
import org.apache.hadoop.hive.ql.plan.showUsersDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;

public class ACLTask extends Task<ACLWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  static final private Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  static final private int separator = Utilities.tabCode;
  static final private int terminator = Utilities.newLineCode;

  @Override
  public int execute() {
    Hive db;
    try {
      db = Hive.get();

      createUserDesc createUserD = work.getCreateUser();
      if (createUserD != null) {
        return createUser(db, createUserD);
      }

      createRoleDesc createRoleD = work.getCreateRole();
      if (createRoleD != null) {
        return createRole(db, createRoleD);
      }

      dropRoleDesc dropRoleD = work.getDropRole();
      if (dropRoleD != null) {
        return dropRole(db, dropRoleD);
      }

      dropUserDesc dropUserD = work.getDropUser();
      if (dropUserD != null) {
        return dropUser(db, dropUserD);
      }

      grantPrisDesc grantPrisD = work.getGrantPris();
      if (grantPrisD != null) {
        return grantPris(db, grantPrisD);
      }

      grantRoleDesc grantRoleD = work.getGrantRole();
      if (grantRoleD != null) {
        return grantRole(db, grantRoleD);
      }

      setPwdDesc setPwdD = work.getSetPwd();
      if (setPwdD != null) {
        return setPwd(db, setPwdD);
      }

      showGrantsDesc showGrantsD = work.getShowGrants();
      if (showGrantsD != null) {
        return showGrants(db, showGrantsD);
      }

      showUsersDesc showUsersD = work.getShowUsers();
      if (showUsersD != null) {
        return showUsers(db, showUsersD);
      }

      revokePriDesc revokePriD = work.getRevokePri();
      if (revokePriD != null) {
        return revokePri(db, revokePriD);
      }

      revokeRoleDesc revokeRoleD = work.getRevokeRole();
      if (revokeRoleD != null) {
        return revokeRole(db, revokeRoleD);
      }
      showRolesDesc showRolesD = work.getShowRoles();
      if (showRolesD != null) {
        return showRoles(db, showRolesD);
      }

      createGroupDesc cGD = work.getCreateGroup();
      if (cGD != null) {
        return createGroup(db, cGD);
      }
      grantGroupDesc gGD = work.getGrantGroup();
      if (gGD != null) {
        return grantGroup(db, gGD);
      }
      revokeGroupDesc rGD = work.getRevokeGroup();
      if (rGD != null) {
        return revokeGroup(db, rGD);
      }
      dropGroupDesc dGD = work.getDropGroup();
      if (dGD != null) {
        return dropGroup(db, dGD);
      }
      showGroupsDesc sGD = work.getShowGroup();
      if (sGD != null) {
        return showGroup(db, sGD);
      }

    } catch (HiveException e) {
      console.printError("FAILED: Error in Access Control: " + e.getMessage(),
          "\n" + StringUtils.stringifyException(e));
      LOG.debug(StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "FAILED: Error in Access Control: " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      return 1;
    }
    assert false;
    return 0;
  }

  private int showGroup(Hive db, showGroupsDesc sGD) throws HiveException {

    List<String> groups = null;
    if (sGD.getPattern() != null) {
      LOG.info("pattern: " + sGD.getPattern());
      groups = db.getGroups(sGD.getPattern());
      LOG.info("results : " + groups.size());
    } else
      groups = db.getGroups(".*");

    try {
      FileSystem fs = sGD.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(sGD.getResFile());
      SortedSet<String> sortedTbls = new TreeSet<String>(groups);
      Iterator<String> iterTbls = sortedTbls.iterator();

      while (iterTbls.hasNext()) {
        outStream.writeBytes(iterTbls.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show groups: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show groups: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show groups: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show groups: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int revokeGroup(Hive db, revokeGroupDesc rGD) throws HiveException {
    boolean re = db.revoke_user_group(rGD.getGroupName(), rGD.getUsers(),
        rGD.getUserName());
    if (re) {
      return 0;
    } else {
      return 1;
    }
  }

  private int grantGroup(Hive db, grantGroupDesc cGD) throws HiveException {
    boolean re = db.grant_user_group(cGD.getGroupName(), cGD.getUsers(),
        cGD.getUserName());
    if (re) {
      return 0;
    } else {
      return 1;
    }
  }

  private int createGroup(Hive db, createGroupDesc cGD) throws HiveException {
    boolean re = db.add_user_group(cGD.getGroupName(), cGD.getCreator(),
        cGD.getDBName());
    if (re) {
      return 0;
    } else {
      return 1;
    }
  }

  private int dropGroup(Hive db, dropGroupDesc cGD) throws HiveException {
    boolean re = db.drop_user_group(cGD.getGroupName(), cGD.getDropper(),
        cGD.getDBName());
    if (re) {
      return 0;
    } else {
      return 1;
    }
  }

  private int showRoles(Hive db, showRolesDesc showRolesD) throws HiveException {

    List<String> roles;
    if (showRolesD.getUser() == null)
      roles = db.showRoles(showRolesD.getWho());
    else
      return 0;

    try {
      FileSystem fs = showRolesD.getTmpFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showRolesD.getTmpFile());
      LOG.info("show roles tmp file:" + showRolesD.getTmpFile().toString());
      SortedSet<String> sortedRoles = new TreeSet<String>(roles);
      Iterator<String> iterRoles = sortedRoles.iterator();

      outStream.writeBytes("ALL roles in TDW:");
      outStream.write(terminator);

      while (iterRoles.hasNext()) {
        outStream.writeBytes(iterRoles.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show roles: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show roles: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    LOG.info("show roles OK");
    return 0;
  }

  private int revokeRole(Hive db, revokeRoleDesc revokeRoleD)
      throws HiveException {

    db.revokeRoleFromUser(revokeRoleD.getWho(), revokeRoleD.getUsers(),
        revokeRoleD.getRoles());
    return 0;
  }

  private int revokePri(Hive db, revokePriDesc revokePriD) throws HiveException {
    db.revokeAuth(revokePriD.getWho(), revokePriD.getUser(),
        revokePriD.getPris(), revokePriD.getDb(), revokePriD.getTable());
    return 0;
  }

  private int showUsers(Hive db, showUsersDesc showUsersD) throws HiveException {
    List<String> users = db.showUsers(showUsersD.getWho());

    try {
      FileSystem fs = showUsersD.getTmpFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showUsersD.getTmpFile());
      SortedSet<String> sortedUsers = new TreeSet<String>(users);
      Iterator<String> iterUsers = sortedUsers.iterator();

      outStream.writeBytes("All users in TDW:");
      outStream.write(terminator);

      while (iterUsers.hasNext()) {
        outStream.writeBytes(iterUsers.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show users: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show users: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    LOG.info("show users OK");
    return 0;
  }

  private int showGrants(Hive db, showGrantsDesc showGrantsD)
      throws HiveException {
    List<String> grants = db.showGrants(showGrantsD.getWho(),
        showGrantsD.getUser());

    try {
      FileSystem fs = showGrantsD.getTmpFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showGrantsD.getTmpFile());
      Iterator<String> iterGrants = grants.iterator();

      while (iterGrants.hasNext()) {
        outStream.writeBytes(iterGrants.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show grants: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show grants: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int setPwd(Hive db, setPwdDesc setPwdD) throws HiveException {
    db.setPasswd(setPwdD.getWho(), setPwdD.getName(), setPwdD.getPasswd());
    return 0;
  }

  private int grantRole(Hive db, grantRoleDesc grantRoleD) throws HiveException {
    db.grantRoleToUser(grantRoleD.getWho(), grantRoleD.getUsers(),
        grantRoleD.getRoles());
    return 0;
  }

  private int grantPris(Hive db, grantPrisDesc grantPrisD) throws HiveException {
    db.grantAuth(grantPrisD.getWho(), grantPrisD.getUser(),
        grantPrisD.getPrivileges(), grantPrisD.getDb(), grantPrisD.getTable());
    return 0;
  }

  private int dropUser(Hive db, dropUserDesc dropUserD) throws HiveException {
    db.dropUsers(dropUserD.getWho(), dropUserD.getUserList());
    return 0;
  }

  private int dropRole(Hive db, dropRoleDesc dropRoleD) throws HiveException {
    db.dropRoles(dropRoleD.getWho(), dropRoleD.getRoles());
    return 0;
  }

  private int createRole(Hive db, createRoleDesc createRoleD)
      throws HiveException {
    db.createRoles(createRoleD.getWho(), createRoleD.getRoles());
    return 0;
  }

  private int createUser(Hive db, createUserDesc createUserD)
      throws HiveException {
    db.createUser(createUserD.getWho(), createUserD.getUserName(),
        createUserD.getPasswd());
    return 0;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "ACL";
  }

}
