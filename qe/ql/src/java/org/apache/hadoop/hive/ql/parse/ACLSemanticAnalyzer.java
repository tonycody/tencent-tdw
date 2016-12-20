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
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ACLWork;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.createGroupDesc;
import org.apache.hadoop.hive.ql.plan.createRoleDesc;
import org.apache.hadoop.hive.ql.plan.createUserDesc;
import org.apache.hadoop.hive.ql.plan.dropGroupDesc;
import org.apache.hadoop.hive.ql.plan.dropRoleDesc;
import org.apache.hadoop.hive.ql.plan.dropUserDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
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
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.showUsersDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;

public class ACLSemanticAnalyzer extends BaseSemanticAnalyzer {

  public ACLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_REVOKE_PRI:
      revokePri(ast);
      break;
    case HiveParser.TOK_GRANT_ROLE:
      grantRole(ast);
      break;
    case HiveParser.TOK_SHOW_ROLES:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      showRoles(ast);
      break;
    case HiveParser.TOK_REVOKE_ROLE:
      revokeRole(ast);
      break;
    case HiveParser.TOK_SHOW_GRANTS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      showGrants(ast);
      break;
    case HiveParser.TOK_DROP_ROLE:
      dropRole(ast);
      break;
    case HiveParser.TOK_CREATE_ROLE:
      createRole(ast);
      break;
    case HiveParser.TOK_CREATE_USER:
      createUser(ast);
      break;
    case HiveParser.TOK_DROP_USER:
      dropUser(ast);
      break;
    case HiveParser.TOK_SHOW_USERS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      showUsers(ast);
      break;
    case HiveParser.TOK_SET_PWD:
      setPwd(ast);
      break;
    case HiveParser.TOK_GRANT_PRIS:
      grantPris(ast);
      break;
    case HiveParser.TOK_SHOWUSERGROUPS:
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      showGroups(ast);
      break;
    case HiveParser.TOK_CREATE_USERGROUP:
      createUserGroup(ast);
      break;
    case HiveParser.TOK_DROP_USERGROUP:
      dropUserGroup(ast);
      break;
    case HiveParser.TOK_GRANT_USERGROUP:
      grantUserGroup(ast);
      break;
    case HiveParser.TOK_REVOKE_USERGROUP:
      revokeUserGroup(ast);
      break;
    default:
      throw new SemanticException("Unsupported command.");

    }

  }

  private void createUserGroup(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      String gName = unescapeIdentifier(ast.getChild(0).getText());
      createGroupDesc cud = new createGroupDesc(gName, ctx.getUserName(),
          ctx.getDBname());
      LOG.debug(gName + "  " + ctx.getUserName() + "  " + ctx.getDBname());
      rootTasks.add(TaskFactory.get(new ACLWork(cud), conf));
    }
  }

  private void dropUserGroup(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      String gName = unescapeIdentifier(ast.getChild(0).getText());
      dropGroupDesc dgd = new dropGroupDesc(gName, ctx.getUserName(),
          ctx.getDBname());
      LOG.debug(gName + "  " + ctx.getUserName() + "  " + ctx.getDBname());
      rootTasks.add(TaskFactory.get(new ACLWork(dgd), conf));
    }
  }

  private void grantUserGroup(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 2) {
      ArrayList<String> users = new ArrayList<String>();
      users.clear();
      String gName = unescapeIdentifier(ast.getChild(0).getText());
      for (int i = 0; i < ast.getChild(1).getChildCount(); ++i) {
        users.add(unescapeIdentifier(ast.getChild(1).getChild(i).getText()));
      }
      grantGroupDesc ggd = new grantGroupDesc(gName, users, ctx.getUserName());
      rootTasks.add(TaskFactory.get(new ACLWork(ggd), conf));
    }

  }

  private void revokeUserGroup(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 2) {
      ArrayList<String> users = new ArrayList<String>();
      users.clear();
      String gName = unescapeIdentifier(ast.getChild(0).getText());
      for (int i = 0; i < ast.getChild(1).getChildCount(); ++i) {
        users.add(unescapeIdentifier(ast.getChild(1).getChild(i).getText()));
      }
      revokeGroupDesc rgd = new revokeGroupDesc(gName, users, ctx.getUserName());
      rootTasks.add(TaskFactory.get(new ACLWork(rgd), conf));
    }
  }

  private void showGroups(ASTNode ast) throws SemanticException {
    showGroupsDesc showGDesc;
    if (ast.getChildCount() == 1) {
      String tableNames = unescapeSQLString(ast.getChild(0).getText());
      showGDesc = new showGroupsDesc(ctx.getResFile(), tableNames);
    } else {
      showGDesc = new showGroupsDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new ACLWork(showGDesc), conf));
    setFetchTask(createFetchTask(showGDesc.getSchema()));
  }

  private void createUser(ASTNode ast) throws SemanticException {
    boolean isDBA = false;
    if (ast.getChildCount() == 3) {
      isDBA = true;
    }
    String userName = unescapeIdentifier(ast.getChild(0).getText());
    String passwd = unescapeSQLString(ast.getChild(1).getText());

    createUserDesc cud = new createUserDesc(userName, passwd, isDBA,
        ctx.getUserName(), ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(cud), conf));
  }

  private void dropUser(ASTNode ast) throws SemanticException {
    ArrayList<String> names = new ArrayList<String>();
    for (int i = 0; i < ast.getChildCount(); ++i) {
      names.add(unescapeIdentifier(ast.getChild(i).getText()));
    }
    dropUserDesc dud = new dropUserDesc(names, ctx.getUserName(),
        ctx.getDBname());
    rootTasks.add(TaskFactory.get(new ACLWork(dud), conf));
  }

  private void setPwd(ASTNode ast) throws SemanticException {
    String userName = null;
    if (ast.getChildCount() == 2) {
      userName = ast.getChild(1).getText();
    }
    String passwd = unescapeSQLString(ast.getChild(0).getText());
    setPwdDesc spd = new setPwdDesc(passwd, userName, ctx.getUserName(),
        ctx.getDBname());
    rootTasks.add(TaskFactory.get(new ACLWork(spd), conf));
  }

  private void showUsers(ASTNode ast) throws SemanticException {

    showUsersDesc sud = new showUsersDesc(ctx.getResFile(), ctx.getUserName(),
        ctx.getDBname());
    rootTasks.add(TaskFactory.get(new ACLWork(sud), conf));
    setFetchTask(createFetchTask(sud.getSchema()));
  }

  private void grantPris(ASTNode ast) throws SemanticException {
    ArrayList<String> pris = new ArrayList<String>();

    for (int i = 0; i < ast.getChild(0).getChildCount(); ++i) {
      pris.add(ast.getChild(0).getChild(i).getText());
    }

    String user = unescapeIdentifier(ast.getChild(1).getText());

    String db = null;
    String table = null;
    if (ast.getChildCount() > 2) {
      db = unescapeIdentifier(ast.getChild(2).getText());
      table = unescapeIdentifier(ast.getChild(3).getText());
    }

    grantPrisDesc gpd = new grantPrisDesc(user, pris, db, table,
        ctx.getUserName(), ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(gpd), conf));
  }

  private void revokePri(ASTNode ast) throws SemanticException {
    ArrayList<String> pris = new ArrayList<String>();

    for (int i = 0; i < ast.getChild(0).getChildCount(); ++i) {
      pris.add(ast.getChild(0).getChild(i).getText());
    }

    String user = unescapeIdentifier(ast.getChild(1).getText());

    String db = null;
    String table = null;
    if (ast.getChildCount() > 2) {
      db = unescapeIdentifier(ast.getChild(2).getText());
      table = unescapeIdentifier(ast.getChild(3).getText());
    }

    revokePriDesc rpd = new revokePriDesc(user, pris, db, table,
        ctx.getUserName(), ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(rpd), conf));
  }

  private void showGrants(ASTNode ast) throws SemanticException {
    String user = null;
    if (ast.getChildCount() == 1)
      user = unescapeIdentifier(ast.getChild(0).getText());

    showGrantsDesc sgd = new showGrantsDesc(user, ctx.getResFile(),
        ctx.getUserName(), ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(sgd), conf));
    setFetchTask(createFetchTask(sgd.getSchema()));
  }

  private void createRole(ASTNode ast) throws SemanticException {
    boolean asDBA = false;
    ArrayList<String> roles = new ArrayList<String>();
    if (ast.getChild(ast.getChildCount() - 1).getText().equalsIgnoreCase("dba")) {
      asDBA = true;
    }
    for (int i = 0; i < ast.getChildCount() - 1; ++i) {
      roles.add(unescapeIdentifier(ast.getChild(i).getText()));
    }
    if (!asDBA)
      roles.add(ast.getChild(ast.getChildCount() - 1).getText());

    createRoleDesc crd = new createRoleDesc(roles, asDBA, ctx.getUserName(),
        ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(crd), conf));
  }

  private void dropRole(ASTNode ast) throws SemanticException {
    ArrayList<String> roles = new ArrayList<String>();
    for (int i = 0; i < ast.getChildCount(); ++i) {
      roles.add(unescapeIdentifier(ast.getChild(i).getText()));
    }
    dropRoleDesc drd = new dropRoleDesc(roles, ctx.getUserName(),
        ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(drd), conf));
  }

  private void showRoles(ASTNode ast) throws SemanticException {
    String user = null;
    if (ast.getChildCount() > 0) {
      user = unescapeIdentifier(ast.getChild(0).getText());
    }
    showRolesDesc srd = new showRolesDesc(user, ctx.getResFile(),
        ctx.getUserName(), ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(srd), conf));
    setFetchTask(createFetchTask(srd.getSchema()));
  }

  private void grantRole(ASTNode ast) throws SemanticException {
    ArrayList<String> roles = new ArrayList<String>();
    ArrayList<String> users = new ArrayList<String>();
    for (int i = 0; i < ast.getChild(0).getChildCount(); ++i) {
      roles.add(unescapeIdentifier(ast.getChild(0).getChild(i).getText()));
    }
    for (int i = 0; i < ast.getChild(1).getChildCount(); ++i) {
      users.add(unescapeIdentifier(ast.getChild(1).getChild(i).getText()));
    }

    grantRoleDesc grd = new grantRoleDesc(roles, users, ctx.getUserName(),
        ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(grd), conf));
  }

  private void revokeRole(ASTNode ast) throws SemanticException {
    ArrayList<String> roles = new ArrayList<String>();
    ArrayList<String> users = new ArrayList<String>();
    for (int i = 0; i < ast.getChild(0).getChildCount(); ++i) {
      roles.add(unescapeIdentifier(ast.getChild(0).getChild(i).getText()));
    }
    for (int i = 0; i < ast.getChild(1).getChildCount(); ++i) {
      users.add(unescapeIdentifier(ast.getChild(1).getChild(i).getText()));
    }

    revokeRoleDesc rrd = new revokeRoleDesc(roles, users, ctx.getUserName(),
        ctx.getDBname());

    rootTasks.add(TaskFactory.get(new ACLWork(rrd), conf));
  }

  private void changeUser(ASTNode ast) throws SemanticException {
    String toName = unescapeIdentifier(ast.getChild(0).getText());
    if (toName.equalsIgnoreCase(SessionState.get().getUserName())) {
      return;
    }
    try {
      if (db.hasAuth(SessionState.get().getUserName(), Hive.Privilege.DBA_PRIV,
          null, null)) {

        if (!db.hasAuth(toName, Hive.Privilege.DBA_PRIV, null, null)) {
          SessionState.get().setUserName(toName);
          return;
        }
      }
      if (ast.getChildCount() < 2) {
        throw new SemanticException("change user error: must set passwd!");
      }
      db.isAUser(toName, unescapeSQLString(ast.getChild(1).getText()));
    } catch (HiveException e) {
      throw new SemanticException("change user error: " + e.getMessage());
    }
  }

  private Task<? extends Serializable> createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);

    fetchWork fetch = new fetchWork(ctx.getResFile().toString(), new tableDesc(
        LazySimpleSerDe.class, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return TaskFactory.get(fetch, this.conf);
  }

}
