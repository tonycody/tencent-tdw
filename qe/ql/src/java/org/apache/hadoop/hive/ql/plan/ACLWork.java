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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

public class ACLWork implements Serializable {

  private static final long serialVersionUID = 1L;
  createUserDesc createUser;
  createRoleDesc createRole;
  dropRoleDesc dropRole;
  dropUserDesc dropUser;
  grantPrisDesc grantPris;
  grantRoleDesc grantRole;
  setPwdDesc setPwd;
  showGrantsDesc showGrants;
  showUsersDesc showUsers;
  revokePriDesc revokePri;
  revokeRoleDesc revokeRole;
  showRolesDesc showRoles;

  createGroupDesc cGD;
  grantGroupDesc gGD;
  revokeGroupDesc rGD;
  dropGroupDesc dGD;
  showGroupsDesc sGD;

  public ACLWork() {
    super();
  }

  public showRolesDesc getShowRoles() {
    return showRoles;
  }

  public void setShowRoles(showRolesDesc showRoles) {
    this.showRoles = showRoles;
  }

  public ACLWork(showRolesDesc showRoles) {
    super();
    this.showRoles = showRoles;
  }

  public ACLWork(createUserDesc createUser) {
    super();
    this.createUser = createUser;
  }

  public ACLWork(createRoleDesc createRole) {
    super();
    this.createRole = createRole;
  }

  public ACLWork(dropRoleDesc dropRole) {
    super();
    this.dropRole = dropRole;
  }

  public ACLWork(dropUserDesc dropUser) {
    super();
    this.dropUser = dropUser;
  }

  public ACLWork(grantPrisDesc grantPris) {
    super();
    this.grantPris = grantPris;
  }

  public ACLWork(grantRoleDesc grantRole) {
    super();
    this.grantRole = grantRole;
  }

  public ACLWork(setPwdDesc setPwd) {
    super();
    this.setPwd = setPwd;
  }

  public ACLWork(showGrantsDesc showGrants) {
    super();
    this.showGrants = showGrants;
  }

  public ACLWork(showUsersDesc showUsers) {
    super();
    this.showUsers = showUsers;
  }

  public ACLWork(revokePriDesc revokePri) {
    super();
    this.revokePri = revokePri;
  }

  public ACLWork(revokeRoleDesc revokeRole) {
    super();
    this.revokeRole = revokeRole;
  }

  public ACLWork(createGroupDesc cG) {
    super();
    this.cGD = cG;
  }

  public createGroupDesc getCreateGroup() {
    return cGD;
  }

  public void setCreateGroup(createGroupDesc cG) {
    this.cGD = cG;
  }

  public ACLWork(grantGroupDesc gG) {
    super();
    this.gGD = gG;
  }

  public grantGroupDesc getGrantGroup() {
    return gGD;
  }

  public void setGrantGroup(grantGroupDesc gG) {
    this.gGD = gG;
  }

  public ACLWork(revokeGroupDesc rG) {
    super();
    this.rGD = rG;
  }

  public revokeGroupDesc getRevokeGroup() {
    return rGD;
  }

  public void setGrantGroup(revokeGroupDesc rG) {
    this.rGD = rG;
  }

  public ACLWork(dropGroupDesc dG) {
    super();
    this.dGD = dG;
  }

  public dropGroupDesc getDropGroup() {
    return dGD;
  }

  public void setDropGroup(dropGroupDesc dG) {
    this.dGD = dG;
  }

  public ACLWork(showGroupsDesc sG) {
    super();
    this.sGD = sG;
  }

  public showGroupsDesc getShowGroup() {
    return sGD;
  }

  public void setShowGroup(showGroupsDesc sG) {
    this.sGD = sG;
  }

  public createUserDesc getCreateUser() {
    return createUser;
  }

  public void setCreateUser(createUserDesc createUser) {
    this.createUser = createUser;
  }

  public createRoleDesc getCreateRole() {
    return createRole;
  }

  public void setCreateRole(createRoleDesc createRole) {
    this.createRole = createRole;
  }

  public dropRoleDesc getDropRole() {
    return dropRole;
  }

  public void setDropRole(dropRoleDesc dropRole) {
    this.dropRole = dropRole;
  }

  public dropUserDesc getDropUser() {
    return dropUser;
  }

  public void setDropUser(dropUserDesc dropUser) {
    this.dropUser = dropUser;
  }

  public grantPrisDesc getGrantPris() {
    return grantPris;
  }

  public void setGrantPris(grantPrisDesc grantPris) {
    this.grantPris = grantPris;
  }

  public grantRoleDesc getGrantRole() {
    return grantRole;
  }

  public void setGrantRole(grantRoleDesc grantRole) {
    this.grantRole = grantRole;
  }

  public setPwdDesc getSetPwd() {
    return setPwd;
  }

  public void setSetPwd(setPwdDesc setPwd) {
    this.setPwd = setPwd;
  }

  public showGrantsDesc getShowGrants() {
    return showGrants;
  }

  public void setShowGrants(showGrantsDesc showGrants) {
    this.showGrants = showGrants;
  }

  public showUsersDesc getShowUsers() {
    return showUsers;
  }

  public void setShowUsers(showUsersDesc showUsers) {
    this.showUsers = showUsers;
  }

  public revokePriDesc getRevokePri() {
    return revokePri;
  }

  public void setRevokePri(revokePriDesc revokePri) {
    this.revokePri = revokePri;
  }

  public revokeRoleDesc getRevokeRole() {
    return revokeRole;
  }

  public void setRevokeRole(revokeRoleDesc revokeRole) {
    this.revokeRole = revokeRole;
  }

}
