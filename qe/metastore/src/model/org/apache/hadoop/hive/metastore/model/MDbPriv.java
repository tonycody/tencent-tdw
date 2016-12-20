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
package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.model.MUser;

public class MDbPriv {

  private MDatabase db;
  private String user;

  private boolean select_priv;
  private boolean insert_priv;
  private boolean index_priv;
  private boolean create_priv;
  private boolean drop_priv;
  private boolean delete_priv;
  private boolean alter_priv;
  private boolean update_priv;
  private boolean createview_priv;
  private boolean showview_priv;

  public MDbPriv() {

  }

  public MDbPriv(MDatabase db, String user) {
    this.db = db;
    this.user = user;

    this.select_priv = false;
    this.insert_priv = false;
    this.index_priv = false;
    this.create_priv = false;
    this.drop_priv = false;
    this.delete_priv = false;
    this.alter_priv = false;
    this.update_priv = false;
    this.createview_priv = false;
    this.showview_priv = false;

  }

  public MDbPriv(MDatabase db, String user, boolean select_priv,
      boolean insert_priv, boolean index_priv, boolean create_priv,
      boolean drop_priv, boolean delete_priv, boolean alter_priv,
      boolean update_priv, boolean createview_priv, boolean showview_priv) {
    this.db = db;
    this.user = user;

    this.select_priv = select_priv;
    this.insert_priv = insert_priv;
    this.index_priv = index_priv;
    this.create_priv = create_priv;
    this.drop_priv = drop_priv;
    this.delete_priv = delete_priv;
    this.alter_priv = alter_priv;
    this.update_priv = update_priv;
    this.createview_priv = createview_priv;
    this.showview_priv = showview_priv;

  }

  public void setDb(MDatabase db) {
    this.db = db;
  }

  public MDatabase getDb() {
    return this.db;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getUser() {
    return this.user;
  }

  public void setSelect_priv(boolean select_priv) {
    this.select_priv = select_priv;
  }

  public boolean getSelect_priv() {
    return this.select_priv;
  }

  public void setInsert_priv(boolean insert_priv) {
    this.insert_priv = insert_priv;
  }

  public boolean getInsert_priv() {
    return this.insert_priv;
  }

  public void setIndex_priv(boolean index_priv) {
    this.index_priv = index_priv;
  }

  public boolean getIndex_priv() {
    return this.index_priv;
  }

  public void setCreate_priv(boolean create_priv) {
    this.create_priv = create_priv;
  }

  public boolean getCreate_priv() {
    return this.create_priv;
  }

  public void setDrop_priv(boolean drop_priv) {
    this.drop_priv = drop_priv;
  }

  public boolean getDrop_priv() {
    return this.drop_priv;
  }

  public void setDelete_priv(boolean delete_priv) {
    this.delete_priv = delete_priv;
  }

  public boolean getDelete_priv() {
    return this.delete_priv;
  }

  public void setAlter_priv(boolean alter_priv) {
    this.alter_priv = alter_priv;
  }

  public boolean getAlter_priv() {
    return this.alter_priv;
  }

  public void setUpdate_priv(boolean update_priv) {
    this.update_priv = update_priv;
  }

  public boolean getUpdate_priv() {
    return this.update_priv;
  }

  public void setCreateview_priv(boolean createview_priv) {
    this.createview_priv = createview_priv;
  }

  public boolean getCreateview_priv() {
    return this.createview_priv;
  }

  public void setShowview_priv(boolean showview_priv) {
    this.showview_priv = showview_priv;
  }

  public boolean getShowview_priv() {
    return this.showview_priv;
  }

}
