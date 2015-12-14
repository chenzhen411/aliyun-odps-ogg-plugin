package com.aliyun.odps.ogg.handler.datahub.dataobject;

import java.util.List;
import java.util.Set;

/**
 * Created by tianli on 15/12/13.
 */
public class RecordMeta {
  private String dbsync_ts;
  private String dbsync_db_name;
  private String dbsync_table_name;
  private String dbsync_modify_time;
  private String dbsync_opertion;
  private Set<String> dbsync_keys;

  public String getDbsync_ts() {
    return dbsync_ts;
  }

  public void setDbsync_ts(String dbsync_ts) {
    this.dbsync_ts = dbsync_ts;
  }

  public String getDbsync_db_name() {
    return dbsync_db_name;
  }

  public void setDbsync_db_name(String dbsync_db_name) {
    this.dbsync_db_name = dbsync_db_name;
  }

  public String getDbsync_table_name() {
    return dbsync_table_name;
  }

  public void setDbsync_table_name(String dbsync_table_name) {
    this.dbsync_table_name = dbsync_table_name;
  }

  public String getDbsync_modify_time() {
    return dbsync_modify_time;
  }

  public void setDbsync_modify_time(String dbsync_modify_time) {
    this.dbsync_modify_time = dbsync_modify_time;
  }

  public String getDbsync_opertion() {
    return dbsync_opertion;
  }

  public void setDbsync_opertion(String dbsync_opertion) {
    this.dbsync_opertion = dbsync_opertion;
  }

  public Set<String> getDbsync_keys() {
    return dbsync_keys;
  }

  public void setDbsync_keys(Set<String> dbsync_keys) {
    this.dbsync_keys = dbsync_keys;
  }
}
