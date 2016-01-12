package com.aliyun.odps.ogg.datahub.table;

import com.aliyun.odps.*;
import com.aliyun.odps.task.SQLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by tianli on 15/12/14.
 */
public class TableParams {
  private final static Logger logger = LoggerFactory.getLogger(TableParams.class);

  private String projectName;
  private String tableName;
  private Map<String, OdpsType> colTypeMap;
  private List<String> partitionCols;
  private long shardNumber;
  private long hubLifeCycle;

  public void doCreate(Tables tables) {
    try {
      if (tables.exists(tableName)) {
        logger.error("Create table failed. Table already exists.");
        throw new RuntimeException("Create table failed. Table already exists.");
      }
    } catch (OdpsException e) {
      logger.error("Check table existence failed. ", e);
      throw new RuntimeException("Check table existence failed. ", e);
    }
    TableSchema schema = new TableSchema();
    for (Map.Entry<String, OdpsType> colTypeEntry : colTypeMap.entrySet()) {
      schema.addColumn(new Column(colTypeEntry.getKey(), colTypeEntry.getValue()));
    }

    if (partitionCols != null && partitionCols.size() > 0) {
      for (String p : partitionCols) {
        schema.addPartitionColumn(new Column(p, OdpsType.STRING));
      }
    }

    try {
      tables.create(projectName, tableName, schema);
      tables.get(tableName).createShards(shardNumber, true, hubLifeCycle);
    } catch (OdpsException e) {
      logger.error("Create table failed. ", e);
      throw new RuntimeException("Create table failed. ", e);
    }
  }

  public void doDrop(Tables tables) {
    try {
      tables.delete(tableName);
    } catch (OdpsException e) {
      logger.error("Drop table failed. ", e);
      throw new RuntimeException("Drop table failed. ", e);
    }
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, OdpsType> getColTypeMap() {
    return colTypeMap;
  }

  public void setColTypeMap(Map<String, OdpsType> colTypeMap) {
    this.colTypeMap = colTypeMap;
  }

  public List<String> getPartitionCols() {
    return partitionCols;
  }

  public void setPartitionCols(List<String> partitionCols) {
    this.partitionCols = partitionCols;
  }

  public long getHubLifeCycle() {
    return hubLifeCycle;
  }

  public void setHubLifeCycle(long hubLifeCycle) {
    this.hubLifeCycle = hubLifeCycle;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public long getShardNumber() {
    return shardNumber;
  }

  public void setShardNumber(long shardNumber) {
    this.shardNumber = shardNumber;
  }
}
