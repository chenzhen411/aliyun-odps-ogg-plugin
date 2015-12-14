/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.odps.ogg.handler.datahub;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.ogg.handler.datahub.alarm.LogAlarm;
import com.aliyun.odps.ogg.handler.datahub.alarm.OggAlarm;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandler;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationTypes;
import com.aliyun.odps.tunnel.TableTunnel;
import com.goldengate.atg.datasource.*;
import com.goldengate.atg.datasource.GGDataSource.Status;
import com.goldengate.atg.datasource.adapt.Op;
import com.goldengate.atg.datasource.meta.DsMetaData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class DatahubHandler extends AbstractHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(DatahubHandler.class);

    private String alarmImplement = LogAlarm.class.getName();

    // Focus cols, comma seperated
    private String focusFields;
    private String keyFields;

    private String tableMap;

    // Table related
    private String project;
    private String tableName;
    private String accessID;
    private String accessKey;
    private String endPoint = "http://service.odps.aliyun.com/api";
    private String datahubEndPoint = "http://dh.odps.aliyun.com";
    private long shardNumber = 1;
    private int shardTimeout = 60;
    private int hubLifeCycle = 7;
    private String timestampField;

    private String dbDateFormat = "yyyy-MM-dd HH:mm:ss";
    private String timestampDateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private String partitionValues;
    private String partitionFields;

    private int batchSize = 1000;
    private int retryCount = 3;
    private int passFailedOperationCount = 0;
    private String failedOperationFileName;

    private HandlerProperties handlerProperties;
    private Odps odps;
    private Table odpsTable;
    private OggAlarm oggAlarm;

    private int sentBatchCount;
    private int sentPartitionCount;

    private String handlerInfoFileName;
    private boolean isBatchCountChanged = false;
    private int badRecordCount;

    @Override
    public void init(DsConfiguration dsConf, DsMetaData dsMeta) {
        super.init(dsConf, dsMeta);


//        if (StringUtils.isEmpty(handlerInfoFileName)) {
//            handlerInfoFileName = "dirdat/" + getName() + "_handler_info";
//        }
        badRecordCount = 0;
        if (StringUtils.isEmpty(failedOperationFileName)) {
            failedOperationFileName = "dirrpt/" + getName() + "_bad_operations.data";
        }

        buildOdps();
        initProperty();
//        loadCountInfo();
    }

//    private void loadCountInfo() {
//        batchEventCount = 0;
//        currentBatchCount = 0;
//        File handlerInfoFile = new File(handlerInfoFileName);
//        if (handlerInfoFile.exists() && !handlerInfoFile.isDirectory()) {
//            DataInputStream in = null;
//            try {
//                in = new DataInputStream(new FileInputStream(handlerInfoFile));
//                sentBatchCount = in.readInt();
//                sentPartitionCount = in.readInt();
//            } catch (IOException e) {
//                logger.warn("Error reading handler info file, may cause duplication ", e);
//                oggAlarm.warn("Error reading handler info file, may cause duplication ", e);
//            } finally {
//                if (in != null) {
//                    try {
//                        in.close();
//                    } catch (IOException e) {
//                        logger.warn("Close handler info file failed. ", e);
//                        oggAlarm.warn("Close handler info file failed. ", e);
//                    }
//                }
//            }
//        } else {
//            sentBatchCount = 0;
//            sentPartitionCount = 0;
//        }
//    }

    private void buildOdps() {
        Account account = new AliyunAccount(accessID, accessKey);
        odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endPoint);
    }

    private Map<String, String> buildMap(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        // "key1/value1,key2/value2,..."
        // all to lower case
        Map<String, String> map = Maps.newHashMap();
        String[] array = str.split(",");
        for(String s: array) {
            String[] pair = s.split("/");
            map.put(pair[0].trim().toLowerCase(), pair[1].trim().toLowerCase());
        }
        return map;
    }

    private Map<String, Set<String>> buildStringSetMap(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        // "table1:name1/type1,name2/type2|table2:name3/type3|...
        // all to lower case
        Map<String, Set<String>> map = Maps.newHashMap();
        String[] tableInfos = str.split("\\|");
        for (String tableInfo: tableInfos) {
            String[] nameList = tableInfo.split(":");
            String name = nameList[0].trim().toLowerCase();
            Set<String> valSet = new HashSet<>();
            for (String s: nameList[1].split(",")) {
                valSet.add(s.split("/")[0].trim().toLowerCase());
            }
            map.put(name, valSet);
        }
        return map;
    }

    private void initProperty() {
        handlerProperties = new HandlerProperties();

        handlerProperties.setOracleOdpsTableMap(buildMap(tableMap));
        handlerProperties.setTableKeysMap(buildStringSetMap(keyFields));
        handlerProperties.setTableFocusMap(buildStringSetMap(focusFields));

        // Set custom alarm...
        try {
            oggAlarm = (OggAlarm) Class.forName(alarmImplement).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("Cannot initialize custom OGG alarm. ", e);
        }
        handlerProperties.setOggAlarm(oggAlarm);

        // Set properties...
        handlerProperties.setTimestampField(timestampField);
        handlerProperties.setSimpleDateFormat(new SimpleDateFormat(timestampDateFormat));

        // Build partition...
        List<String> partitionCols = getPropCols(partitionFields);
        List<String> partitionVals = getPropCols(partitionValues);

        // TODO partition map
        if (partitionCols != null) {
            if (partitionVals != null & partitionCols.size() == partitionVals.size()) {
                handlerProperties.setPartitionCols(partitionCols);
                handlerProperties.setPartitionVals(partitionVals);
            } else {
                logger.error("Number of partition cols and vals are not consistent.");
                oggAlarm.error("Number of partition cols and vals are not consistent.");
                throw new RuntimeException("Number of partition cols and vals are not consistent.");
            }
        }

        // Other properties
        handlerProperties.setProject(project);
        handlerProperties.setShardNumber(shardNumber);
        handlerProperties.setOdpsTables(odps.tables());
        handlerProperties.setShardTimeout(shardTimeout);
        handlerProperties.setBatchSize(batchSize);
        handlerProperties.setRetryCount(retryCount);
        handlerProperties.setDbDateFormat(dbDateFormat);
        handlerProperties.setMetaFieldName("meta");

        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(datahubEndPoint);
        handlerProperties.setTunnel(tunnel);
    }


    private List<String> getPropCols(String propertyString) {
        if (StringUtils.isEmpty(propertyString)) {
            logger.warn("Property is empty. property name:" + propertyString);
            oggAlarm.warn("Property is empty. property name:" + propertyString);
            return null;
        }
        List<String> propList = Lists.newArrayList();
        String[] propCols = propertyString.split(",");
        for (int i = 0; i < propCols.length; i++) {
            propList.add(propCols[i].split("/")[0].trim().toLowerCase());
        }
        return propList;
    }

    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        return super.metaDataChanged(e, meta);
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        //currentBatchCount = 0;
        return super.transactionBegin(e, tx);
    }


    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
        Status status = Status.OK;
        super.operationAdded(e, tx, dsOperation);

        Op op = new Op(dsOperation, getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        // Get corresponding operationHandler
        OperationTypes operationType = OperationTypes.valueOf(dsOperation.getOperationType().toString());
        OperationHandler operationHandler = operationType.getOperationHandler();
        if (operationHandler != null) {
            try {
                operationHandler.process(op, handlerProperties);
                handlerProperties.totalOperations++;
            } catch (Exception e1) {
                if (e1 instanceof OperationHandler.OdpsTableNotSetException) {
                    status = Status.OK;
                    logger.warn(e1.getMessage());
                    oggAlarm.warn(e1.getMessage());
                }else if (e1 instanceof OdpsWriter.WriteHubRetryFailedException) {
                    status = Status.ABEND;
                    oggAlarm.error("Failed after retry", e1);
                    logger.error("Failed after retry", e1);
                } else {
                    String msg = "Unable to process operation.";
                    if (badRecordCount < passFailedOperationCount) {
                        status = Status.OK;
                        badRecordCount++;
                        logBadOperation(op, e1.getMessage());
                        oggAlarm.warn(msg, e1);
                        logger.warn(msg, e1);
                    } else {
                        oggAlarm.error(msg, e1);
                        logger.error(msg, e1);
                        status = Status.ABEND;
                    }
                }
            }
        } else {
            String msg = "Unable to instantiate operation handler. Transaction ID: " + tx.getTranID()
                    + ". Operation type: " + dsOperation.getOperationType().toString();
            if (badRecordCount < passFailedOperationCount) {
                status = Status.OK;
                badRecordCount++;
                logBadOperation(op, msg);
                oggAlarm.warn(msg);
                logger.warn(msg);
            } else {
                oggAlarm.error(msg);
                logger.error(msg);
                status = Status.ABEND;
            }
        }
        return status;
    }

    private void logBadOperation(Op op, String msg) {
        StringBuilder sb = new StringBuilder();
        String sep = "\n";
        sb.append("Bad Operation...").append("\n");
        sb.append("Operation Read Time: ").append(op.getOperation().getReadTimeAsString()).append(".\n");
        sb.append("Error msg: ").append(msg).append(".\n");
        sb.append("Full table name: ").append(op.getTableName().getFullName()).append(".\n");
        sb.append("Operation type: ").append(op.getOperationType().toString()).append(".\n");
        sb.append("Operation detail: ").append("\n");

        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            sb.append(sep).append(colName).append(":'").append(cols.get(i).getBeforeValue()).append("' -> '").append(cols.get(i).getAfterValue()).append("'");
        }
        sb.append("\n\n");
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(failedOperationFileName, true));
            bw.write(sb.toString());
            bw.close();
        } catch (IOException e) {
            logger.error("logBadOperation() failed. ", e);
            oggAlarm.error("logBadOperation() failed. ", e);
            throw new RuntimeException("logBadOperation() failed. ", e);
        }
    }

    @Override
    public Status transactionCommit(DsEvent e, DsTransaction tx) {
        Status status = super.transactionCommit(e, tx);
        for(OdpsWriter writer: handlerProperties.getTableWriterMap().values()) {
            try {
                writer.flushAll();
            } catch (Exception e1) {
                status = status.ABEND;
                oggAlarm.error("Unable to deliver records", e1);
                logger.error("Unable to deliver records", e1);
                // TODO: record which table
                //updateHandlerInfo();
            }
        }
        handlerProperties.totalTxns++;
        return status;
    }


    @Override
    public String reportStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append(":- Status report: mode=").append(getMode());
        sb.append(", transactions=").append(handlerProperties.totalTxns);
        sb.append(", operations=").append(handlerProperties.totalOperations);
        sb.append(", inserts=").append(handlerProperties.totalInserts);
        sb.append(", updates=").append(handlerProperties.totalUpdates);
        sb.append(", deletes=").append(handlerProperties.totalDeletes);

        return sb.toString();
    }

    @Override
    public void destroy() {
        oggAlarm.warn("Handler destroying...");
        super.destroy();
    }
//
//    private void updateHandlerInfo() {
//        int writerCachePartitionCount = odpsWriter.getSentPartitionCount();
//        if (writerCachePartitionCount == sentPartitionCount && !isBatchCountChanged) {
//            return;
//        }
//        sentPartitionCount = writerCachePartitionCount;
//        DataOutputStream out = null;
//        try {
//            out = new DataOutputStream(new FileOutputStream(handlerInfoFileName, false));
//            out.writeInt(sentBatchCount);
//            out.writeInt(sentPartitionCount);
//        } catch (IOException e) {
//            oggAlarm.error("Error writing handler info file. sentBatchCount=" + sentBatchCount
//                    + ", sentPartitionCount=" + sentPartitionCount + ".", e);
//            logger.error("Error writing handler info file. sentBatchCount=" + sentBatchCount
//                    + ", sentPartitionCount=" + sentPartitionCount + ".", e);
//        } finally {
//            if (out != null) {
//                try {
//                    out.close();
//                } catch (IOException e) {
//                    oggAlarm.error("Close handler info file failed. sentBatchCount=" + sentBatchCount
//                            + ", sentPartitionCount=" + sentPartitionCount + ".", e);
//                    logger.error("Close handler info file failed. sentBatchCount=" + sentBatchCount
//                            + ", sentPartitionCount=" + sentPartitionCount + ".", e);
//                }
//            }
//        }
//    }

    public String getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(String partitionValues) {
        this.partitionValues = partitionValues;
    }

    public String getFocusFields() {
        return focusFields;
    }

    public void setFocusFields(String focusFields) {
        this.focusFields = focusFields;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAccessID() {
        return accessID;
    }

    public void setAccessID(String accessID) {
        this.accessID = accessID;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getDatahubEndPoint() {
        return datahubEndPoint;
    }

    public void setDatahubEndPoint(String datahubEndPoint) {
        this.datahubEndPoint = datahubEndPoint;
    }

    public long getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(long shardNumber) {
        this.shardNumber = shardNumber;
    }

    public int getShardTimeout() {
        return shardTimeout;
    }

    public void setShardTimeout(int shardTimeout) {
        this.shardTimeout = shardTimeout;
    }

    public String getPartitionFields() {
        return partitionFields;
    }

    public void setPartitionFields(String partitionFields) {
        this.partitionFields = partitionFields;
    }

    public String getKeyFields() {
        return keyFields;
    }

    public void setKeyFields(String keyFields) {
        this.keyFields = keyFields;
    }

    public int getHubLifeCycle() {
        return hubLifeCycle;
    }

    public void setHubLifeCycle(int hubLifeCycle) {
        this.hubLifeCycle = hubLifeCycle;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public String getTimestampDateFormat() {
        return timestampDateFormat;
    }

    public void setTimestampDateFormat(String timestampDateFormat) {
        this.timestampDateFormat = timestampDateFormat;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
    }

    public String getDbDateFormat() {
        return dbDateFormat;
    }

    public void setDbDateFormat(String dbDateFormat) {
        this.dbDateFormat = dbDateFormat;
    }

    public String getAlarmImplement() {
        return alarmImplement;
    }

    public void setAlarmImplement(String alarmImplement) {
        this.alarmImplement = alarmImplement;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getPassFailedOperationCount() {
        return passFailedOperationCount;
    }

    public void setPassFailedOperationCount(int passFailedOperationCount) {
        this.passFailedOperationCount = passFailedOperationCount;
    }

    public String getFailedOperationFileName() {
        return failedOperationFileName;
    }

    public void setFailedOperationFileName(String failedOperationFileName) {
        this.failedOperationFileName = failedOperationFileName;
    }

    public String getHandlerInfoFileName() {
        return handlerInfoFileName;
    }

    public void setHandlerInfoFileName(String handlerInfoFileName) {
        this.handlerInfoFileName = handlerInfoFileName;
    }

    public String getTableMap() {
        return tableMap;
    }

    public void setTableMap(String tableMap) {
        this.tableMap = tableMap;
    }
}
