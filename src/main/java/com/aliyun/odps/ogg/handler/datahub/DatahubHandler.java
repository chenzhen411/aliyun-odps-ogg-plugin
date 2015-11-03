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
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.ogg.handler.datahub.alarm.LogAlarm;
import com.aliyun.odps.ogg.handler.datahub.alarm.OggAlarm;
import com.aliyun.odps.ogg.handler.datahub.dataobject.OdpsRowDO;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationHandler;
import com.aliyun.odps.ogg.handler.datahub.operations.OperationTypes;
import com.aliyun.odps.ogg.handler.datahub.utils.ColNameUtil;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamWriter;
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
import java.util.concurrent.TimeoutException;

public class DatahubHandler extends AbstractHandler {
    private final static Logger logger = LoggerFactory
            .getLogger(DatahubHandler.class);

    private String alarmImplement = LogAlarm.class.getName();

    // Operation type & time
    private String operTypeField;
    private String operTimeField;

    // Focus cols, comma seperated
    private String focusFields;
    private String keyFields;

    // Table related
    private String project;
    private String tableName;
    private String accessID;
    private String accessKey;
    private String endPoint = "http://service.odps.aliyun.com/api";
    private String datahubEndPoint = "http://dh.odps.aliyun.com";
    private int shardNumber = 1;
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
    private OdpsWriter odpsWriter;
    private Odps odps;
    private Table odpsTable;
    private OggAlarm oggAlarm;

    private int batchEventCount;
    private int currentBatchCount;
    private int sentBatchCount;
    private int sentPartitionCount;

    private String handlerInfoFileName;
    private boolean isBatchCountChanged = false;
    private int badRecordCount;

    @Override
    public void init(DsConfiguration dsConf, DsMetaData dsMeta) {
        super.init(dsConf, dsMeta);
        if (StringUtils.isEmpty(handlerInfoFileName)) {
            handlerInfoFileName = "dirdat/" + getName() + "_handler_info";
        }
        if (StringUtils.isEmpty(failedOperationFileName)) {
            failedOperationFileName = "dirrpt/" + getName() + "_bad_operations.data";
        }
        badRecordCount = 0;
        buildOdps();
        initProperty();
        loadCountInfo();
    }

    private void loadCountInfo() {
        batchEventCount = 0;
        currentBatchCount = 0;
        File handlerInfoFile = new File(handlerInfoFileName);
        if (handlerInfoFile.exists() && !handlerInfoFile.isDirectory()) {
            DataInputStream in = null;
            try {
                in = new DataInputStream(new FileInputStream(handlerInfoFile));
                sentBatchCount = in.readInt();
                sentPartitionCount = in.readInt();
            } catch (IOException e) {
                logger.warn("Error reading handler info file, may cause duplication ", e);
                oggAlarm.warn("Error reading handler info file, may cause duplication ", e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn("Close handler info file failed. ", e);
                        oggAlarm.warn("Close handler info file failed. ", e);
                    }
                }
            }
        } else {
            sentBatchCount = 0;
            sentPartitionCount = 0;
        }
    }

    private void buildOdps() {
        Account account = new AliyunAccount(accessID, accessKey);
        odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endPoint);
        odpsTable = odps.tables().get(tableName);
    }

    private void initProperty() {
        handlerProperties = new HandlerProperties();
        // Set custom alarm...
        try {
            oggAlarm = (OggAlarm) Class.forName(alarmImplement).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException("Cannot initialize custom OGG alarm. ", e);
        }
        handlerProperties.setOggAlarm(oggAlarm);

        // Set properties...
        handlerProperties.setOperTimeField(operTimeField);
        handlerProperties.setOperTypeField(operTypeField);
        handlerProperties.setTimestampField(timestampField);
        handlerProperties.setSimpleDateFormat(new SimpleDateFormat(timestampDateFormat));
        handlerProperties.setOdpsTable(odpsTable);

        // Build focus cols
        String[] focusCols = getPropCols(focusFields);
        Map<String, Boolean> focusColMap = buildColMap(focusCols);
        handlerProperties.setFocusColMap(focusColMap);

        // Build key cols
        String[] keyCols = getPropCols(keyFields);
        Map<String, Boolean> keyColMap = buildColMap(keyCols);
        handlerProperties.setKeyColMap(keyColMap);

        // Init rowDOs
        List<OdpsRowDO> rowDOs = Lists.newLinkedList();
        handlerProperties.setOdpsRowDOs(rowDOs);

        // Build input columns
        List<String> inputColNames = Lists.newLinkedList();
        if (StringUtils.isNotEmpty(operTimeField)) {
            inputColNames.add(operTimeField);
        }
        if (StringUtils.isNotEmpty(operTypeField)) {
            inputColNames.add(operTypeField);
        }
        if (keyCols != null) {
            for (String s : keyCols) {
                inputColNames.add(s);
            }
        }
        if (focusCols != null) {
            for (String s : focusCols) {
                inputColNames.add(ColNameUtil.getBeforeName(s));
                inputColNames.add(ColNameUtil.getAfterName(s));
            }
        }
        handlerProperties.setInputColNames(inputColNames);

        // Build partition...
        String[] partitionCols = getPropCols(partitionFields);
        String[] partitionVals = getPropCols(partitionValues);

        if (partitionCols != null) {
            if (partitionVals != null & partitionCols.length == partitionVals.length) {
                handlerProperties.setPartitionCols(partitionCols);
                handlerProperties.setPartitionVals(partitionVals);
                handlerProperties.setPartitionMap(buildPartitionMap());
            } else {
                logger.error("Number of partition cols and vals are not consistent.");
                oggAlarm.error("Number of partition cols and vals are not consistent.");
                throw new RuntimeException("Number of partition cols and vals are not consistent.");
            }
        }

        // RecordBuilder
        handlerProperties.setRecordBuilder(initRecordBuilder());

        // OdpsWriter
        try {
            odpsWriter = buildOdpsWriter();
        } catch (TunnelException | IOException | TimeoutException e) {
            logger.error("Initialize OdpsWriter failed.", e);
            oggAlarm.error("Initialize OdpsWriter failed.", e);
            throw new RuntimeException("Initialize OdpsWriter failed.", e);
        }
    }

    private RecordBuilder initRecordBuilder() {
        return new RecordBuilder(odpsTable, dbDateFormat, handlerProperties.getInputColNames());
    }

    private String[] getPropCols(String propertyString) {
        if (StringUtils.isEmpty(propertyString)) {
            logger.warn("Property is empty. property name:" + propertyString);
            oggAlarm.warn("Property is empty. property name:" + propertyString);
            return null;
        }
        String[] propCols = propertyString.split(",");
        for (int i = 0; i < propCols.length; i++) {
            propCols[i] = propCols[i].split("/")[0].trim();
        }
        return propCols;
    }

    private Map buildPartitionMap() {
        Map partitionMap = Maps.newHashMap();
        for (Partition partition : odpsTable.getPartitions()) {
            partitionMap.put(partition.getPartitionSpec().toString(), true);
        }
        return partitionMap;
    }

    private OdpsWriter buildOdpsWriter() throws TunnelException, IOException, TimeoutException {
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.setEndpoint(datahubEndPoint);
        StreamClient streamClient = tunnel.createStreamClient(project, tableName);
        streamClient.loadShard(shardNumber);
        StreamWriter[] streamWriters = buildStreamWriters(streamClient);
        return new OdpsWriter(odpsTable, streamWriters, sentPartitionCount, retryCount);
    }

    // Wait for loading shard, default timeout: 60s
    private StreamWriter[] buildStreamWriters(StreamClient streamClient) throws IOException, TunnelException,
            TimeoutException {
        StreamWriter[] streamWriters;
        final StreamClient.ShardState finish = StreamClient.ShardState.LOADED;
        long now = System.currentTimeMillis();
        long endTime = now + shardTimeout * 1000;
        List<Long> shardIDList = null;
        while (now < endTime) {
            HashMap<Long, StreamClient.ShardState> shardStatus = streamClient
                    .getShardStatus();
            shardIDList = new ArrayList<Long>();
            Set<Long> keys = shardStatus.keySet();
            Iterator<Long> iter = keys.iterator();
            while (iter.hasNext()) {
                Long key = iter.next();
                StreamClient.ShardState value = shardStatus.get(key);
                if (value.equals(finish)) {
                    shardIDList.add(key);
                }
            }
            now = System.currentTimeMillis();
            if (shardIDList.size() == shardNumber) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // DO NOTHING...
            }
        }
        if (shardIDList != null && shardIDList.size() > 0) {
            streamWriters = new StreamWriter[shardIDList.size()];
            for (int i = 0; i < shardIDList.size(); i++) {
                streamWriters[i] = streamClient.openStreamWriter(shardIDList.get(i));
            }
        } else {
            oggAlarm.error("buildStreamWriters() error, have no loaded shards.");
            throw new TimeoutException("buildStreamWriters() error, have no loaded shards.");
        }
        return streamWriters;
    }

    private Map<String, Boolean> buildColMap(String[] cols) {
        if (cols == null) {
            return null;
        }
        Map<String, Boolean> colMap = Maps.newHashMap();
        for (String c : cols) {
            colMap.put(c, true);
        }
        return colMap;
    }

    @Override
    public Status metaDataChanged(DsEvent e, DsMetaData meta) {
        return super.metaDataChanged(e, meta);
    }

    @Override
    public Status transactionBegin(DsEvent e, DsTransaction tx) {
        currentBatchCount = 0;
        return super.transactionBegin(e, tx);
    }

    @Override
    public Status operationAdded(DsEvent e, DsTransaction tx, DsOperation dsOperation) {
        Status status = Status.OK;
        super.operationAdded(e, tx, dsOperation);
        // If already sent batch, update counters
        if (currentBatchCount < sentBatchCount) {
            batchEventCount++;
            if (batchEventCount == batchSize) {
                batchEventCount = 0;
                currentBatchCount++;
            }
            return status;
        }

        // If haven't sent batch, do processing
        Op op = new Op(dsOperation, getMetaData().getTableMetaData(dsOperation.getTableName()), getConfig());
        // Get corresponding operationHandler
        OperationTypes operationType = OperationTypes.valueOf(dsOperation.getOperationType().toString());
        OperationHandler operationHandler = operationType.getOperationHandler();
        if (operationHandler != null) {
            try {
                operationHandler.process(op, handlerProperties);
                batchEventCount++;
                // If batch is full, send batch
                if (batchEventCount == batchSize) {
                    status = sendEvents();
                    if (status == Status.OK) {
                        batchEventCount = 0;
                        currentBatchCount++;
                        sentBatchCount++;
                        isBatchCountChanged = true;
                    }
                    updateHandlerInfo();
                }
                handlerProperties.totalOperations++;
            } catch (Exception e1) {
                String msg = "Unable to process operation.";
                if (badRecordCount < passFailedOperationCount) {
                    status = Status.OK;
                    badRecordCount++;
                    logBadOperation(op, e1.toString());
                    oggAlarm.warn(msg, e1);
                    logger.warn(msg, e1);
                } else {
                    oggAlarm.error(msg, e1);
                    logger.error(msg, e1);
                    status = Status.ABEND;
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
        String sep = "";
        sb.append("Operation Read Time: ").append(op.getOperation().getReadTimeAsString()).append(". Error msg: ")
                .append(msg).append(". Operation key: ");
        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            Map<String, Boolean> keyColMap = handlerProperties.getKeyColMap();
            if (keyColMap != null && keyColMap.containsKey(colName)) {
                sb.append(sep).append(colName).append("=").append(cols.get(i).getAfterValue());
                sep = ",";
            }
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
        super.transactionCommit(e, tx);
        Status status = sendEvents();
        if (status == Status.OK) {
            batchEventCount = 0;
            if (sentBatchCount != 0) {
                sentBatchCount = 0;
                isBatchCountChanged = true;
            }
        }
        updateHandlerInfo();
        handlerProperties.totalTxns++;
        return status;
    }

    private Status sendEvents() {
        Status status = Status.OK;
        List<OdpsRowDO> rowDOs = handlerProperties.getOdpsRowDOs();

        if (rowDOs != null && rowDOs.size() > 0) {
            try {
                odpsWriter.write(rowDOs);
                rowDOs.clear();
            } catch (Exception e) {
                oggAlarm.error("Unable to deliver events, event size: " + rowDOs.size(), e);
                logger.error("Unable to deliver events, event size: " + rowDOs.size(), e);
                status = Status.ABEND;
            }
        } else {
            oggAlarm.warn("No records available to send.");
            logger.warn("No records available to send.");
        }
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

    private void updateHandlerInfo() {
        int writerCachePartitionCount = odpsWriter.getSentPartitionCount();
        if (writerCachePartitionCount == sentPartitionCount && !isBatchCountChanged) {
            return;
        }
        sentPartitionCount = writerCachePartitionCount;
        DataOutputStream out = null;
        try {
            out = new DataOutputStream(new FileOutputStream(handlerInfoFileName, false));
            out.writeInt(sentBatchCount);
            out.writeInt(sentPartitionCount);
        } catch (IOException e) {
            oggAlarm.error("Error writing handler info file. sentBatchCount=" + sentBatchCount
                    + ", sentPartitionCount=" + sentPartitionCount + ".", e);
            logger.error("Error writing handler info file. sentBatchCount=" + sentBatchCount
                    + ", sentPartitionCount=" + sentPartitionCount + ".", e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    oggAlarm.error("Close handler info file failed. sentBatchCount=" + sentBatchCount
                            + ", sentPartitionCount=" + sentPartitionCount + ".", e);
                    logger.error("Close handler info file failed. sentBatchCount=" + sentBatchCount
                            + ", sentPartitionCount=" + sentPartitionCount + ".", e);
                }
            }
        }
    }

    public String getOperTypeField() {
        return operTypeField;
    }

    public void setOperTypeField(String operTypeField) {
        this.operTypeField = operTypeField;
    }

    public String getOperTimeField() {
        return operTimeField;
    }

    public void setOperTimeField(String operTimeField) {
        this.operTimeField = operTimeField;
    }

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

    public int getShardNumber() {
        return shardNumber;
    }

    public void setShardNumber(int shardNumber) {
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
}
