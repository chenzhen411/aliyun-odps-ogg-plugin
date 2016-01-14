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
package com.aliyun.odps.ogg.handler.datahub.operations;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.ogg.handler.datahub.OdpsWriter;
import com.aliyun.odps.ogg.handler.datahub.dataobject.RecordMeta;
import com.aliyun.odps.ogg.handler.datahub.utils.BucketPath;
import com.aliyun.odps.ogg.handler.datahub.utils.OdpsUtils;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.ogg.handler.datahub.HandlerProperties;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.google.common.collect.Maps;

public abstract class OperationHandler {

    private final static Logger logger = LoggerFactory.getLogger(OperationHandler.class);

    public abstract void process(Op op, HandlerProperties handlerProperties)
            throws Exception;

    public static class OdpsTableNotSetException extends RuntimeException {
        public OdpsTableNotSetException(String msg) {
            super(msg);
        }
    }

    protected void processOperation(Op op, HandlerProperties handlerProperties) throws ParseException, TunnelException, IOException, TimeoutException {
        String fullTableName = op.getTableName().getFullName().toLowerCase();
        OdpsWriter odpsWriter = getOdpsWriter(handlerProperties, fullTableName);

        Map<String, String> rowMap = Maps.newHashMap();
        // Get meta
//        RecordMeta meta = new RecordMeta();
//        // TODO check timestamp
//        meta.setDbsync_ts(op.getOperation().getReadTimeAsString());
//        meta.setDbsync_db_name(op.getTableName().getSchemaName());
//        meta.setDbsync_table_name(op.getTableName().getShortName());
//        meta.setDbsync_modify_time(op.getTimestamp());
//        meta.setDbsync_opertion(op.getOperationType().fullString());
//        meta.setDbsync_keys(handlerProperties.getTableKeysMap().get(fullTableName));
//
//        Gson gson = new Gson();
//        rowMap.put(handlerProperties.getMetaFieldName(), gson.toJson(meta));
        rowMap.put(handlerProperties.getOpTypeFieldName(), op.getOperationType().fullString());
        rowMap.put(handlerProperties.getReadTimeFieldName(), op.getOperation().getReadTimeAsString());

        // Get data
        Set<String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
        Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if (focusFields != null && focusFields.contains(colName)) {
                rowMap.put(OdpsUtils.getColNameBefore(colName), cols.get(i).getBeforeValue());
                rowMap.put(OdpsUtils.getColNameAfter(colName), cols.get(i).getAfterValue());
            } else if (keyFields != null && keyFields.contains(colName)) {
                String colValue = cols.get(i).getAfterValue();
                if (StringUtils.isEmpty(colValue)) {
                    colValue = cols.get(i).getBeforeValue();
                }
                rowMap.put(colName, colValue);
            }
        }

        // Partition
        String partitionSpec = null;
        List<String> partitionCols = handlerProperties.getPartitionCols();
        if (partitionCols != null && partitionCols.size() != 0) {
            long timestamp;
            try {
                String timeStampField = handlerProperties.getTimestampField();
                String timestampValue = StringUtils.isEmpty(timeStampField) ? op.getTimestamp() :
                    rowMap.get(timeStampField.toLowerCase());
                timestamp = handlerProperties.getSimpleDateFormat().parse(timestampValue).getTime();
            } catch (ParseException e) {
                logger.error("Parse operation timestamp error. ", e);
                throw new RuntimeException("Parse operation timestamp error. ", e);
            }
            partitionSpec = getPartitionSpec(odpsWriter, handlerProperties, timestamp, rowMap, true);
        }
        try {
            odpsWriter.addToList(partitionSpec, rowMap);
        } catch (Exception e) {
            StringBuilder sb = new StringBuilder();
            sb.append("------------------------\n");
            sb.append("Failed operation info:\n");
            sb.append("table: " + fullTableName);
            sb.append("\npartitionSpec: " + partitionSpec);
            sb.append("\nvalues: ");
            for (Map.Entry<String, String> entry : rowMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                sb.append(key == null ? "#hxRealNullLabel#" : key)
                    .append("=")
                    .append(value == null ? "#hxRealNullLabel#" : value)
                    .append("\t,\t");
            }
            sb.append("\n------------------------\n");
            logger.error(sb.toString());
            throw new RuntimeException(e);
        }
    }

    // Get OdpsWriter from the map, create if not exist
    private OdpsWriter getOdpsWriter(HandlerProperties handlerProperties, String fullTableName) throws TunnelException, TimeoutException, IOException {
        Map<String, OdpsWriter> tableWriterMap = handlerProperties.getTableWriterMap();
        if (tableWriterMap == null) {
            tableWriterMap = Maps.newHashMap();
            handlerProperties.setTableWriterMap(tableWriterMap);
        }
        OdpsWriter odpsWriter = tableWriterMap.get(fullTableName);
        if (odpsWriter == null) {
            String odpsTableName = handlerProperties.getOracleOdpsTableMap().get(fullTableName);
            if (odpsTableName == null) {
                throw new OdpsTableNotSetException("Oracle table name: " + fullTableName);
            }
            List<String> inputColNames = Lists.newLinkedList();
            // Meta cols
            //inputColNames.add(handlerProperties.getMetaFieldName());
            inputColNames.add(handlerProperties.getOpTypeFieldName());
            inputColNames.add(handlerProperties.getReadTimeFieldName());

            // Key cols
            Set<String> keyFields = handlerProperties.getTableKeysMap().get(fullTableName);
            if (keyFields != null) {
                for (String k: keyFields) {
                    inputColNames.add(k);
                }
            }
            // Focus cols
            Set<String> focusFields = handlerProperties.getTableFocusMap().get(fullTableName);
            if (focusFields != null) {
                for (String f: focusFields) {
                    inputColNames.add(OdpsUtils.getColNameBefore(f));
                    inputColNames.add(OdpsUtils.getColNameAfter(f));
                }
            }

            // Build writer
            Table odpsTable = handlerProperties.getOdpsTables().get(odpsTableName);
            odpsWriter = OdpsUtils.buildOdpsWriter(odpsTable,
                handlerProperties.getTunnel(),
                handlerProperties.getProject(),
                odpsTableName,
                handlerProperties.getShardNumber(),
                handlerProperties.getBatchSize(),
                handlerProperties.getRetryCount(),
                handlerProperties.getShardTimeout(),
                handlerProperties.getDbDateFormat(),
                inputColNames,
                handlerProperties);
            tableWriterMap.put(fullTableName, odpsWriter);
        }
        return odpsWriter;
    }

    private String getPartitionSpec(OdpsWriter odpsWriter,
                                    HandlerProperties handlerProperties,
                                    long timestamp,
                                    Map rowMap,
                                    boolean autoCreatePartition) {
        List<String> partitionVals = handlerProperties.getPartitionVals();
        List<String> partitionCols = handlerProperties.getPartitionCols();

        if (partitionCols == null || partitionVals == null || partitionCols.size() == 0) {
            return StringUtils.EMPTY;
        }
        if (partitionCols.size() != partitionVals.size()) {
            throw new RuntimeException("Partition fields number not equals partition values number.");
        }
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (int i = 0; i < partitionCols.size(); i++) {
            String realPartVal = BucketPath.escapeString(partitionVals.get(i), timestamp, rowMap);
            sb.append(sep).append(partitionCols.get(i)).append("='").append(realPartVal).append("'");
            sep = ",";
        }
        String partitionSpec = sb.toString();
        Set<String> partitionSet = odpsWriter.getPartitionSet();
        if (autoCreatePartition && !partitionSet.contains(partitionSpec)) {
            try {
                odpsWriter.createPartition(new PartitionSpec(partitionSpec), true);
            } catch (OdpsException e) {
                logger.error("Create partition failed. ", e);
                throw new RuntimeException("Create partition failed. ", e);
            }
            partitionSet.add(partitionSpec);
        }
        return partitionSpec;
    }
}
