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

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.ogg.handler.datahub.alarm.OggAlarm;
import com.aliyun.odps.ogg.handler.datahub.utils.BucketPath;
import com.aliyun.odps.ogg.handler.datahub.utils.ColNameUtil;
import com.aliyun.odps.ogg.handler.datahub.RecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.ogg.handler.datahub.HandlerProperties;
import com.aliyun.odps.ogg.handler.datahub.dataobject.OdpsRowDO;
import com.goldengate.atg.datasource.DsColumn;
import com.goldengate.atg.datasource.adapt.Op;
import com.google.common.collect.Maps;

public abstract class OperationHandler {

    private final static Logger logger = LoggerFactory.getLogger(OperationHandler.class);

    public abstract void process(Op op, HandlerProperties handlerProperties)
            throws Exception;

    protected void processOperation(Op op, HandlerProperties handlerProperties) throws ParseException{
        OdpsRowDO odpsRowDO = prepareOdpsRow(op, handlerProperties);
        handlerProperties.getOdpsRowDOs().add(odpsRowDO);
    }

    private OdpsRowDO prepareOdpsRow(Op op, HandlerProperties handlerProperties) throws ParseException {
        OdpsRowDO odpsRowDO = new OdpsRowDO();

        Map<String, String> rowMap = Maps.newHashMap();
        // Operation type & time
        String operTypeField = handlerProperties.getOperTypeField();
        if (StringUtils.isNotEmpty(operTypeField)) {
            rowMap.put(operTypeField.toLowerCase(), op.getOperationType().toString());
        }
        String operTimeField = handlerProperties.getOperTimeField();
        if (StringUtils.isNotEmpty(operTimeField)) {
            rowMap.put(operTimeField.toLowerCase(), op.getTimestamp());
        }
        // Key Cols and Focus Cols
        List<DsColumn> cols = op.getColumns();

        Map<String, Boolean> focusColMap = handlerProperties.getFocusColMap();
        Map<String, Boolean> keyColMap = handlerProperties.getKeyColMap();

        for (int i = 0; i < cols.size(); i++) {
            String colName = op.getTableMeta().getColumnName(i).toLowerCase();
            if (focusColMap != null && focusColMap.containsKey(colName)) {
                rowMap.put(ColNameUtil.getBeforeName(colName), cols.get(i).getBeforeValue());
                rowMap.put(ColNameUtil.getAfterName(colName), cols.get(i).getAfterValue());
            } else if (keyColMap != null && keyColMap.containsKey(colName)) {
                rowMap.put(colName, cols.get(i).getAfterValue());
            }
        }

        // Partition
        String[] partitionCols = handlerProperties.getPartitionCols();
        if (partitionCols != null && partitionCols.length != 0) {
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
            String partitionSpec = getPartitionSpec(handlerProperties, timestamp, rowMap, true);
            odpsRowDO.setPartitionSpec(partitionSpec);
        }

        RecordBuilder recordBuilder = handlerProperties.getRecordBuilder();
        odpsRowDO.setRecord(recordBuilder.buildRecord(rowMap));
        return odpsRowDO;
    }

    private String getPartitionSpec(HandlerProperties handlerProperties, long timestamp, Map rowMap,
                                    boolean autoCreatePartition) {
        String[] partitionVals = handlerProperties.getPartitionVals();
        String[] partitionCols = handlerProperties.getPartitionCols();

        if (partitionCols == null || partitionVals == null || partitionCols.length == 0) {
            return StringUtils.EMPTY;
        }
        if (partitionCols.length != partitionVals.length) {
            throw new RuntimeException("Partition fields number not equals partition values number.");
        }
        StringBuilder sb = new StringBuilder();
        String sep = "";
        for (int i = 0; i < partitionCols.length; i++) {
            String realPartVal = BucketPath.escapeString(partitionVals[i], timestamp, rowMap);
            sb.append(sep).append(partitionCols[i]).append("='").append(realPartVal).append("'");
            sep = ",";
        }
        String partitionSpec = sb.toString();
        Map parMap = handlerProperties.getPartitionMap();
        if (autoCreatePartition && !parMap.containsKey(partitionSpec)) {
            try {
                handlerProperties.getOdpsTable().createPartition(new PartitionSpec(partitionSpec), true);
            } catch (OdpsException e) {
                logger.error("Create partition failed. ", e);
                throw new RuntimeException("Create partition failed. ", e);
            }
            parMap.put(partitionSpec, true);
        }
        return partitionSpec;
    }
}
