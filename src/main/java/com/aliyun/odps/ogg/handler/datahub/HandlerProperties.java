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
package com.aliyun.odps.ogg.handler.datahub;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aliyun.odps.Tables;
import com.aliyun.odps.ogg.handler.datahub.alarm.OggAlarm;
import com.aliyun.odps.tunnel.TableTunnel;

public class HandlerProperties {
	
	public HandlerProperties() {
	}

	private Map<String, OdpsWriter> tableWriterMap;
	private Map<String, String> oracleOdpsTableMap;
	private TableTunnel tunnel;
	private Tables odpsTables;
	private String project;
	private long shardNumber;
	private int shardTimeout;
	private int batchSize;
	private int retryCount;
	private String dbDateFormat;
	private Map<String, Set<String>> tableKeysMap;
	private Map<String, Set<String>> tableFocusMap;
	private String metaFieldName;
	private String opTypeFieldName;
	private String readTimeFieldName;

	private int skipSendTimes;
	private int sendTimesInTx;
	private String handlerInfoFileName;

	public Long totalInserts = 0L;
	public Long totalUpdates = 0L;
	public Long totalDeletes =  0L;
	public Long totalTxns = 0L;
	public Long totalOperations = 0L;

	private OggAlarm oggAlarm;
	private List<String> partitionCols;
	private List<String> partitionVals;

	private String timestampField;
	private SimpleDateFormat simpleDateFormat;

	public List<String> getPartitionVals() {
		return partitionVals;
	}

	public void setPartitionVals(List<String> partitionVals) {
		this.partitionVals = partitionVals;
	}

	public List<String> getPartitionCols() {
		return partitionCols;
	}

	public void setPartitionCols(List<String> partitionCols) {
		this.partitionCols = partitionCols;
	}

	public String getTimestampField() {
		return timestampField;
	}

	public void setTimestampField(String timestampField) {
		this.timestampField = timestampField;
	}

	public SimpleDateFormat getSimpleDateFormat() {
		return simpleDateFormat;
	}

	public void setSimpleDateFormat(SimpleDateFormat simpleDateFormat) {
		this.simpleDateFormat = simpleDateFormat;
	}

	public OggAlarm getOggAlarm() {
		return oggAlarm;
	}

	public void setOggAlarm(OggAlarm oggAlarm) {
		this.oggAlarm = oggAlarm;
	}

	public Map<String, OdpsWriter> getTableWriterMap() {
		return tableWriterMap;
	}

	public void setTableWriterMap(Map<String, OdpsWriter> tableWriterMap) {
		this.tableWriterMap = tableWriterMap;
	}

	public Map<String, String> getOracleOdpsTableMap() {
		return oracleOdpsTableMap;
	}

	public void setOracleOdpsTableMap(Map<String, String> oracleOdpsTableMap) {
		this.oracleOdpsTableMap = oracleOdpsTableMap;
	}

	public TableTunnel getTunnel() {
		return tunnel;
	}

	public void setTunnel(TableTunnel tunnel) {
		this.tunnel = tunnel;
	}

	public Tables getOdpsTables() {
		return odpsTables;
	}

	public void setOdpsTables(Tables odpsTables) {
		this.odpsTables = odpsTables;
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
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

	public String getDbDateFormat() {
		return dbDateFormat;
	}

	public void setDbDateFormat(String dbDateFormat) {
		this.dbDateFormat = dbDateFormat;
	}

	public Map<String, Set<String>> getTableKeysMap() {
		return tableKeysMap;
	}

	public void setTableKeysMap(Map<String, Set<String>> tableKeysMap) {
		this.tableKeysMap = tableKeysMap;
	}

	public Map<String, Set<String>> getTableFocusMap() {
		return tableFocusMap;
	}

	public void setTableFocusMap(Map<String, Set<String>> tableFocusMap) {
		this.tableFocusMap = tableFocusMap;
	}

	public String getMetaFieldName() {
		return metaFieldName;
	}

	public void setMetaFieldName(String metaFieldName) {
		this.metaFieldName = metaFieldName;
	}

	public String getOpTypeFieldName() {
		return opTypeFieldName;
	}

	public void setOpTypeFieldName(String opTypeFieldName) {
		this.opTypeFieldName = opTypeFieldName;
	}

	public String getReadTimeFieldName() {
		return readTimeFieldName;
	}

	public void setReadTimeFieldName(String readTimeFieldName) {
		this.readTimeFieldName = readTimeFieldName;
	}

	public int getSkipSendTimes() {
		return skipSendTimes;
	}

	public void setSkipSendTimes(int skipSendTimes) {
		this.skipSendTimes = skipSendTimes;
	}

	public int getSendTimesInTx() {
		return sendTimesInTx;
	}

	public void setSendTimesInTx(int sendTimesInTx) {
		this.sendTimesInTx = sendTimesInTx;
	}

	public void incrementSendTimesInTx() {
		sendTimesInTx ++;
	}

	public String getHandlerInfoFileName() {
		return handlerInfoFileName;
	}

	public void setHandlerInfoFileName(String handlerInfoFileName) {
		this.handlerInfoFileName = handlerInfoFileName;
	}
}
