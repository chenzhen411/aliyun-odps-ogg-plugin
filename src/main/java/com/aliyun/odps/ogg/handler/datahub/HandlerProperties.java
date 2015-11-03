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

import com.aliyun.odps.Table;
import com.aliyun.odps.ogg.handler.datahub.alarm.OggAlarm;
import com.aliyun.odps.ogg.handler.datahub.dataobject.OdpsRowDO;

public class HandlerProperties {
	
	public HandlerProperties() {
	}
	
	public Long totalInserts = 0L;
	public Long totalUpdates = 0L;
	public Long totalDeletes =  0L;
	public Long totalTxns = 0L;
	public Long totalOperations = 0L;

	private OggAlarm oggAlarm;
	private String operTypeField;
	private String operTimeField;
	private Map<String, Boolean> focusColMap;
	private Map<String, Boolean> keyColMap;
	private List<OdpsRowDO> odpsRowDOs;
	private List<String> inputColNames;
	private String[] partitionCols;
	private String[] partitionVals;

	private String timestampField;
	private SimpleDateFormat simpleDateFormat;
	private RecordBuilder recordBuilder;

	public RecordBuilder getRecordBuilder() {
		return recordBuilder;
	}

	public void setRecordBuilder(RecordBuilder recordBuilder) {
		this.recordBuilder = recordBuilder;
	}

	public String[] getPartitionVals() {
		return partitionVals;
	}

	public void setPartitionVals(String[] partitionVals) {
		this.partitionVals = partitionVals;
	}

	public String[] getPartitionCols() {
		return partitionCols;
	}

	public void setPartitionCols(String[] partitionCols) {
		this.partitionCols = partitionCols;
	}

	private Map<String, Boolean> partitionMap;

	private Table odpsTable;

	public Table getOdpsTable() {
		return odpsTable;
	}

	public void setOdpsTable(Table odpsTable) {
		this.odpsTable = odpsTable;
	}

	public Map<String, Boolean> getPartitionMap() {
		return partitionMap;
	}

	public void setPartitionMap(Map<String, Boolean> partitionMap) {
		this.partitionMap = partitionMap;
	}


	public List<String> getInputColNames() {
		return inputColNames;
	}

	public void setInputColNames(List<String> inputColNames) {
		this.inputColNames = inputColNames;
	}

	public Map<String, Boolean> getFocusColMap() {
		return focusColMap;
	}

	public void setFocusColMap(Map<String, Boolean> focusColMap) {
		this.focusColMap = focusColMap;
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

	public List<OdpsRowDO> getOdpsRowDOs() {
		return odpsRowDOs;
	}

	public void setOdpsRowDOs(List<OdpsRowDO> odpsRowDOs) {
		this.odpsRowDOs = odpsRowDOs;
	}
	public Map<String, Boolean> getKeyColMap() {
		return keyColMap;
	}

	public void setKeyColMap(Map<String, Boolean> keyColMap) {
		this.keyColMap = keyColMap;
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
}
