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

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecordBuilder {
    private Column[] odpsColumns;
    private SimpleDateFormat dateFormat;
    private Map<String, OdpsType> colNameTypeMap;

    final static Set trueString = new HashSet() {{
        add("true");
        add("1");
        add("y");
    }};

    final static Set falseString = new HashSet() {{
        add("false");
        add("0");
        add("n");
    }};

    public RecordBuilder(Table odpsTable, String dateFormatString, List<String> inputColNames) {
        TableSchema tableSchema = odpsTable.getSchema();
        odpsColumns = tableSchema.getColumns().toArray(new Column[0]);
        dateFormat = new SimpleDateFormat(dateFormatString);
        colNameTypeMap = buildColNameTypeMap(inputColNames, tableSchema);
    }

    private Map<String, OdpsType> buildColNameTypeMap(List<String> inputColNames, TableSchema tableSchema) {
        Map<String, OdpsType> odpsNameTypeMap = Maps.newHashMap();
        for (Column column : tableSchema.getColumns()) {
            odpsNameTypeMap.put(column.getName().toLowerCase(), column.getType());
        }
        Map<String, OdpsType> colNameTypeMap = Maps.newHashMap();
        for (String colName : inputColNames) {
            if (!StringUtils.isEmpty(colName)) {
                if (odpsNameTypeMap.containsKey(colName)) {
                    colNameTypeMap.put(colName, odpsNameTypeMap.get(colName));
                } else {
                    throw new RuntimeException(this.getClass().getName() +
                            " buildColNameTypeMap() error, field not exists in odps table, field=" + colName);
                }
            }
        }
        return colNameTypeMap;
    }

    public Record buildRecord(Map<String, String> rowMap) {
        Record record = new ArrayRecord(odpsColumns);
        for (Map.Entry<String, String> mapEntry : rowMap.entrySet()) {
            try {
                setField(record, mapEntry.getKey(), mapEntry.getValue(), colNameTypeMap.get(mapEntry.getKey()));
            } catch (Exception e) {
                throw new RuntimeException("Error in parsing field '"
                    + mapEntry.getKey() + "'", e);
            }
        }
        return record;
    }

    private void setField(Record record, String field, String fieldValue, OdpsType odpsType) throws ParseException {
        if (StringUtils.isNotEmpty(field) && StringUtils.isNotEmpty(fieldValue)) {
            if (odpsType == null) {
                throw new RuntimeException("Unknown column type: " + odpsType + " of " + field + "=" + fieldValue);
            }
            switch (odpsType) {
                case STRING:
                    record.setString(field, fieldValue);
                    break;
                case BIGINT:
                    record.setBigint(field, Long.parseLong(fieldValue));
                    break;
                case DATETIME:
                    if (dateFormat != null) {
                        record.setDatetime(field, dateFormat.parse(fieldValue));
                    }
                    break;
                case DOUBLE:
                    record.setDouble(field, Double.parseDouble(fieldValue));
                    break;
                case BOOLEAN:
                    if (trueString.contains(fieldValue.toLowerCase())) {
                        record.setBoolean(field, true);
                    } else if (falseString.contains(fieldValue.toLowerCase())) {
                        record.setBoolean(field, false);
                    }
                    break;
                case DECIMAL:
                    record.setDecimal(field, new BigDecimal(fieldValue));
                default:
                    throw new RuntimeException("Unknown column type: " + odpsType + " of " + field + "=" + fieldValue);
            }
        }
    }
}
