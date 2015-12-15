package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.ogg.datahub.table.TableParams;
import com.aliyun.odps.ogg.datahub.table.utils.ColNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.*;

public abstract class CmdHandler {

    private final static Logger logger = LoggerFactory.getLogger(CmdHandler.class);

    public void setPropertyFileName(String propertyFileName) {
        this.propertyFileName = propertyFileName;
    }

    private String propertyFileName;
    private String hubHandlerName;

    public void setHubHandlerName(String hubHandlerName) {
        this.hubHandlerName = hubHandlerName;
    }

    private Properties prop;

    protected Tables tables;

    protected List<TableParams> tableParamsList;

    protected void init() {
        loadProperties();
        // Init ODPS
        String project = getHubProperty("project");
        Odps odps = new Odps(new AliyunAccount(getHubProperty("accessID"), getHubProperty("accessKey")));
        odps.setDefaultProject(project);
        odps.setEndpoint(getHubProperty("endPoint"));
        tables = odps.tables();

        Map<String, String> oracleOdpsTableMap = buildMap(getHubProperty("tableMap"));
        Map<String, Map<String, OdpsType>> tableKeysMap = buildStringPairsMap(getHubProperty("keyFields"));
        Map<String, Map<String, OdpsType>> tableFocusMap = buildStringPairsMap(getHubProperty("focusFields"));
        tableParamsList = buildTableParams(project, oracleOdpsTableMap, tableKeysMap, tableFocusMap);
    }

    private void loadProperties() {
        prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(propertyFileName);
            prop.load(fis);
            if(StringUtils.isEmpty(hubHandlerName)) {
                hubHandlerName = Preconditions.checkNotNull(prop.getProperty("gg.handlerlist"), "handler is not specified");
                if(hubHandlerName.split(",").length != 1) {
                    throw new RuntimeException("More than one handler founded. You should specify the handler name in the arguments.");
                }
            }
        } catch (java.io.IOException e) {
            logger.error("Load properties file failed. ", e);
            throw new RuntimeException("Load properties file failed. ", e);
        }
    }

    private List<TableParams> buildTableParams(String project,
                                  Map<String, String> oracleOdpsTableMap,
                                  Map<String, Map<String, OdpsType>> tableKeysMap,
                                  Map<String, Map<String, OdpsType>> tableFocusMap) {
        List<TableParams> paramsList = Lists.newLinkedList();
        for (Map.Entry<String, String> entry: oracleOdpsTableMap.entrySet()) {
            String oracleTable = entry.getKey();

            TableParams params = new TableParams();
            params.setProjectName(project);
            params.setTableName(entry.getValue());

            long shardNumber = Long.valueOf(
                Preconditions.checkNotNull(getHubProperty("shardNumber"), "shardNumber is not specified."));
            long hubLifeCycle = Long.valueOf(
                Preconditions.checkNotNull(getHubProperty("hubLifeCycle"), "hubLifeCycle is not specified."));
            params.setShardNumber(shardNumber);
            params.setHubLifeCycle(hubLifeCycle);

            Map<String, OdpsType> colTypeMap = Maps.newLinkedHashMap();
            colTypeMap.put(getHubProperty("opTypeField", "optype"), OdpsType.STRING);
            colTypeMap.put(getHubProperty("readTimeField", "readtime"), OdpsType.STRING);
            if (tableKeysMap != null) {
                Map<String, OdpsType> keyFields = tableKeysMap.get(oracleTable);
                if (keyFields != null) {
                    for (Map.Entry<String, OdpsType> stringOdpsTypeEntry: keyFields.entrySet()) {
                        colTypeMap.put(stringOdpsTypeEntry.getKey(), stringOdpsTypeEntry.getValue());
                    }
                }
            }
            if (tableFocusMap != null) {
                Map<String, OdpsType> focusFields = tableFocusMap.get(oracleTable);
                if (focusFields != null) {
                    for (Map.Entry<String, OdpsType> stringOdpsTypeEntry: focusFields.entrySet()) {
                        colTypeMap.put(ColNameUtil.getBeforeName(stringOdpsTypeEntry.getKey()), stringOdpsTypeEntry.getValue());
                        colTypeMap.put(ColNameUtil.getAfterName(stringOdpsTypeEntry.getKey()), stringOdpsTypeEntry.getValue());
                    }
                }
            }
            params.setColTypeMap(colTypeMap);

            // Partition
            List<String> partitions = Lists.newLinkedList();
            String partitionColumns = getHubProperty("partitionFields");
            if (StringUtils.isNotEmpty(partitionColumns)) {
                String[] partitionCols = partitionColumns.split(",");
                for (int i = 0; i < partitionCols.length; i++) {
                    partitions.add(partitionCols[i].trim().toLowerCase());
                }
            }
            params.setPartitionCols(partitions);
            paramsList.add(params);
        }
        return paramsList;
    }


    protected String getHubProperty(String propertyName) {
        return prop.getProperty("gg.handler." + hubHandlerName + "." + propertyName);
    }

    protected String getHubProperty(String propertyName, String defaultValue) {
        String res = getHubProperty(propertyName);
        if (StringUtils.isEmpty(res)) {
            return defaultValue;
        }
        return res;
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

    private Map<String, Map<String, OdpsType>> buildStringPairsMap(String str) {
        if (StringUtils.isEmpty(str)) {
            return null;
        }
        // "table1:name1/type1,name2/type2|table2:name3/type3|...
        // all to lower case
        Map<String, Map<String, OdpsType>> map = Maps.newHashMap();
        String[] tableInfos = str.split("\\|");
        for (String tableInfo: tableInfos) {
            String[] nameList = tableInfo.split(":");
            String name = nameList[0].trim().toLowerCase();
            Map<String, OdpsType> pairs = Maps.newHashMap();
            for (String s: nameList[1].split(",")) {
                String[] nameType = s.split("/");
                pairs.put(nameType[0].trim().toLowerCase(), OdpsType.valueOf(nameType[1].trim().toUpperCase()));
            }
            map.put(name, pairs);
        }
        return map;
    }

    public abstract void process();
}
