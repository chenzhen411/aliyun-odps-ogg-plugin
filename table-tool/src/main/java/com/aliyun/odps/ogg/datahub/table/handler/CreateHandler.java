package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ogg.datahub.table.utils.ColNameUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CreateHandler extends CmdHandler {

    private final static Logger logger = LoggerFactory.getLogger(CreateHandler.class);

    @Override
    public void process() {
        init();
        try {
            if (tables.exists(tableName)) {
                logger.error("Create table failed. Table already exists.");
                throw new RuntimeException("Create table failed. Table already exists.");
            }
        } catch (OdpsException e) {
            logger.error("Check table existence failed. ", e);
            throw new RuntimeException("Check table existence failed. ", e);
        }
        Map<String, OdpsType> hubColTypeMap = buildColTypeMap();
        TableSchema schema = new TableSchema();
        for(Map.Entry<String, OdpsType> colTypeEntry: hubColTypeMap.entrySet()) {
            schema.addColumn(new Column(colTypeEntry.getKey(), colTypeEntry.getValue()));
        }

        String partitionColumns = getHubProperty("partitionFields");
        if(StringUtils.isNotEmpty(partitionColumns)) {
            String[] partitionCols = partitionColumns.split(",");
            for (int i = 0; i < partitionCols.length; i++) {
                schema.addPartitionColumn(new Column(partitionCols[i].trim(), OdpsType.STRING));
            }
        }

        try {
            tables.create(tableName, schema);
            int shardNumber = Integer.valueOf(Preconditions.checkNotNull(getHubProperty("shardNumber"), "shardNumber is not specified."));
            int hubLifeCycle = Integer.valueOf(Preconditions.checkNotNull(getHubProperty("hubLifeCycle"), "hubLifeCycle is not specified."));
            tables.get(tableName).createShards(shardNumber, true, hubLifeCycle);
        } catch (OdpsException e) {
            logger.error("Create table failed. ", e);
            throw new RuntimeException("Create table failed. ", e);
        }
    }

    private Map buildColTypeMap() {
        Map hubColTypeMap = Maps.newHashMap();

        String operTimeField = getHubProperty("operTimeField");
        if (StringUtils.isNotEmpty(operTimeField)) {
            hubColTypeMap.put(operTimeField, OdpsType.STRING);
        }
        String operTypeField = getHubProperty("operTypeField");
        if (StringUtils.isNotEmpty(operTypeField)) {
            hubColTypeMap.put(operTypeField, OdpsType.STRING);
        }

        String keyColumns = getHubProperty("keyFields");
        if(StringUtils.isNotEmpty(keyColumns)) {
            for (String s : keyColumns.split(",")) {
                String[] pair = s.split("/");
                hubColTypeMap.put(pair[0].trim(), OdpsType.valueOf(pair[1].trim()));
            }
        }

        String focusColumns = getHubProperty("focusFields");
        if(StringUtils.isNotEmpty(focusColumns)) {
            for (String s : focusColumns.split(",")) {
                String[] pair = s.split("/");
                hubColTypeMap.put(ColNameUtil.getBeforeName(pair[0].trim()), OdpsType.valueOf(pair[1].trim()));
                hubColTypeMap.put(ColNameUtil.getAfterName(pair[0].trim()), OdpsType.valueOf(pair[1].trim()));
            }
        }

        return hubColTypeMap;
    }
}
