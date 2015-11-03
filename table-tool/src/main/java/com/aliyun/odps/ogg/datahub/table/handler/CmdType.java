package com.aliyun.odps.ogg.datahub.table.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CmdType {
    CREATE(CreateHandler.class),
    DROP(DropHandler.class);

    final private Logger logger = LoggerFactory.getLogger(CmdType.class);

    private CmdHandler cmdHandler;
    private CmdType(Class<?> clazz) {
        try {
            cmdHandler = (CmdHandler) clazz.newInstance();
        } catch (Exception e) {
            logger.error("Unable to instantiate operation handler. " + clazz, e);
        }
    }

    public CmdHandler getCmdHandler(String propertyFileName) {
        this.cmdHandler.setPropertyFileName(propertyFileName);
        return this.cmdHandler;
    }
}
