package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.ogg.datahub.table.TableParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateHandler extends CmdHandler {

    private final static Logger logger = LoggerFactory.getLogger(CreateHandler.class);

    @Override
    public void process() {
        init();
        for (TableParams params: tableParamsList) {
            params.doCreate(tables);
        }
    }

}
