package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ogg.datahub.table.TableParams;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class DropHandler extends CmdHandler {

    private final static Logger logger = LoggerFactory.getLogger(DropHandler.class);

    @Override
    public void process() {
        init();
        for (TableParams params: tableParamsList) {
            params.doDrop(tables);
        }
    }
}
