package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.OdpsException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class DropHandler extends CmdHandler {

    private final static Logger logger = LoggerFactory.getLogger(DropHandler.class);

    @Override
    public void process() {
        init();
        Scanner in = new Scanner(System.in);
        System.out.print("Drop table " + tableName + "? (y/n):");
        String inputString = in.nextLine();
        if (!StringUtils.equals(inputString, "y")) {
            return;
        }
        try {
            tables.delete(tableName);
        } catch (OdpsException e) {
            logger.error("Drop table failed. ", e);
            throw new RuntimeException("Drop table failed. ", e);
        }
    }
}
