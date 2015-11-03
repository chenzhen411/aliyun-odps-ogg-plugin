package com.aliyun.odps.ogg.datahub.table;

import com.aliyun.odps.ogg.datahub.table.handler.CmdType;
import com.aliyun.odps.ogg.datahub.table.handler.CmdHandler;

public class TableManipulator {
    public static void main(String[] args) {
        if(args.length != 2 && args.length != 3) {
            System.out.println("Invalid arguments number. Arguments: CREATE/DROP PROPERTIES_FILE [HANDLER_NAME]");
            return;
        }
        String cmdString = args[0];
        String propertyFileName = args[1];

        CmdType cmdType = CmdType.valueOf(cmdString.toUpperCase());
        CmdHandler cmdHandler = cmdType.getCmdHandler(propertyFileName);
        if(args.length == 3) {
            cmdHandler.setHubHandlerName(args[2]);
        }

        cmdHandler.process();
        System.out.println("Done.");
    }
}
