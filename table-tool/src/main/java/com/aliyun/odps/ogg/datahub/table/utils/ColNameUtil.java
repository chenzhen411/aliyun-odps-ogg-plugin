package com.aliyun.odps.ogg.datahub.table.utils;

/**
 * Created by tianli on 15/8/25.
 */
public class ColNameUtil {
    public static String getBeforeName(String colName) {
        return colName + "_before";
    }

    public static String getAfterName(String colName) {
        return colName + "_after";
    }
}
