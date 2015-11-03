package com.aliyun.odps.ogg.datahub.table.handler;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.AliyunAccount;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

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
    protected String tableName;

    protected void init() {
        loadProperties();
        buildOdpsTables();
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

    private void buildOdpsTables() {
        Odps odps = new Odps(new AliyunAccount(getHubProperty("accessID"), getHubProperty("accessKey")));
        odps.setDefaultProject(getHubProperty("project"));
        odps.setEndpoint(getHubProperty("endPoint"));
        tables = odps.tables();
        tableName = Preconditions.checkNotNull(getHubProperty("tableName"), "tableName is not specified.");
    }

    protected String getHubProperty(String propertyName) {
        return prop.getProperty("gg.handler." + hubHandlerName + "." + propertyName);
    }

    public abstract void process();
}
