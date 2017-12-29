package com.uber.athenax.backend.server.yarn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public abstract class HdfsWrapper {

    Logger logger = LoggerFactory.getLogger(HdfsWrapper.class);

    public static Configuration loadConf() {
        return loadConf(new Configuration());
    }

    public static Configuration loadConf(Configuration configuration) {
        String USER_NAME = System.getenv("HADOOP_USER_NAME");
        if (StringUtils.isBlank(USER_NAME)) {
            USER_NAME = "linkoop";
        }
        System.setProperty("HADOOP_USER_NAME", USER_NAME);
        String CONF_PATH = System.getenv("HADOOP_CONF_DIR");
        if (StringUtils.isNotBlank(CONF_PATH)) {
            configuration.addResource(new Path(CONF_PATH + File.separator + "core-site.xml"));
            configuration.addResource(new Path(CONF_PATH + File.separator + "hdfs-site.xml"));
        }
        //logger.info("HADOOP_USER_NAME = {}, HADOOP_CONF_DIR = {}", USER_NAME, CONF_PATH);
        return configuration;
    }


}
