package com.zyz.kafka.utill;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zyz on 2017/9/6.
 */
public class PropertiesUtils {

    private static Logger LOGGER = LoggerFactory.getLogger(PropertiesUtils.class);


    public static Properties getProperties(String filePath){
        InputStream in = ClassLoader.getSystemResourceAsStream(filePath);
        Properties properties = new Properties();
        try {
            properties.load(in);
        }catch (Exception e){
            LOGGER.error("load file=>{}",filePath,e);
        }
        return properties;
    }

}
