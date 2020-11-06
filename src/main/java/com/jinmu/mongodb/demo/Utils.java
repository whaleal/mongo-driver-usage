package com.jinmu.mongodb.demo;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import java.io.InputStream;
import java.util.Properties;

/**
 * 工具类
 */
public class Utils {
    private static Properties properties;

    static {
        getConfig();
    }

    /**
     * 从配置文件中加载配置
     */
    public static Properties getConfig() {
        if (properties == null) {
            properties = new Properties();

            try {
                InputStream configStream = Utils.class.getResourceAsStream("/config.properties");
                properties.load(configStream);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return properties;
    }

    /**
     * 根据配置文件中的连接字符串创建MongoClient
     */
    public static MongoClient getMongoClient() {
        //Mongo连接字符串
        ConnectionString connStr = new ConnectionString(getConfig().getProperty("CONN_STRING"));
        MongoClient client = MongoClients.create(connStr);
        return client;
    }


}
