package com.jinmu.mongodb.demo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * 访问MongoDB的基类，供client和base给派生类使用
 */
public abstract class MongoBase {
    protected MongoClient mongoClient;
    protected String defaultDbName;

    /**
     * 构造方法
     * @param mongoClient 访问数据库时用的client
     * @param defaultDbName 默认访问的数据库名
     */
    public MongoBase(MongoClient mongoClient, String defaultDbName) {
        this.mongoClient = mongoClient;
        this.defaultDbName = defaultDbName;
    }

    /**
     * 如果调用这个构造函数，则表示访问的默认的数据库
     * @param mongoClient
     */
    public MongoBase(MongoClient mongoClient) {
        this(mongoClient, "demo");
    }

    /**
     * 获取默认数据库（由派生类的初始化时传入）
     * @return
     */
    protected MongoDatabase getDefaultDataBase() {
        MongoDatabase database = this.mongoClient.getDatabase(this.defaultDbName);
        return database;
    }


}
