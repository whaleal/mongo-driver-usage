package com.jinmu.mongodb.demo.query;

import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.Utils;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * 此类演示查询
 * 查询都加入了事务，虽然一般查询不需要事务
 */
public class FindDemo extends MongoBase {

    /**
     * 构造函数
     *
     * @param mongoClient
     */
    public FindDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 根据条件查询
     */
    public Iterable findByXXX(String fieldName, String value) {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();

        FindIterable<Document> documents = null;
        try {
            //开启事务
            session.startTransaction();

            //根据条件查询
            if ("_id".equals(fieldName))
                documents = coll.find(session, Filters.eq("_id", new ObjectId(value)));
            else
                documents = coll.find(session, Filters.eq(fieldName, value));


                //提交事务
                session.commitTransaction();
        } catch (Exception e) {

            e.printStackTrace();

            //发生异常回滚
            session.abortTransaction();
        } finally {
            session.close();
        }

        return documents;
    }

    /**
     * 查询全部
     */
    public Iterable findAll() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();

        //返回值
        FindIterable<Document> documents = null;

        try {
            //开启事务
            session.startTransaction();

            //查询全部
            documents = coll.find();

            //提交事务
            session.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();

            //回滚
            session.abortTransaction();
        } finally {
            //关闭session
            session.close();
        }

        return documents;
    }

    public static void main(String[] args) {
        MongoClient mongoClient = Utils.getMongoClient();
        FindDemo findDemo = new FindDemo(mongoClient);

        //根据id查询
        //Iterable iter = findDemo.findByXXX("_id", "XXXXX");

        //查询全部
        Iterable iterable = findDemo.findAll();

        for (Object o:
             iterable) {
            System.out.println(o);
        }


    }
}
