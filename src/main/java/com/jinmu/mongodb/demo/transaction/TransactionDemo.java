package com.jinmu.mongodb.demo.transaction;

import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.Utils;
import com.mongodb.client.*;
import org.bson.Document;

/**
 * 这里演示事务的另外一种实现方式
 * 如果一个方法内要执行多条sql语句的话，这种方式更方便
 */
public class TransactionDemo extends MongoBase {

    /**
     * 构造函数
     *
     * @param mongoClient
     */
    public TransactionDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 事务实现
     * 将数据添加到两个不同的db当中，当然也可以添加到同一个db中
     */
    public void transactionTest() {
        //获取session
        final ClientSession session = this.mongoClient.startSession();

        //获取两个不同的db
        final MongoDatabase defaultDataBase = this.getDefaultDataBase();//默认dataBase  -> "demo"
        final MongoDatabase db = this.mongoClient.getDatabase("demoTransaction");


        TransactionBody txnBody = new TransactionBody() {
            @Override
            public String execute() {
                //获取两个coll
                MongoCollection<Document> coll = defaultDataBase.getCollection("test");
                MongoCollection<Document> coll1 = db.getCollection("test");

                //添加数据
                coll.insertOne(session, new Document("text", "事务数据测试1"));

                //模拟异常发生
                //System.out.println(3/0);

                coll1.insertOne(session, new Document("text", "事务数据测试2"));

                return "将数据添加到不同的dataBase中！";
            }
        };
        try {
            session.withTransaction(txnBody);
        } catch (Exception e) {
            e.printStackTrace();
            session.abortTransaction();
        } finally {
            session.close();
        }

    }

    /**
     * 查询两个数据库内数据，看发生异常之后数据是否有被提交
     */
    public void queryData() {
        MongoCollection<Document> coll = this.getDefaultDataBase().getCollection("test");
        MongoCollection<Document> coll1 = this.mongoClient.getDatabase("demoTransaction").getCollection("test");

        FindIterable<Document> documents = coll.find();
        for (Object o :
                documents) {
            System.out.println(o);
        }

        System.out.println("======================================");

        FindIterable<Document> documents1 = coll1.find();
        for (Object o :
                documents1) {
            System.out.println(o);
        }
    }

    /**
     * 清空不同dataBase内的coll
     */
    public void clear() {

        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //删除coll
        coll.drop();

        //创建coll
        db.createCollection("test");

        //第二个dataBase
        MongoDatabase db1 = this.mongoClient.getDatabase("demoTransaction");

        MongoCollection<Document> coll1 = db1.getCollection("test");

        //删除coll
        coll1.drop();

        //创建coll
        db1.createCollection("test");

    }

    public static void main(String[] args) {
        MongoClient client = Utils.getMongoClient();
        TransactionDemo transactionDemo = new TransactionDemo(client);

        //到方法内将注释的异常打开，模拟异常出现情况，看数据会不会一同添加
        transactionDemo.transactionTest();

        //查询两个dataBase内的数据
        transactionDemo.queryData();

        //清空两个dataBase内的coll
        transactionDemo.clear();

    }
}
