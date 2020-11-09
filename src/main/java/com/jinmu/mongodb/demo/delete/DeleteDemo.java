package com.jinmu.mongodb.demo.delete;

import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.Utils;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.logging.Filter;

/**
 * 这里演示删除
 */
public class DeleteDemo extends MongoBase {

    /**
     * 构造函数
     * @param mongoClient
     */
    public DeleteDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 单条数据删除
     * 如果匹配多条数据，只会删除第一条
     */
    public void deleteOne(String fieldName,String value){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();

        try{
            //开启事务
            session.startTransaction();

            //单条数据删除
            if ("_id".equals(fieldName))
                coll.deleteOne(session,Filters.eq("_id",new ObjectId(value)));
            else
                coll.deleteOne(session,Filters.eq(fieldName,value));

            //提交事务
            session.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            //回滚
            session.abortTransaction();
        }finally {
            //关闭session
            session.close();
        }
    }

    /**
     * 多条数据删除
     */
    public  void deleteMany(String fieldName,String value){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();

        try{
            //开启事务
            session.startTransaction();

            //多条数据删除
            if ("_id".equals(fieldName))
                coll.deleteMany(session,Filters.eq("_id",new ObjectId(value)));
            else
                coll.deleteMany(session,Filters.eq(fieldName,value));

            //提交事务
            session.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();
            //回滚
            session.abortTransaction();
        }finally {
            //关闭session
            session.close();
        }
    }

    public static void main(String[] args) {
        MongoClient mongoClient = Utils.getMongoClient();

        DeleteDemo deleteDemo = new DeleteDemo(mongoClient);

        //单条数据删除
        deleteDemo.deleteOne("name","abc");

        //多条数据删除
        deleteDemo.deleteMany("name","abc");

    }
}
