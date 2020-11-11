package com.jinmu.mongodb.demo.update;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.Utils;
import com.jinmu.mongodb.demo.entity.User;
import com.jinmu.mongodb.demo.utils.UserUtil;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * 这里演示修改
 * 修改加入了事务
 */
public class UpdateDemo extends MongoBase {

    /**
     * 构造方法
     *
     * @param mongoClient
     */
    public UpdateDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 单条数据修改，如果匹配到了很多条数据，只会修改第一条
     */
    public void updateOneDocument(String fieldName, String value,Document document) {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");


        //获取session
        ClientSession session = this.mongoClient.startSession();
        try {
            //开启事务
            session.startTransaction();

            //单条数据修改
            if ("_id".equals(fieldName))
                coll.updateOne(session, Filters.eq("_id", new ObjectId(value)), document);
            else
                coll.updateOne(session, Filters.eq(fieldName, value), document);

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

    }

    /**
     * 多条数据修改
     */
    public void updateManyDocument(String fieldName, String value,Document document) {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();
        try {
            //开启事务
            session.startTransaction();

            //根据条件修改多条数据
            if ("_id".equals(fieldName))
                coll.updateMany(session, Filters.eq("_id", new ObjectId(value)), document);
            else
                coll.updateMany(session, Filters.eq(fieldName, value), document);

            //提交事务
            session.commitTransaction();
        }catch (Exception e){
            e.printStackTrace();

            //回滚
            session.abortTransaction();
        }finally{
            //关闭session
            session.close();
        }
    }

    public static void main(String[] args) {
        MongoClient mongoClient = Utils.getMongoClient();
        UpdateDemo updateDemo = new UpdateDemo(mongoClient);

        //要修改的数据
        User user = UserUtil.getUser();
        Document document = JSONObject.parseObject(JSON.toJSONString(user), Document.class);

        //单个数据修改
        updateDemo.updateOneDocument("name","abc",document);

        //多个数据修改
        updateDemo.updateManyDocument("name","abc",document);

    }
}
