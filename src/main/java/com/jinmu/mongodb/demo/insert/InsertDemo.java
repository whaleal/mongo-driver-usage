package com.jinmu.mongodb.demo.insert;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jinmu.mongodb.demo.Utils;
import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.entity.User;
import com.jinmu.mongodb.demo.utils.UserUtil;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.swing.text.Document;
import java.util.ArrayList;
import java.util.List;

/**
 * 此类演示如何插入数据
 * 添加加入了事务
 */
public class InsertDemo extends MongoBase {

    /**
     * 构造方法
     *
     * @param mongoClient
     */
    public InsertDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 单个数据添加
     */
    public void insertOneDocument(User user) {
        //从基类中拿到db
        MongoDatabase dataBase = this.getDefaultDataBase();


        //拿到coll
        MongoCollection<User> coll = dataBase.getCollection("test",User.class);

        //获取session
        ClientSession session = this.mongoClient.startSession();

        try {
            //开启事务
            session.startTransaction();

            //添加数据
            coll.insertOne(session, user);


            //提交事务
            session.commitTransaction();

        } catch (Exception e) {

            e.printStackTrace();

            //出现异常回滚事务（事务会自动回滚，不过还是加上这一条语句）
            session.abortTransaction();

        } finally {

            //关闭session
            session.close();

        }
    }

    /**
     * 批量添加
     */
    public void insertManyDocument(List list) {
        //从基类中拿到db
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection coll = db.getCollection("test");

        //获取session
        ClientSession session = this.mongoClient.startSession();

        try {
            //开启事务
            session.startTransaction();

            //批量添加
            coll.insertMany(session, list);

            //提交事务
            session.commitTransaction();
        } catch (Exception e) {

            e.printStackTrace();
            //出现异常回滚
            session.abortTransaction();
        } finally {
            //关闭session
            session.close();
        }

    }

    /**
     * 测试
     */
    public static void main(String[] args) {
        MongoClient mongoClient = Utils.getMongoClient();
        InsertDemo insertDemo = new InsertDemo(mongoClient);

        //单条数据添加
        insertDemo.insertOneDocument(UserUtil.getUser());

        //批量添加
        //生成添加数据
        /*List list = new ArrayList();
        for (int i = 0; i < 10; i++) {
            //从工具类中拿到有值的user
            User user = UserUtil.getUser();

            Document document = JSONObject.parseObject(JSON.toJSONString(user), Document.class);

            list.add(document);
        }

        insertDemo.insertManyDocument(list);*/

    }

}
