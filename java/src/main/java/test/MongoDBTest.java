package test;

import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import entity.User;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import utils.DateUtil;
import utils.MongoDBUtil;
import utils.UserUtil;

import java.util.*;
import java.util.logging.Filter;

/**
 * CRUD测试类
 */
public class MongoDBTest {
    private MongoCollection coll;

    /**
     * Before给coll赋值
     */
    @Before
    public void before(){
        coll = MongoDBUtil.getBasicConnect();
    }


    /**
     * 单个数据添加测试
     */
    @Test
    public  void insertOneTest() {

        //从工具类中拿到有值的user对象
        User user = UserUtil.getUser();

        //因为insertOne插入不支持实体对象形式数据，所以先转换成map
        Map map = JSON.parseObject(JSON.toJSONString(user), Map.class);

        //创建文档对象
        Document document = new Document(map);

        //将数据添加到mongodb数据库中
        coll.insertOne(document);
    }


    /**
     * 批量数据添加测试
     */
    @Test
    public  void insertManyTest() {

        List list = new ArrayList();

        for (int i = 0; i < 10; i++) {
            User user = UserUtil.getUser();
            Map map = JSON.parseObject(JSON.toJSONString(user), Map.class);
            Document document = new Document(map);

            list.add(document);
        }

        coll.insertMany(list);
    }

    /**
     * 查询
     */
    @Test
    public  void findTest(){
        //find()查询全部，find(Bson bson)根据条件查询
        Document document = new Document("_id",new ObjectId("5fa124926c856a42bb8d9b9e"));
        FindIterable iterable = coll.find(document);
        for (Object obj:
        iterable) {
            System.out.println(obj);
        }
    }

    /**
     * 修改，只会修改单个值，即使符合条件的数据有很多，只会改第一条
     * 　$inc　:  增加一个指定值
     *   $mul　　　 　　　　乘以一个指定值
     *   $rename　　　 　　 重命名　　　　　　
     * 　$setOnInsert　　　  更新操作对现有的文档没有影响，而是新插入了一个文档，则在这新插入的文档中加上一个指定字段
     *   $set　　　　　　　　修改值
     *   $unset　　　　　　  删除文档中指定字段
     *   $min　　　　　　　  更新文档中小于指定值的字段
     *   $max　　　　　　　 更新文档中大于指定值的字段
     *   $currentDate　　　 设置当前时间，日期或者时间戳
     */
    @Test
    public void updateOneTest(){
        User user = UserUtil.getUser();
        user.setName("XXX");
        Map map = JSON.parseObject(JSON.toJSONString(user), Map.class);

        coll.updateOne(Filters.eq("_id",new ObjectId("5fa1636d09ed1a3d5d48a97e")),new Document("$set",map));
        }

    /**
     * 修改符合条件的数据（修改多条）
     */
    @Test
     public void updateManyTest(){
         User user = UserUtil.getUser();
         user.setName("abc");
         Map map = JSON.parseObject(JSON.toJSONString(user), Map.class);
         coll.updateMany(Filters.eq("name","XXX"),new Document("$set",map));
        }

    /**
     * 单个删除，只删除一条，即使符合条件也只删除第一条
      */
    @Test
    public void deleteOneTest(){
        coll.deleteOne(Filters.eq("name","abc"));
    }

    /**
     * 批量删除，删除符合条件的所有数据
     */
    @Test
    public void deleteManyTest(){
        coll.deleteMany(Filters.eq("name","abc"));
    }



}


