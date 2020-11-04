package utils;

import com.mongodb.client.*;
import org.bson.Document;

/**
 * MongoDB工具类
 */
public class MongoDBUtil {
    /**
     * 不通过认证获取连接
     */
    public static MongoCollection getBasicConnect(){
        //获取连接，默认主机名是localhost，端口号是27017，协议名是：mongodb
        //MongoClient mongoClient = MongoClients.create("mongodb://host1:27017");    Or   MongoClient mongoClient = MongoClients.create( "mongodb://localhost:27017" );
        //或者连接到多个实例
        //MongoClient mongoClient = MongoClients.create("mongodb://host1:27017,host2:27018");
        MongoClient mongoClient = MongoClients.create();

        //获取Database
        MongoDatabase dataBase = mongoClient.getDatabase("demo");
            //获取全部集合名
            //MongoIterable<String> listCollectionNames = dataBase.listCollectionNames();

        //获取collection（文档）
        MongoCollection<Document> coll = dataBase.getCollection("test");

        return coll;
    }

    /**
     * 通过认证获取连接
     */
    public static MongoCollection getAuthConnection(){
        //获取连接
        MongoClient mongoClient = MongoClients.create("mongodb://user:password@27017");

        //获取dataBase
        MongoDatabase dataBase = mongoClient.getDatabase("demo");

        //获取collection（文档）
        MongoCollection<Document> coll = dataBase.getCollection("test");

        return coll;
    }
}
