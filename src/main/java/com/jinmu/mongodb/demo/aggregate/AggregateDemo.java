package com.jinmu.mongodb.demo.aggregate;

import com.jinmu.mongodb.demo.MongoBase;
import com.jinmu.mongodb.demo.Utils;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.xml.crypto.dom.DOMCryptoContext;
import java.util.Arrays;

/**
 * 这是演示聚合的方法
 */
public class AggregateDemo extends MongoBase {

    /**
     * 构造方法
     *
     * @param mongoClient
     */
    public AggregateDemo(MongoClient mongoClient) {
        super(mongoClient);
    }

    /**
     * 查询出age大于70的user，age $gt 70
     * $match,在mysql中是while，如果用在$group后面就是having
     * db.collectionName.aggregate([{$match:{age:{$gt:70}}}])
     */
    public Iterable queryUserByAge() {

        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        //aggregate方法参数需要list，用Arrays的asList方法将document对象转成list
        //一个{}就代表一个document对象
        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        new Document("$match",
                                new Document("age",
                                        new Document("$gt", 70)
                                )
                        )
                )
        );

        return iterable;

    }

    /**
     * 不分组求出age的总和
     * $group中的_id表示的是由哪个字段分组，是必须的，
     * totalAge是别名，后面接$sum函数，$age表示字段，如果是求总数（比如user的总人数）的话也是$sum函数，但是后面不是接的字段，而是数字1
     * db.collectionName.aggregate([{$group:{_id:null,totalAge:{$sum:"$age"}}}])
     */
    public Iterable queryTotalAgeByNotGroup() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        new Document("$group",
                                new Document("_id", null)
                                        .append("totalAge",
                                                new Document("$sum", "$age")
                                        )
                        )
                )
        );

        return iterable;

    }

    /**
     * 以name分组求出age的总和
     * db.collectionName.aggregate([{$group:{_id:$name,totalAge:{$sum:"$age"}}}])
     */
    public Iterable queryTotalAgeByGroup() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(Arrays.asList(
                new Document("$group",
                        new Document("_id", "$name")
                                .append("totalAge",
                                        new Document("$sum", "$age")))
        ));


        return iterable;
    }

    /**
     * 以name分组求出age的总和并且过滤掉totalAge小于等于0的数据
     * db.collectionName.aggregate([{$group:{_id:$name,totalAge:{$sum:"$age"}}},{$match:{totalAge:{$gt:0}}}])
     * 这里的$match相当与mysql的having，因为它是在$group后面执行的
     */
    public Iterable queryTotalAgeByGroupAndTotalAge() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        //list的第一个参数$group
                        new Document("$group",
                                new Document("_id", "$name")
                                        .append("totalAge",
                                                new Document("$sum", "$age")
                                        )
                        ),
                        //第二个参数$match
                        new Document("$match",
                                new Document("totalAge",
                                        new Document("$gt", 0)
                                )
                        )
                )
        );

        return iterable;
    }

    /**
     * 先过滤掉age小于等于70的user，再将结果根据name分组求age总和
     * db.collectionName.aggregate([{$match:{age:{$gt:70}}},{$group:{_id:"$name",totalAge:{$sum:"$age"}}}])
     * 这里的match相当与mysql的while
     */
    public Iterable queryTotalAgeByTotalAgeAndGroup() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        //list第一个参数$match，过滤age <= 70的user
                        new Document("$match",
                                new Document("age",
                                        new Document("$gt", 70)
                                )
                        ),
                        //第二个参数$group，将过滤结果以name字段分组，并且计算出age的总数
                        new Document("$group",
                                new Document("_id", "$name")
                                        .append("totalAge",
                                                new Document("$sum", "$age")
                                        )
                        )
                )
        );

        return iterable;
    }

    /**
     * 求年龄最大的人
     * db.collectionName.aggregate([{$group:{_id:null,maxAge:{$max:"$age"}}}])
     */
    public Iterable maxAge() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        new Document("$group",
                                new Document("_id", null)
                                        .append("maxAge",
                                                new Document("$max", "$age")
                                        )
                        )
                )
        );

        return iterable;
    }

    /**
     * 查询年龄最小的人
     * db.collectionName.aggregate([{$group:{_id:null,minAge:{$min:"$age"}}}])
     */
    public Iterable minAge() {
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(Arrays.asList(
                new Document("$group",
                        new Document("_id", null)
                                .append("minAge",
                                        new Document("$min", "$age")
                                )
                        )
                )
        );

        return iterable;
    }

    /**
     * 查询出年龄的平均值
     * db.collectionName.aggregate([{$group:{_id:null,avgAge:{$avg:"$age"}}}])
     */
    public Iterable avgAge(){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        new Document("$group",
                                new Document("_id", null)
                                        .append("avgAge",
                                                new Document("$avg", "$age")
                                        )
                        )
                )
        );
        return iterable;
    }

    /**
     * 每个月的count字段的总数是多少
     * db.collectionName.aggregate([{$project:{_id:0,month:{$month:"$date"},count:"$count"}}])
     * db.collectionName.aggregate([{$match:{_id:0,{$month:"$date"}:{$eq:}}}])
     */
    public void queryMonthCount(){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        new Document("$match",new Document("_id",null)),
                        new Document("$project",
                                new Document("_id", 0)
                                        .append("month",
                                                new Document("$month", "$date"))
                                        .append("count", "$count")
                        )
                )
        );
       // AggregateIterable<Document> iterable = coll.aggregate(Arrays.asList(new Document("$match", new Document("_id", 0).append("month", new Document("$eq", new Document("$month", "$date"))))));

        for (Object o:
             iterable) {
            System.out.println(o);
        }

    }


    public static void main(String[] args) {
        //从utils获取mongoClient
        MongoClient mongoClient = Utils.getMongoClient();

        //创建aggregateDemo
        AggregateDemo aggregateDemo = new AggregateDemo(mongoClient);

        //测试$match()
        //查询出age大于70的user
        Iterable iterable7 = aggregateDemo.queryUserByAge();

        //测试$group()
        //不分组求出age的总和
        Iterable iterable = aggregateDemo.queryTotalAgeByNotGroup();

        //以name分组求出age的总和
        Iterable iterable1 = aggregateDemo.queryTotalAgeByGroup();

        //以name分组求出age的总和并且过滤掉totalAge小于等于0的数据
        Iterable iterable2 = aggregateDemo.queryTotalAgeByGroupAndTotalAge();

        //先过滤掉age小于等于70的user，再将结果根据name分组求age总和
        Iterable iterable3 = aggregateDemo.queryTotalAgeByTotalAgeAndGroup();

        //查询出年龄最大的人
        Iterable iterable4 = aggregateDemo.maxAge();

        //查询出年龄最小的人
        Iterable iterable5 = aggregateDemo.minAge();

        //查询出user年龄的平均值
        Iterable iterable6 = aggregateDemo.avgAge();

        //
        aggregateDemo.queryMonthCount();
    }

}
