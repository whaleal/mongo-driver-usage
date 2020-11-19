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
                                .append("minAge", new Document("$min", "$age"))
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
     *先将日期截取到月份，再根据日期分组并且计算count列的总数
     * 最后降序
     * db.collectionName.aggregate([{$project,{day:{$substr:["$date",0,7]}],count:"$count"}},{$group:{_id:"$day",countTotal:{$sum:"$count"}}},{$sort:{"_id":-1}}])
     */
    public Iterable queryMonthCount(){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        //list第一个参数
                        new Document("$project",
                                new Document("day",
                                        new Document("$substr",
                                                Arrays.asList("$date", 0, 7)
                                        )
                                ).append("count","$count")
                        ),
                        //第二个参数
                        new Document("$group",
                                new Document("_id", "$day")
                                        .append("countTotal",
                                                new Document("$sum", "$count")
                                               )
                                    ),
                        //第三个参数
                        new Document("$sort",new Document("_id",-1))
                            )
        );
        return iterable;

    }

    /**
     * 每天的数据量
     * 先将date使用substr方法截取到day，再根据date分组，同时计算出每组总数
     * 最后降序
     * db.collectionName.aggregate([{$project:{day:{$substr:["$date",0,10]}}},{$group:{_id:"$day",number:{$sum:1}}}])
     */
    public Iterable queryDayTotal(){
        MongoDatabase db = this.getDefaultDataBase();

        MongoCollection<Document> coll = db.getCollection("test");

        AggregateIterable<Document> iterable = coll.aggregate(
                Arrays.asList(
                        //list第一个参数
                        new Document("$project",
                                new Document("day",
                                        new Document("$substr", Arrays.asList("$date", 0, 10))
                                )
                        ),
                        //第二个参数
                        new Document("$group",
                                new Document("_id", "$day")
                                        .append("number",
                                            new Document("$sum", 1)
                                               )
                        ),
                         //第三个参数
                        new Document("$sort",
                                new Document("_id",-1)
                        )
        ));

        return iterable;
    }

    /**
     * 这里要测试的方法有点多，暂时没找到更好的测试方法
     * @param args
     */
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

        //每个月的count字段的总数是多少
        Iterable iterable9 = aggregateDemo.queryMonthCount();

        //每天数据量
        Iterable iterable8 = aggregateDemo.queryDayTotal();

        for (Object o:
             iterable9) {
            System.out.println(o);
        }
    }

}
