package com.jinmu.mongodb.demo.entity;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * User实体类
 * {
 *   name:"abc",
 *   age:18 随机数,
 *   address:[
 *   {
 *    street:"xxx",
 *    road:"xxx"
 *   },
 *   {
 *   address:"unknown"
 *   },
 *   "unknown"],
 *   ABC:{
 *   "s":2,
 *   "b":4
 *   },
 *   date: ISODate("2020-1-1")随机日期,
 *   count:100<random int>
 * }
 */
public class User {
    private String name;
    private Integer age;
    private List<Object> address;
    private Map<String,Object> abc;
    private Date date;
    private Integer count;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public List<Object> getAddress() {
        return address;
    }

    public void setAddress(List<Object> address) {
        this.address = address;
    }

    public Map<String, Object> getAbc() {
        return abc;
    }

    public void setAbc(Map<String, Object> abc) {
        this.abc = abc;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", address=" + address +
                ", abc=" + abc +
                ", date=" + date +
                ", count=" + count +
                '}';
    }
}
