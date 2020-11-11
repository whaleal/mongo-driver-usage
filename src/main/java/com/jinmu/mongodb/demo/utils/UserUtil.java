package com.jinmu.mongodb.demo.utils;


import com.jinmu.mongodb.demo.entity.User;

import java.util.*;

/**
 * User工具类
 */
public class UserUtil {
    /**
     * 获取一个有值的user
     */
    public static User getUser(){
        User user = new User();
        //姓名
        user.setName("abc");
        //年龄
        Random random = new Random();
        user.setAge(random.nextInt(100));
        //address
        List address = new ArrayList();
        //添加address里的streetMap
        Map streetMap = new HashMap();
        streetMap.put("street","xxx");
        streetMap.put("rod","xxx");
        address.add(streetMap);
        //添加address里的addressMap
        Map addressMap = new HashMap();
        addressMap.put("address","unknown");
        address.add(addressMap);
        //添加address里的unknown
        address.add("unknown");
        user.setAddress(address);
        //ABC
        Map abc = new HashMap();
        abc.put("s",2);
        abc.put("b",4);
        user.setAbc(abc);
        //count
        user.setCount(random.nextInt(200));

        return user;
    }
}
