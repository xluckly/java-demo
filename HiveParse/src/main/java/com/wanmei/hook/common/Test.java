package com.wanmei.hook.common;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/7/17 14:15
 */
public class Test {

    public static void main(String[] args) {
        try {
            KafkaUtil.writeTokafka("test");
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
