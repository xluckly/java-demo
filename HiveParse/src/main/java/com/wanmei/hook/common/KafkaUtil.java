package com.wanmei.hook.common;

import com.google.gson.Gson;
import com.google.inject.internal.cglib.core.$DebuggingClassWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/7/16 19:13
 */
public class KafkaUtil {

    public static final String broker_list = "10.2.45.108:9092";
    public static final String topic = "first";  // kafka topic
    public static KafkaProducer producer;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.timeout.ms",30000);
        producer = new KafkaProducer<String, String>(props);
        System.out.println("static init");
    }

    public static void writeTokafka(String str) throws InterruptedException {


        try {
            System.out.println("send ");
            Long begin = System.currentTimeMillis();
            producer.send(new ProducerRecord<String, String>(topic, str));
            Long end = System.currentTimeMillis();

            System.out.println("send success time: " + (end - begin));
        } catch (Exception e) {
            System.out.println(e);
        }


    }

}


