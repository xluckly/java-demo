package com.test.cache.redis;


import com.test.cache.CacheApplication;
import com.whalin.MemCached.MemCachedClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;

/**
 * @author chao.cheng
 * @createTime 2020/5/25 3:01 下午
 * @description
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CacheApplication.class})
@Slf4j
public class MemcachedCommandTest {

    @Autowired
    private MemCachedClient memCachedClient;

    @Test
    public void test() throws Exception {
        memCachedClient.set("flagKey", 1);
        Object flagValue = memCachedClient.get("flagKey");
        log.info("flagValue:"+flagValue);

        memCachedClient.set("testKey","1",new Date(1000));
        Thread.sleep(1000);
        Object testValue = memCachedClient.get("testKey");
        log.info("testValue:"+testValue);

        memCachedClient.set("flagKey",10);
        log.info(memCachedClient.get("flagKey").toString());

        memCachedClient.replace("flagKey","20");
        log.info("replace flagKey:"+memCachedClient.get("flagKey"));

        memCachedClient.delete("flagKey");
        log.info("delete flagKey:"+memCachedClient.get("flagKey"));

    }


}
