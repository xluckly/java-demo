package com.test.cache.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/12 10:20
 */

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RedisCommonTest {
    @Autowired
    StringRedisTemplate template;

    @Test
    public void test() {
        //String
        template.opsForValue().set("first","hello");
        log.info("set key first " + template.opsForValue().get("first") );
        template.opsForValue().set("second", "1");
        template.opsForValue().increment("second");
        log.info("second value " + template.opsForValue().get("second"));
        template.opsForValue().increment("second ", 2); //每次加2
        log.info("second value increment 2 " + template.opsForValue().get("second"));
        template.opsForValue().decrement("second");
        log.info("second value decrement 1" + template.opsForValue().get("second"));
        template.opsForValue().decrement("second", 2);
        // 每次减2
        log.info("second value decrement 2" + template.opsForValue().get("second"));

        //list
        template.opsForList().set("list", 1, "1");
        log.info("list " + template.opsForList().toString());

        template.opsForList().rightPush("list","2");
        log.info("list " + template.opsForList().toString());

        template.opsForList().rightPush("list","3", "4");
        log.info("list " + template.opsForList().toString());



    }
}
