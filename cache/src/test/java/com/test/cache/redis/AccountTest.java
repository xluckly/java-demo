package com.test.cache.redis;

import com.test.cache.CacheApplication;
import com.test.cache.ehcache.service.AccountService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/12 10:56
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {CacheApplication.class})
public class AccountTest {

    @Autowired
    private AccountService service;

    @Test
    public void test() throws Exception {

        service.saveAccountByName("abc");
        service.getAccountByName("abc");
    }
}

