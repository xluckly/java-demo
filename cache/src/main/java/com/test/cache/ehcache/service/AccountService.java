package com.test.cache.ehcache.service;

import com.test.cache.ehcache.bean.Acount;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/11 18:18
 */
@Slf4j
@Service
public class AccountService {
    @Cacheable(value = "accountCache")
    public Acount getAccountByName(String accountName) throws Exception {
        log.info("==缓存" + accountName + "进行到此刻===");
        Acount account = getFromDB(accountName);

        if (account == null) {
            throw new Exception("account is null");
        } else {
            return account;
        }
    }

    public Acount getFromDB(String accountName) {
        log.info("从数据库获取数据并且返回!");
        return new Acount(accountName);
    }

    @CachePut(value = "accountCache", key = "#accountName")
    public void saveAccountByName(String accountName) {
        log.info("缓存数据 " + accountName + "已经保存成功");
    }
}
