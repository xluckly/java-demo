package com.test.cache.ehcache;


import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/11 17:30
 */

@Slf4j

public class EhcacheDemo {
    private static CacheManager cacheManager;
    static {
        String path = EhcacheDemo.class.getResource("/").toString() + "/ehcache.xml";
        String[] tmp = path.split("\\:");
        cacheManager = new CacheManager(tmp[1]);


    }
    public void print() {
        cacheManager.addCacheIfAbsent("first");
        Cache cache = cacheManager.getCache("first");
        cache.put(new Element("hello", "word"));
        Element element = cache.get("hello");

        log.info("第一个元素是 " + element);

    }

}
