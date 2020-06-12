package com.test.cache.guavacache;

import com.google.common.cache.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/11 18:04
 */
@Service
@Slf4j
public class GuavaCacheDemo {
    private static LoadingCache<String,String>cache = CacheBuilder.newBuilder()
            .maximumSize(20)
            .expireAfterAccess(20, TimeUnit.SECONDS)
            .removalListener(new RemovalListener<String, String>() {
                @Override
                public void onRemoval(RemovalNotification<String, String> removalNotification) {
                    log.info("有缓存数据被清除了：" + removalNotification.getKey() + "-" + removalNotification.getValue());
                }

            }).build(new CacheLoader<String, String>() {
                @Override
                public String load(String key) throws Exception {
                    log.info("数据缓存加载:" + key);
                    return "test_" + key;
                }
            });

    /**
     * 通过key获取缓存value
     * 如果key不存在，将调用CacheLoader的load方法再加载其他的数据
     *
     * @param key
     * @return
     */
    public static String get(String key) {
        try {
            String value = cache.get(key);
            log.info("get value:" + value);
            return value;
        } catch (Exception e) {
            log.error("get value is error! key:" + key, e);
        }
        return null;
    }

    /**
     * 移除缓存
     *
     * @param key
     */
    public static void remove(String key) {
        try {
            cache.invalidate(key);
        } catch (Exception e) {
            log.error("移除缓存数据失败 key:" + key, e);
        }
    }

    /**
     * 全部清空缓存
     */
    public static void removeAll() {
        try {
            cache.invalidateAll();
        } catch (Exception e) {
            log.error("全部清空缓存失败", e);
        }
    }

    /**
     * 保存缓存数据
     * 如果缓存中已经有该key，则会先移除这个缓存，这时候会触发RemovalListener监听器，触发之后，再添加这个key和value
     *
     * @param key
     * @param value
     */
    public static void put(String key, String value) {
        try {
            cache.put(key, value);
            log.info("缓存保存成功! key:" + key + " value:" + value);
        } catch (Exception e) {
            log.error("保存缓存数据失败 key:" + key + " value:" + value);
        }
    }

    /**
     * 查询缓存中所有数据的map
     * @return
     */
    public static ConcurrentMap<String, String> viewCaches() {
        log.info("缓存中存放数据的列表:" + cache.asMap());
        return cache.asMap();
    }

}
