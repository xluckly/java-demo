package com.test.cache;

import com.test.cache.guavacache.GuavaCacheDemo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication(scanBasePackages = "com")
@EnableCaching
public class CacheApplication {

    public static void main(String[] args) {

        GuavaCacheDemo ehcacheDemo = (GuavaCacheDemo)SpringApplication.run(CacheApplication.class, args).getBean("guavaCacheDemo");
        GuavaCacheDemo.put("fisr", "1");

    }

}
