package com.test.cache.ehcache.bean;

import lombok.Getter;
import lombok.Setter;

/**
 * @author hanqingkuo@pwrd.com
 * @date 2020/6/11 17:05
 */

@Setter
@Getter
public class Acount {
    private int id;
    private String name;
    public Acount(String name) {
        this.name = name;
    }


}
