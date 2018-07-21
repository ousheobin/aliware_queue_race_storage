/*
 * Copyright (C) 2018. Shaobin.Ou
 * All rights reserved
 */

package io.openmessaging.storage.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUMap<K,V> {

    private static final float HASH_LOAD_FACTORY = 0.75f;
    private LinkedHashMap<K,V> map;
    private int size;

    public LRUMap(int size) {
        this.size = size;
        int capacity = (int) Math.ceil( this.size / HASH_LOAD_FACTORY ) + 1;
        map = new LinkedHashMap<K,V>(capacity, HASH_LOAD_FACTORY, true){
            private static final long serialVersionUID = 1;

            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > LRUMap.this.size;
            }
        };
    }

    public V get(K key) {
        return map.get(key);
    }

    public void put(K key, V value) {
        synchronized(map){
            map.put(key, value);
        }
    }


}