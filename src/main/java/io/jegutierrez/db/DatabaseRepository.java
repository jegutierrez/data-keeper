package io.jegutierrez.db;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DatabaseRepository {
    private final ConcurrentMap<String, byte[]> kvs;

    public DatabaseRepository() {
        kvs = new ConcurrentHashMap<>();
    }
    public void put(String key, byte[] data) {
        kvs.put(key, data.clone());
    }

    public byte[] get(String key) {
        return kvs.getOrDefault(key, null);
    }
}