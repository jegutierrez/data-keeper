package io.jegutierrez.db;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseRepository {
    private static final Logger log = LoggerFactory.getLogger(DatabaseRepository.class);
    private final ConcurrentMap<String, String> kvs;

    public DatabaseRepository() {
        kvs = new ConcurrentHashMap<>();
    }

    public void put(String key, String data) {
        kvs.put(key, new String(data));
    }

    public String get(String key) {
        return kvs.getOrDefault(key, null);
    }

    public Map<String, String> getDataToSync() {
        HashMap<String, String> copy = new HashMap<>();
        copy.putAll(kvs);
        return copy;
    }

    public void syncData(Map<String, String> data) {
        for (String key : data.keySet()) {
            log.info(key + ":" + new String(data.get(key)));
            kvs.put(key, data.get(key));
        }
    }
}