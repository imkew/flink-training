package org.apache.flink.connectors.mongodb.utils;

import java.io.Serializable;

/**
 * @Author: kewang
 * @Date: 2022/4/18 15:48
 */
public class MongodbConf implements Serializable {
    private String database;
    private String collection;
    private String uri;
    private int maxConnectionIdleTime;

    public MongodbConf(String database, String collection, String uri, int maxConnectionIdleTime) {
        this.database = database;
        this.collection = collection;
        this.uri = uri;
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    public String getDatabase() {
        return this.database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCollection() {
        return this.collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getUri() {
        return this.uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public int getMaxConnectionIdleTime() {
        return this.maxConnectionIdleTime;
    }

    public void setMaxConnectionIdleTime(int maxConnectionIdleTime) {
        this.maxConnectionIdleTime = maxConnectionIdleTime;
    }

    @Override
    public String toString() {
        return "MongodbConf{database='" + this.database + '\'' + ", collection='" + this.collection + '\'' + ", uri='" + this.uri + '\'' + ", maxConnectionIdleTime=" + this.maxConnectionIdleTime + '}';
    }
}

