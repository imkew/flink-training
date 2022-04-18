package org.apache.flink.connectors.mongodb.sink;

import org.apache.flink.connectors.mongodb.utils.MongodbConf;

/**
 * @Author: kewang
 * @Date: 2022/4/18 15:49
 */
public class MongodbSinkConf extends MongodbConf {
    private final int batchSize;

    public MongodbSinkConf(String database, String collection, String uri, int maxConnectionIdleTime, int batchSize) {
        super(database, collection, uri, maxConnectionIdleTime);
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return this.batchSize;
    }

    @Override
    public String toString() {
        return "MongodbSinkConf{" + super.toString() + "batchSize=" + this.batchSize + '}';
    }
}

