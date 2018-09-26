package com;

import com.canal.CanalConnector;
import com.canal.KafkaHandler;
import com.kafka.KafkaProducer;

public class CacheServer {
    public static void main(String[] args) {
        CanalConnector canal = new CanalConnector();
        canal.init();
        canal.registerHandler(new KafkaHandler());
        canal.start();
    }
}
