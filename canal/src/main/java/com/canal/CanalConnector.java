package com.canal;

import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;

public class CanalConnector {

    private final com.alibaba.otter.canal.client.CanalConnector connector;
    private final CanalEntity entity;
    private volatile boolean running = false;

    public CanalConnector() {
        this.entity = CanalEntity.getInstance();
        SocketAddress socketAddress = new InetSocketAddress(entity.canalHostIp, entity.canalHostPort);
        this.connector = CanalConnectors.newSingleConnector(socketAddress, entity.canalDesignation, entity.mysqlUserName, entity.mysqlPassword);

    }

    public void init() {
        connector.connect();
        connector.subscribe();
        running = connector.checkValid();
    }

    public void start() {
        if (!connector.checkValid()) init();
        while (running) {
            Message message = connector.getWithoutAck(entity.batchSize);
        }

    }
}
