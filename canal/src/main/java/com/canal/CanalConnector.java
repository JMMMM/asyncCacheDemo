package com.canal;

import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CanalConnector {

    private final com.alibaba.otter.canal.client.CanalConnector connector;
    private final CanalEntity entity;
    private volatile boolean running = false;

    private final List<BinlogHandler> handlers;

    public CanalConnector() {
        this.entity = CanalEntity.getInstance();
        SocketAddress socketAddress = new InetSocketAddress(entity.canalHostIp, entity.canalHostPort);
        this.connector = CanalConnectors.newSingleConnector(socketAddress, entity.canalDesignation, entity.mysqlUserName, entity.mysqlPassword);
        handlers = new ArrayList<>();
    }

    public void init() {
        connector.connect();
        connector.subscribe();
        running = connector.checkValid();
    }

    public void registerHandler(BinlogHandler binlogHandler) {
        handlers.add(binlogHandler);
    }

    public void removeHandler(BinlogHandler binlogHandler) {
        handlers.remove(binlogHandler);
    }

    public void start() {
        if (!connector.checkValid()) init();
        while (running) {
            //阻塞直到有数据返回
            Message message = connector.getWithoutAck(entity.batchSize);
            long batchId = message.getId();
            long batchSize = message.getEntries().size();
            if (batchId != -1 && batchSize != 0) {
                List<CanalEntry.Entry> entries = message.getEntries();
                handlers.forEach((handler) -> handler.process(entries));
                connector.ack(batchId);
            } else {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }


    public static void main(String[] args) {
        CanalConnector connector = new CanalConnector();
        connector.init();
        connector.registerHandler((entries) -> {
            entries.forEach(entry -> {
                System.out.println(entry.getEntryType());
            });
        });
        connector.start();
    }
}