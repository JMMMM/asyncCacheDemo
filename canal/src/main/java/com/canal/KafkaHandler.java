package com.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.kafka.KafkaProducer;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import kafka.utils.Json;

import java.util.Arrays;
import java.util.List;

public class KafkaHandler implements BinlogHandler {

    private KafkaProducer producer;

    public KafkaHandler() {
        producer = new KafkaProducer();
    }

    @Override
    public void process(List<CanalEntry.Entry> entries) {
        entries.stream().filter((entry) -> filter(entry)).forEach((entry) -> {
            try {
                CanalEntry.RowChange row = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                producer.send("async_cache", "", row.toByteArray());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        });
    }

    private boolean filter(CanalEntry.Entry entry) {
        List<CanalEntry.EntryType> filters = Arrays.asList(CanalEntry.EntryType.TRANSACTIONBEGIN, CanalEntry.EntryType.TRANSACTIONEND);
        return !filters.contains(entry.getEntryType());
    }
}
