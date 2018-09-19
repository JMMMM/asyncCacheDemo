package com.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

public interface BinlogHandler {
    void process(List<CanalEntry.Entry> entries);
}
