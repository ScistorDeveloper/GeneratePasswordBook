package com.scistor.process.operator;

import com.scistor.process.record.Record;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public interface TransformInterface {

    void init(Map<String, String> config, ArrayBlockingQueue<Record> queue);
    List<String> validate();//参数校验
    void producer();//该算子数据入Kafka的逻辑
    void consumer();//从Kafka中都该算子数据并做合并的逻辑

}
