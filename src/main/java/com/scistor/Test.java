package com.scistor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Created by Administrator on 2017/11/15.
 */
public class Test {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties props = new Properties();
		props.put("producer.type","sync");
		props.put("bootstrap.servers", "172.16.18.228:9092,172.16.18.229:9092,172.16.18.234:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("request.required.acks", "1");
		KafkaProducer producer = new KafkaProducer<String, String>(props);

		String line = "www.baidu.com" + "||" + "123456" + "||" + "123456";
		ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>("5d1434d9-0e20-4d91-a3cd-b5078a7ef849", UUID.randomUUID().toString(), line);
		producer.send(kafkaRecord).get();
		System.out.println(String.format("一条数据[%s]已经写入Kafka, topic:[%s]", line, "5d1434d9-0e20-4d91-a3cd-b5078a7ef849"));



	}
}
