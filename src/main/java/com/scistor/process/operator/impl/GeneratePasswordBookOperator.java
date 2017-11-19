package com.scistor.process.operator.impl;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import com.scistor.process.operator.TransformInterface;
import com.scistor.process.pojo.SlavesLocation;
import com.scistor.process.record.Record;
import com.scistor.process.utils.ErrorUtil;
import com.scistor.process.utils.RunningConfig;
import com.scistor.process.utils.TopicUtil;
import com.scistor.process.utils.ZKOperator;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class GeneratePasswordBookOperator implements TransformInterface, RunningConfig
{

    private static final Log LOG = LogFactory.getLog(GeneratePasswordBookOperator.class);
    private static final Integer ZK_SESSION_TIMEOUT = 24*60*60*1000; //24hour
    private String zookeeper_addr;
    private String topic;
    private String taskId;
    private String mainclass;
    private String task_type;
    private ArrayBlockingQueue<Record> queue;
    private String PRODUCE_PATH ;
    private String broker_list;
    private KafkaProducer producer;
    private ConsumerConnector consumer;
    private Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    private Map<String, Set<Map<String, String>>> records = new HashMap<String, Set<Map<String, String>>>();
    private List<Integer> slavesPort = new ArrayList<Integer>();
    private List<String> slavesIP = new ArrayList<String>();

    @Override
    public void init(Map<String, String> config, ArrayBlockingQueue<Record> queue) {
        this.zookeeper_addr = config.get("zookeeper_addr");
        this.broker_list = config.get("broker_list");
        this.topic = config.get("topic");
        this.taskId = config.get("taskId");
        this.task_type = config.get("task_type");
        this.mainclass = config.get("mainclass");
        this.queue = queue;
        this.PRODUCE_PATH = config.get("PRODUCE_PATH");

        Properties props = new Properties();
        if (task_type.equals("producer")) {
            props.put("producer.type","sync");
            props.put("bootstrap.servers", broker_list);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("request.required.acks", "1");
            producer = new KafkaProducer<String, String>(props);
            TopicUtil.createTopic(zookeeper_addr, topic);
        } else if (task_type.equals("consumer")) {
            props.put("zookeeper.connect", zookeeper_addr);
            props.put("auto.offset.reset","smallest");
            props.put("group.id", "HS");
            props.put("zookeeper.session.timeout.ms", "86400000");
            props.put("zookeeper.sync.time.ms", "5000");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "5000");
            topicCountMap.put(topic, 1);
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        }
    }

    @Override
    public List<String> validate() {
        List<String> errorInfo = new ArrayList<String>();
        final CountDownLatch cdl = new CountDownLatch(1);
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(zookeeper_addr, ZK_SESSION_TIMEOUT, new Watcher(){
                @Override
                public void process(WatchedEvent event) {
                    if(event.getState() == Event.KeeperState.SyncConnected){
                        cdl.countDown();
                    }
                }
            });
        } catch (IOException e) {
            LOG.error(e);
            errorInfo.add(String.format("任务:[%s], 算子:[%s]校验失败，原因:[zookeeper 未能连接]， zookeeper 地址:[%s]", taskId, mainclass, zookeeper_addr));
            return errorInfo;
        }
        return null;
    }

    @Override
    public void producer() {
        boolean is_continue =true;
        ZooKeeper zookeeper = null;
        try {
            while(is_continue){
                if(queue.size() > 0) {
                    Map<String, String> record = (Map<String, String>) queue.take();
                    String host = record.get("host");
                    String username = record.get("username");
                    String password = record.get("password");
                    if (!isBlank(host) && !isBlank(username) && !isBlank(password)) {
                        String line = host + "||" + username + "||" + password;
                        ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), line);
                        producer.send(kafkaRecord).get();
                        LOG.info(String.format("一条数据[%s]已经写入Kafka, topic:[%s]", line, topic));
                    }
                }else {
                    //检查数据解析线程是不已经完成
                    zookeeper = ZKOperator.getZookeeperInstance(zookeeper_addr);
                    if(!ZKOperator.checkPath(zookeeper, PRODUCE_PATH)){
                        LOG.info("数据生产结束，退出");
                        is_continue=false;
                        zookeeper.close();
                    }else{
                        Thread.sleep(1000);
                        zookeeper.close();
                    }
                }
            }
        }
        catch (Exception e){
            ErrorUtil.ErrorLog(LOG, e);
        } finally {
            producer.close();
        }
    }

    @Override
    public void consumer() {

        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(topic);

        Thread[] threads = new Thread[msgStreamList.size()];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new HanldMessageThread(msgStreamList.get(i)));
            threads[i].start();
        }

        System.out.println(String.format("Number of thread is [%s]", threads.length));

        List<Long> lastRunnableTimeOrigin = new ArrayList<Long>(threads.length);
        for (int i = 0; i < threads.length; i++) {
            long currentTime = System.currentTimeMillis();
            lastRunnableTimeOrigin.add(currentTime);
        }

        List<Long> lastRunnableTime = new ArrayList<Long>(threads.length);
        for (int i = 0; i < threads.length; i++) {
            long currentTime = System.currentTimeMillis();
            lastRunnableTime.add(currentTime);
        }

        ZooKeeper zookeeper = null;
        try {
            zookeeper = ZKOperator.getZookeeperInstance(zookeeper_addr);
            getSlaves();
            while(true) {
                boolean b = true;
                for (int i = 0; i < slavesIP.size(); i++) {
                    b = b & !(ZKOperator.checkPath(zookeeper, LIVING_SLAVES + "/" + slavesIP.get(i) + ":" + slavesPort.get(i) + "/" + taskId));
                }
                if (b) {
                    long start1 = System.currentTimeMillis();
                    long start = System.currentTimeMillis();
                    while (true) {
                        for (int i = 0; i < threads.length; i++) {
                            if (threads[i].getState().equals(Thread.State.RUNNABLE)) {
                                lastRunnableTime.set(i, System.currentTimeMillis());
                            }
                        }
                        long end = System.currentTimeMillis();
                        if (end - start > 1000) {
                            boolean flag = true;
                            for (int i = 0; i < threads.length; i++) {
                                if (lastRunnableTimeOrigin.get(i).equals(lastRunnableTime.get(i))) {
                                    flag = flag & true;
                                } else {
                                    flag = flag & false;
                                }
                            }
                            if (flag == true) {
                                File file = new File("/HS/res.txt");
                                BufferedWriter br = new BufferedWriter(new FileWriter(file));
                                Iterator<Map.Entry<String, Set<Map<String, String>>>> iterator = records.entrySet().iterator();
                                while (iterator.hasNext()) {
                                    Map.Entry<String, Set<Map<String, String>>> entry = iterator.next();
                                    JSONObject obj = new JSONObject();
                                    obj.put("Host", entry.getKey());
                                    obj.put("Book", entry.getValue());
                                    br.write(obj.toJSONString());
                                }
                                br.close();
                                break;
                            } else {
                                for (int i = 0; i < threads.length; i++) {
                                    long time = lastRunnableTime.get(i);
                                    lastRunnableTimeOrigin.set(i, time);
                                    start = System.currentTimeMillis();
                                }
                            }
                        }
                    }
                    if (consumer != null) {
                        consumer.shutdown();
                    }
                    long end1 = System.currentTimeMillis();
                    System.out.println(String.format("time waited:[%s]", end1 - start1));
                    break;
                }
            }
        } catch (IOException e) {
            ErrorUtil.ErrorLog(LOG, e);
        } finally {
            try {
                zookeeper.close();
                TopicUtil.delTopic(zookeeper_addr, topic);
            } catch (InterruptedException e) {
                ErrorUtil.ErrorLog(LOG, e);
            }
        }

    }

    private boolean isBlank(String str) {
        boolean flag = false;
        if (null == str || str.equals("")) {
            flag = true;
        }
        return flag;
    }

    class HanldMessageThread implements Runnable {

        private KafkaStream<byte[], byte[]> kafkaStream = null;

        public HanldMessageThread(KafkaStream<byte[], byte[]> kafkaStream) {
            super();
            this.kafkaStream = kafkaStream;
        }

        public void run() {
            System.out.println("111");
            ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
            while (iterator.hasNext()) {
                String message = new String(iterator.next().message());
                LOG.info(String.format("已经在Kafka topic:[%s], 消费一条数据:[%s]", topic, message));
                String[] split = message.split("\\|\\|");
                String host = split[0];
                String username = split[1];
                String password = split[2];
                Set<Map<String, String>> set = records.get(host);
                if (null == set) {
                    set = new HashSet<Map<String, String>>();
                    Map<String, String> record = new HashMap<String, String>();
                    record.put("username", username);
                    record.put("password", password);
                    set.add(record);
                    records.put(host, set);
                } else {
                    Map<String, String> record = new HashMap<String, String>();
                    record.put("username", username);
                    record.put("password", password);
                    set.add(record);
                    records.put(host, set);
                }
            }
            System.out.println("end.......");
        }
    }

    private void getSlaves() {
        ZooKeeper zookeeper = null;
        try {
            zookeeper = ZKOperator.getZookeeperInstance(zookeeper_addr);
            List<SlavesLocation> slavesLocation = ZKOperator.getLivingSlaves(zookeeper);
            for (SlavesLocation slaveLocation : slavesLocation) {
                slavesIP.add(slaveLocation.getIp());
                slavesPort.add(slaveLocation.getPort());
            }
        } catch (Exception e) {
            ErrorUtil.ErrorLog(LOG, e);
        }finally{
            if(!Objects.equal(zookeeper, null)){
                try {
                    zookeeper.close();
                } catch (InterruptedException e) {
                    ErrorUtil.ErrorLog(LOG, e);
                }
            }
        }
    }

    public static void main(String[] args) {
        GeneratePasswordBookOperator passwdBook = new GeneratePasswordBookOperator();
    }

}
