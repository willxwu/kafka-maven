package com.yanxizhu.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 生产者的使用：同步\异步发送消息
 */
public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Map<String, Object> configs = new HashMap<>();
        //初始化链接到broker地址
        configs.put("bootstrap.servers", "192.168.56.30:9092");
        //key的序列化器
        configs.put("key.serializer", IntegerSerializer.class);
        //value的序列化器
        configs.put("value.serializer", StringSerializer.class);

        //消极确认机制
        configs.put("acks", "all");
        //重试次数
        configs.put("reties", "3");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        //自定义消息头
        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader("token", "produce.yanxizhu.com".getBytes()));

        //发送消息组装
        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "topic_1",
                0,
                0,
                "Hello Word",
                headers
        );

/*        //生产者：消息同步发送消息
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get();
        //打印发送信息后，返回结果
        System.out.println("消息主题:"+metadata.topic());
        System.out.println("消息分区:"+metadata.partition());
        System.out.println("消息偏移量:"+metadata.offset());*/


        //生产者：消息异步确认
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                if (null != exception) {
                    //打印发送信息后，返回结果
                    System.out.println("消息主题:" + recordMetadata.topic());
                    System.out.println("消息分区:" + recordMetadata.partition());
                    System.out.println("消息偏移量:" + recordMetadata.offset());
                } else {
                    System.out.println("异步消息发送异常:" + exception.getMessage());
                }
            }
        });
        //最后关闭消费者
        producer.close();
    }
}
