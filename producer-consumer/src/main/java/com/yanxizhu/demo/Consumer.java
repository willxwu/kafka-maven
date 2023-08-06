package com.yanxizhu.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 消费者的使用:只拉取一次，没有使用while(true)
 */
public class Consumer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        //初始化链接到broker地址
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.30:9092");
        //key的反序列化器配置
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        //value的反序列化器配置
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //配置消费组ID(目的是为了避免消息被重复消费)
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-demo");
        //偏移量配置：
        //earliest:如果找不到当前消费者的有效偏移量，则自动重置到最开始
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(configs);

        //先订阅后消费
        consumer.subscribe(Arrays.asList("topic_1"));

        //消费消息
/*
        //如果主题中没有可以消费的消息，则该方法可以放到while中，没过3秒重新拉取一次
        //如果还没拉取到，过3秒再次拉取，防止while循环太密集的poll调用。
        while(true) {
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3_0000);
        }*/

        //批量从主题的分区拉取消息（没有使用while（true）只能拉取一次）
        ConsumerRecords<Integer, String> consumerRecords = consumer.poll(3_0000);

        //遍历本次从主题的分区中拉取到的批量消息
        consumerRecords.forEach(new java.util.function.Consumer<ConsumerRecord<Integer, String>>() {
            @Override
            public void accept(ConsumerRecord<Integer, String> record) {
                //打印获取到的消息
                System.out.println("消息主题:" + record.topic() + "\t" +
                                    "消息分区:" + record.partition() + "\t" +
                                    "消息偏移量:" + record.offset() + "\t" +
                                    "消息KEY:" + record.key() + "\t" +
                                    "消息VALUE:" + record.value());
            }
        });

        //关闭消费者
        consumer.close();
    }
}
