package com.gxf.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:24 AM
 **/
public class TestMultiBrokerConsumer {
  private static Logger logger = LoggerFactory.getLogger(TestMultiBrokerConsumer.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();
  private static Properties props = getConsumerProperties("10");

  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props);
    ConsumerTask c2 = new ConsumerTask(consumer2,1, "my-replicated-topic");
    executorService.submit(c2);
  }


  private static Properties getConsumerProperties(String groupId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("max.poll.records", 1000);
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    return props;
  }

}
