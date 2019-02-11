package com.gxf.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:24 AM
 **/
public class FruitConsumer {
  private static Logger logger = LoggerFactory.getLogger(FruitConsumer.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();
  private static Properties props = CommonUtil.getConsumerProperties("8");

  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer1 = new KafkaConsumer<String, String>(props);
    ConsumerTask c1 = new ConsumerTask(consumer1,1, "fruit");

    KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props);
    ConsumerTask c2 = new ConsumerTask(consumer2,2, "fruit");

    KafkaConsumer<String, String> consumer3 = new KafkaConsumer<String, String>(props);
    ConsumerTask c3 = new ConsumerTask(consumer3,3, "fruit");

    executorService.submit(c1);
    executorService.submit(c2);
    executorService.submit(c3);
  }

}
