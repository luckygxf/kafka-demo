package com.gxf.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:24 AM
 **/
public class Consumer {
  private static Logger logger = LoggerFactory.getLogger(Consumer.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();

  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer1 = new KafkaConsumer<String, String>(CommonUtil.getConsumerProperties("8"));
    ConsumerTask c1 = new ConsumerTask(consumer1,1);
    executorService.submit(c1);
  }

  static class ConsumerTask implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private int id;

    public ConsumerTask(KafkaConsumer<String, String> consumer, int id) {
      this.consumer = consumer;
      this.id = id;
    }

    @Override
    public void run() {
      try {
        String topic = "test";
        List<String> topicList = new ArrayList();
        topicList.add("test");
        topicList.add("test1");
        consumer.subscribe(topicList);
        int count = 100;
        while(count > 0) {
          Duration duration = Duration.ofSeconds(1);
          ConsumerRecords<String, String> records = consumer.poll(duration);
//          logger.info("id: {}, records.count():{} ======================= ", id, records.count());
          for (ConsumerRecord<String, String> record : records) {
            logger.info("topic:{}, partition:{}, offset:{}, key:{}, value:{} ======================= ",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
          }
          Thread.sleep(1000);
        }
      } catch (Exception e){
        logger.error(e.getMessage(), e);
      }
    }
  }


}
