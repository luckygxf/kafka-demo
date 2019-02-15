package com.gxf.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
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
        int count = Integer.MAX_VALUE;
        while(count > 0) {
          Duration duration = Duration.ofSeconds(1);
          ConsumerRecords<String, String> records = consumer.poll(duration);
          for (ConsumerRecord<String, String> record : records) {
            logger.info("topic:{}, partition:{}, offset:{}, key:{}, value:{} ======================= ",
                record.topic(), record.partition(), record.offset(), record.key(), record.value());
          }
          logger.info("consumer start paused=========");
          Set<TopicPartition> topicPartitionSet = consumer.assignment();
          for (TopicPartition topicPartition : topicPartitionSet)
            logger.info("topicPartition:{}", topicPartition);
          if (count == Integer.MAX_VALUE)
            consumer.pause(topicPartitionSet);
          if (count == Integer.MAX_VALUE - 20)
            consumer.resume(consumer.paused());
          count --;
        }
      } catch (WakeupException e){
        logger.error("catch WakeupException", e);
      } catch (Exception e) {
        logger.error("catch Exception", e);
      } finally {
        logger.info("start close consumer");
        consumer.close();
        logger.info("end close consumer");
      }
    }
  }

  private static void resumeConsumerThread(KafkaConsumer<String, String> consumer) {
    Thread thread = new Thread(() -> {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      logger.info("start to resume consumer");
//      Set<TopicPartition> topicPartitionSet = consumer.assignment();
      Set<TopicPartition> topicPartitionSet = consumer.paused();
      for (TopicPartition topicPartition : topicPartitionSet)
        logger.info("topicPartition:{}", topicPartition);
      consumer.resume(topicPartitionSet);
    });
    thread.start();
  }

  private static void assignConsumerPartition(KafkaConsumer<String, String> consumer) {
    String topic = "test";
    int parttion = 1;
    List<PartitionInfo> topicPartitionList = consumer.partitionsFor(topic);
    for (PartitionInfo partitionInfo : topicPartitionList)
      logger.info("topic:{}, partition:{}", partitionInfo.topic(), partitionInfo.partition());
    //assign partition
    List<TopicPartition> assignPartions = new ArrayList<>();
    assignPartions.add(new TopicPartition(topic, parttion));
    consumer.assign(assignPartions);
  }

  private static void startThreadToWakeupconsumer(KafkaConsumer<String, String> consumer){
    Thread t = new Thread(() -> {
      consumer.wakeup();
    });
    t.start();
  }

  private static void addExitConsumerHook(KafkaConsumer<String, String> consumer) throws Exception {
    Thread mainThread = Thread.currentThread();
    //add hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("trigger shutdown hook, start close consumer");
      consumer.wakeup();
      logger.info("close consumer success");
      try {
        mainThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));
    Thread.sleep(10000);
  }


}
