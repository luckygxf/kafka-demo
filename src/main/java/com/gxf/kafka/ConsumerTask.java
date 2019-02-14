package com.gxf.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import kafka.cluster.Partition;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/29 4:33 PM
 **/
public class ConsumerTask implements Runnable {
  private KafkaConsumer<String, String> consumer;
  private int id;
  private static Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
  private boolean isClose = false;
  private String topic;
  private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

  public ConsumerTask(KafkaConsumer<String, String> consumer, int id, String topic) {
    this.consumer = consumer;
    this.id = id;
    this.topic = topic;
  }

  @Override
  public void run() {
    try {
      List<String> topics = new ArrayList<>();
      topics.add(topic);
//      Pattern pattern = Pattern.compile(topic);
      consumer.subscribe(topics, new RebananceHandler());
      int count = 100;
      while(count > 0 && !isClose) {
        Duration duration = Duration.ofSeconds(1);
        ConsumerRecords<String, String> records = consumer.poll(duration);
        for (ConsumerRecord<String, String> record : records) {
          logger.info("topic:{}, partition:{}, offset:{}, record.key:{}, recorde.value:{} ============",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
          currentOffset.put(new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1, "no metadata"));
        }
        Thread.sleep(1000);
        if (count == 80) {
          consumer.commitSync();
          logger.info("commit msg-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= and seek offset to 0");
          for (TopicPartition topicPartition : consumer.assignment())
            consumer.seek(topicPartition, 0);
        }
        count --;
        consumer.commitAsync(currentOffset, null);
        currentOffset = new HashMap<>();
      }
      consumer.close();
      logger.info("consumer closed!========");
    } catch (Exception e){
      logger.error(e.getMessage(), e);
    }
  }

  public boolean isClose() {
    return isClose;
  }

  public void setClose(boolean close) {
    isClose = close;
  }


  private class RebananceHandler implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      logger.info("");
      logger.info("====trigger banance listener lost partition in rebalance offset:{} =====", currentOffset);
      consumer.commitSync(currentOffset);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

    }
  }
}
