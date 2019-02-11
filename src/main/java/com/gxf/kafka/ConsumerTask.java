package com.gxf.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/1/29 4:33 PM
 **/
public class ConsumerTask implements Runnable {
  private KafkaConsumer<String, String> consumer;
  private int id;
  private static Logger logger = LoggerFactory.getLogger(ConsumerTask.class);
  private boolean isClose = false;
  private String topic;

  public ConsumerTask(KafkaConsumer<String, String> consumer, int id, String topic) {
    this.consumer = consumer;
    this.id = id;
    this.topic = topic;
  }

  @Override
  public void run() {
    try {
//      consumer.subscribe(Arrays.asList(topic));
      Pattern pattern = Pattern.compile(topic);
      consumer.subscribe(pattern);
      int count = 100;
      while(count > 0 && !isClose) {
        Duration duration = Duration.ofSeconds(1);
        ConsumerRecords<String, String> records = consumer.poll(duration);
        for (ConsumerRecord<String, String> record : records) {
          logger.info("topic:{}, partition:{}, offset:{}, record.key:{}, recorde.value:{} ============",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        Thread.sleep(1000);
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
}
