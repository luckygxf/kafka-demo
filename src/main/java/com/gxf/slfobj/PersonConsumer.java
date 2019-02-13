package com.gxf.slfobj;

import com.gxf.beans.Person;
import com.gxf.kafka.CommonUtil;
import com.gxf.kafka.Consumer;
import com.gxf.kafka.ConsumerTask;
import com.gxf.kafka.TestConsumer1;
import com.gxf.util.PersonJsonDeserializer;
import java.time.Duration;
import java.util.Arrays;
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
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/2/13 2:58 PM
 **/
public class PersonConsumer {
  private static Logger logger = LoggerFactory.getLogger(TestConsumer1.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();
  private static Properties props = getConsumerProperties("8");
  private static KafkaConsumer<String, Person> consumer = new KafkaConsumer(props);

  public static void main(String[] args) throws Exception {
    try {
      String topic = "person";
      consumer.subscribe(Arrays.asList(topic));
      int count = 100;
      while(count > 0) {
        Duration duration = Duration.ofSeconds(1);
        ConsumerRecords<String, Person> records = consumer.poll(duration);
        for (ConsumerRecord<String, Person> record : records) {
          logger.info("======================= offset:{}, partition:{}, person:{}",
              record.offset(), record.partition() ,record.value());
        }
        Thread.sleep(1000);
      }
    } catch (Exception e){
      logger.error(e.getMessage(), e);
    }
  }

  private static Properties getConsumerProperties(String groupId) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("max.poll.records", 1000);
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", PersonJsonDeserializer.class.getName());
    return props;
  }

}
