package com.gxf.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:11 AM
 **/
public class TestProducer {
  private static KafkaProducer<String, String> producer = null;
  private static Logger logger = LoggerFactory.getLogger(TestProducer.class);

  static {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    producer = new KafkaProducer<String, String>(props);
  }

  public static void main(String[] args) throws Exception {
    String topic = "test";
    String key = "name";

    int count = 0;
    while(count < 10000) {
      String value = "guanxianseng" + count ++;
      topic = ((count & 1) == 1) ? "test" : "test1";
      producer.send(new ProducerRecord<String, String>(topic,null,value));
      logger.info("send msg:{}, topic:{} ================ ", value, topic);
      Thread.sleep(1000);
    }
  }

}
