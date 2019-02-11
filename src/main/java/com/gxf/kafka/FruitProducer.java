package com.gxf.kafka;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:11 AM
 **/
public class FruitProducer {
  private static KafkaProducer<String, String> producer = new KafkaProducer<String, String>(CommonUtil.getProducerProperties());
  private static Logger logger = LoggerFactory.getLogger(FruitProducer.class);

  public static void main(String[] args) throws Exception {
    String topic = "fruit";
    String[] keys = new String[]{Fruit.APPLE.toString(), Fruit.ORANGE.toString(), Fruit.BANANA.toString()};

    int count = 0;
    while(count < 10000) {
      int index = getRandomInt(3);
      String value = "guanxianseng" + (count ++) + keys[index] ;
      producer.send(new ProducerRecord<String, String>(topic,keys[index],value));
      logger.info("send msg:{} ================ ", value);
      Thread.sleep(1000);
    }
  }

  private static int getRandomInt(int n) {
    Random random = new Random(System.currentTimeMillis());
    int res = random.nextInt(n);
    return res;
  }

}
