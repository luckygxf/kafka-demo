package com.gxf.slfobj;

import com.gxf.beans.Person;
import com.gxf.kafka.CommonUtil;
import com.gxf.kafka.TestProducer;
import com.gxf.util.PersonJsonSearializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/2/13 2:57 PM
 **/
public class PersonProducer {
  private static KafkaProducer<String, Person> producer = new KafkaProducer<String, Person>(getProducerProperties());
  private static Logger logger = LoggerFactory.getLogger(TestProducer.class);


  public static void main(String[] args) throws Exception {
    String topic = "person";
    String key = "name";
    Person person = new Person("guanxianseng", 18);

    int count = 0;
    String name = "guanxianseng_";
    while(count < 10000) {
      person.setName(name + count);
      producer.send(new ProducerRecord<String, Person>(topic,null, person));
      logger.info("send person:{} ================ ", person);
      Thread.sleep(1000);
      count ++;
    }
  }

  private static Properties getProducerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", PersonJsonSearializer.class.getName());
    return props;
  }
}
