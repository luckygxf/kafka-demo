package com.gxf.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:24 AM
 **/
public class TestConsumerOffset {
  private static Logger logger = LoggerFactory.getLogger(TestConsumerOffset.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();
  private static Properties props = CommonUtil.getConsumerProperties("9");

  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props);
    ConsumerTask c2 = new ConsumerTask(consumer2,1, "__consumer_offsets");
    executorService.submit(c2);
  }


}
