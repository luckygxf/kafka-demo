package com.gxf.kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 11:24 AM
 **/
public class TestConsumer3 {
  private static Logger logger = LoggerFactory.getLogger(TestConsumer3.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();


  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer1 = new KafkaConsumer<String, String>(CommonUtil.getConsumerProperties("8"));
    ConsumerTask c1 = new ConsumerTask(consumer1,3, "test");
    executorService.submit(c1);
    Thread.sleep(20000);
//    c1.setClose(true);
  }


}
