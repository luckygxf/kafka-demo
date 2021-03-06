package com.gxf.rebanance.listener;

import com.gxf.kafka.CommonUtil;
import com.gxf.kafka.ConsumerTask;
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
public class TestConsumer2 {
  private static Logger logger = LoggerFactory.getLogger(TestConsumer2.class);
  private static ExecutorService executorService = Executors.newCachedThreadPool();


  public static void main(String[] args) throws Exception {
    KafkaConsumer<String, String> consumer1 = new KafkaConsumer<String, String>(CommonUtil.getConsumerProperties("9"));
    ConsumerTask c1 = new ConsumerTask(consumer1,2, "test");
    executorService.submit(c1);
  }

}
