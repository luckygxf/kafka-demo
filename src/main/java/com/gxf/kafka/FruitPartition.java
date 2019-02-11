package com.gxf.kafka;

import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description: 自定义分区器 水果分区器
 * @Date: Created in : 2019/1/25 9:20 PM
 **/
public class FruitPartition implements Partitioner {
  private static int partitionIndex = 1;

  /**
   * 定义了3中水果，3个分区
   * */
  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {
//    int partitionNums = cluster.partitionsForTopic(topic).size();
//    return (partitionIndex & 1) == 1 ? 1 : 0;
    if (((String) key).equals(Fruit.APPLE)) {
      return 0;
    } else if (((String) key).equals(Fruit.BANANA)) {
      return 1;
    } else {
      return 2;
    }
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
