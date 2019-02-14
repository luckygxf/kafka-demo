package com.gxf.test;

import java.util.List;
import java.util.TreeMap;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/2/11 10:52 AM
 **/
public class TestForEach {

  public static void main(String[] args) {
   testTreeMap();
  }

  private static void testTreeMap() {
    TreeMap<Integer, Integer> buckets = new TreeMap<>();
    buckets.put(100, 100);
    buckets.put(90, 90);
    buckets.put(80, 80);
    buckets.put(70, 70);
    buckets.put(60, 60);
    buckets.put(50, 50);

    System.out.println(buckets.higherKey(100));
  }
}
