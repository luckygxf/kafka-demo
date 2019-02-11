package com.gxf.kafka;

/**
 * @Author: <guanxianseng@163.com>
 * @Description:
 * @Date: Created in : 2019/1/25 9:21 PM
 **/
public enum Fruit {
  APPLE(1, "苹果"), ORANGE(2, "橘子"), BANANA(3, "香蕉")
  ;

  private int id;
  private String name;

  Fruit(int id, String name) {
    this.id = id;
    this.name = name;
  }

  @Override
  public String toString() {
    return "Fruit{" +
        "id=" + id +
        ", name='" + name + '\'' +
        '}';
  }
}
