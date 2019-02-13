package com.gxf.util;

import com.alibaba.fastjson.JSON;
import com.gxf.beans.Person;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/2/13 3:47 PM
 **/
public class PersonJsonDeserializer implements Deserializer<Person> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public Person deserialize(String topic, byte[] data) {
    return JSON.parseObject(data, Person.class);
  }

  @Override
  public void close() {

  }
}
