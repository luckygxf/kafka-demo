package com.gxf.util;

import com.alibaba.fastjson.JSON;
import com.gxf.beans.Person;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2019/2/13 3:45 PM
 **/
public class PersonJsonSearializer implements Serializer<Person> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  @Override
  public byte[] serialize(String topic, Person data) {
    return JSON.toJSONBytes(data);
  }

  @Override
  public void close() {

  }
}
