# kafka demo  
1. 生产者、消费者demo  
2. 自定义分区器  
3. 传输自定义对象  
  1. 自定义序列化、发序列化器，序列化为二进制数组，如fastjson等工具  
4. 一个消费者订阅多个主题
  1. 消费主题list  
  2. topic使用正则表达式  
5. rebanance listener  
  1. topic test 3个partition，一个consumer, 一个listener, 增加一个consumer触发rebanance listener  
6. 使用seek函数，跳转到指定的offset  
7. 独立消费者  
  1. 获取kafka集群的topic和partition  
  2. 手动指定topic和partition  
  3. 初始化consumer不设置groupid  
8. 使用consumer.wakeup()退出consumer, 捕获wakeupException  