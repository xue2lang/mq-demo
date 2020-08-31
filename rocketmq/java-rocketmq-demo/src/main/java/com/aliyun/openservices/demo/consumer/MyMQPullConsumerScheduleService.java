package com.aliyun.openservices.demo.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.remoting.RPCHook;

/**
 * @author qingtian
 */
public class MyMQPullConsumerScheduleService extends MQPullConsumerScheduleService {

  public MyMQPullConsumerScheduleService(String consumerGroup) {
    super(consumerGroup);
  }

  public MyMQPullConsumerScheduleService(String consumerGroup, RPCHook rpcHook) {
    super(consumerGroup, rpcHook);
  }


  @Override
  public void registerPullTaskCallback(final String topic, final PullTaskCallback callback) {
    DefaultMQPullConsumer pullConsumer = this.getDefaultMQPullConsumer();
    this.getCallbackTable().put(pullConsumer.withNamespace(topic), callback);
    pullConsumer.registerMessageQueueListener(topic, null);
  }
}
