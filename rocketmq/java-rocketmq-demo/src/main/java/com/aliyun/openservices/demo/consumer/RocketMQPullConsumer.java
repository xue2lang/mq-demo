/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.openservices.demo.consumer;

import com.aliyun.openservices.demo.MqConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;

public class RocketMQPullConsumer {

  private static RPCHook getAclRPCHook() {
    return new AclClientRPCHook(new SessionCredentials(MqConfig.ACCESS_KEY, MqConfig.SECRET_KEY));
  }


  private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();





  public static void main(String[] args) throws MQClientException {

    final AtomicLong atomicLong = new AtomicLong(0);

    for (int i = 0; i < 2; i++) {
      new Thread(
          new Runnable() {
            @Override
            public void run() {
              consumer();
            }
          }
      ).start();
    }




  }


  private static void consumer() {


    try {



      final DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(getAclRPCHook());
      consumer.setConsumerGroup(MqConfig.GROUP_ID);
      consumer.setAccessChannel(AccessChannel.CLOUD);
      consumer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
      consumer.setInstanceName(UUID.randomUUID().toString());
      consumer.setMessageModel(MessageModel.CLUSTERING);


      MQPullConsumerScheduleService consumerScheduleService = new MQPullConsumerScheduleService(consumer);



      consumerScheduleService.registerPullTaskCallback(MqConfig.TOPIC, new PullTaskCallback() {
        @lombok.SneakyThrows
        @Override
        public void doPullTask(MessageQueue messageQueue, PullTaskContext context) {

            long id = Thread.currentThread().getId();

            MQPullConsumer pullConsumer = context.getPullConsumer();
            //获取消息的offset，指定从store中获取
              long offset = pullConsumer.fetchConsumeOffset(messageQueue, true);
              System.out.println(id+" start ------ queue "+messageQueue.getQueueId()+"---offset " + offset);

              //拉取消息
              try {
                //阻塞消息
                PullResult pullResult = pullConsumer.pull(messageQueue, "*", offset, 32);
                //根据状态进行处理
                if (pullResult.getPullStatus() != PullStatus.FOUND) {
                  return;
                }
                //输出消息
                List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
                for (MessageExt messageExt : msgFoundList) {
                  System.out.println(id + " 信息内容 ："+ messageExt);
                }

                //提交消费位点
                pullConsumer.updateConsumeOffset(messageQueue,pullResult.getNextBeginOffset());

              } catch (Exception e) {
                e.printStackTrace();
              }




        }
      });


      //启动
      consumerScheduleService.start();



      TimeUnit.SECONDS.sleep(6000);


      //关闭
      consumerScheduleService.shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }





  // 保存上次消费的消息下标
  private static void putMessageQueueOffset(MessageQueue mq,
      long nextBeginOffset) {
    OFFSE_TABLE.put(mq, nextBeginOffset);
  }

  // 获取上次消费的消息的下标
  private static Long getMessageQueueOffset(MessageQueue mq, long defaultOffSet) {
    Long offset = OFFSE_TABLE.get(mq);
    if (offset != null) {
      return offset;
    }
    return defaultOffSet;
  }

}
