///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.aliyun.openservices.demo.consumer;
//
//import com.aliyun.openservices.demo.MqConfig;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import org.apache.rocketmq.acl.common.AclClientRPCHook;
//import org.apache.rocketmq.acl.common.SessionCredentials;
//import org.apache.rocketmq.client.AccessChannel;
//import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
//import org.apache.rocketmq.client.consumer.MQPullConsumer;
//import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
//import org.apache.rocketmq.client.consumer.PullResult;
//import org.apache.rocketmq.client.consumer.PullStatus;
//import org.apache.rocketmq.client.consumer.PullTaskCallback;
//import org.apache.rocketmq.client.consumer.PullTaskContext;
//import org.apache.rocketmq.client.exception.MQClientException;
//import org.apache.rocketmq.common.message.MessageExt;
//import org.apache.rocketmq.common.message.MessageQueue;
//import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
//import org.apache.rocketmq.remoting.RPCHook;
//
//public class RocketMQPullConsumer {
//
//  private static RPCHook getAclRPCHook() {
//    return new AclClientRPCHook(new SessionCredentials(MqConfig.ACCESS_KEY, MqConfig.SECRET_KEY));
//  }
//
//
//  private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();
//
//
//  public static void main(String[] args) throws MQClientException, InterruptedException {
//    final DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer(getAclRPCHook());
//    pullConsumer.setConsumerGroup(MqConfig.GROUP_ID_PULL);
//    pullConsumer.setAccessChannel(AccessChannel.CLOUD);
//    pullConsumer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
//    pullConsumer.setInstanceName(UUID.randomUUID().toString());
//    pullConsumer.setMessageModel(MessageModel.CLUSTERING);
//
//    //需要处理的消息topic
//    pullConsumer.start();
//
//    while (true) {
//      boolean waiting = true;
//      Set<MessageQueue> mqs = pullConsumer.fetchMessageQueuesInBalance(MqConfig.TOPIC_PULL);
//      //未获取到负载均衡的时候，等待1S重新获取
//      if (mqs == null || mqs.size() == 0) {
//        waiting = false;
//        System.out.println("-------sleep------");
//        Thread.sleep(1000L);
//      }
//      for (MessageQueue mq : mqs) {
//        System.out.printf("Consume from the queue: " + mq + "%n");
//        SINGLE_MQ:
//        while (true) {
//          long offset = pullConsumer.fetchConsumeOffset(mq, false);
//          try {
//            PullResult pullResult =
//                pullConsumer.pullBlockIfNotFound(mq, null, offset, 32);  //遍历所有queue，挨个调用pull
//
//            System.out.printf("%s%n", pullResult);
//            switch (pullResult.getPullStatus()) {
//              case FOUND:
//                offset = pullResult.getNextBeginOffset();
//                pullConsumer.updateConsumeOffset(mq, offset);
//                break;
//              case NO_MATCHED_MSG:
//                break;
//              case NO_NEW_MSG:
//                break SINGLE_MQ;
//              case OFFSET_ILLEGAL:
//                break;
//              default:
//                break;
//            }
//          } catch (Exception e) {
//            e.printStackTrace();
//          }
//        }
//      }
//      if (waiting) {
//        Thread.sleep(100L);
//      }
//    }
//
//  }
//
//
//  private static void thread() {
//    final AtomicLong atomicLong = new AtomicLong(0);
//
//    for (int i = 0; i < 2; i++) {
//      new Thread(
//          new Runnable() {
//            @Override
//            public void run() {
//              consumer();
//            }
//          }
//      ).start();
//    }
//  }
//
//
//  private static void consumer() {
//
//    try {
//
//      final DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(getAclRPCHook());
//      consumer.setConsumerGroup(MqConfig.GROUP_ID);
//      consumer.setAccessChannel(AccessChannel.CLOUD);
//      consumer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
//      consumer.setInstanceName(UUID.randomUUID().toString());
//      consumer.setMessageModel(MessageModel.CLUSTERING);
//
//      MQPullConsumerScheduleService consumerScheduleService = new MQPullConsumerScheduleService(consumer);
//
//      consumerScheduleService.registerPullTaskCallback(MqConfig.TOPIC, new PullTaskCallback() {
//        @lombok.SneakyThrows
//        @Override
//        public void doPullTask(MessageQueue messageQueue, PullTaskContext context) {
//
//          long id = Thread.currentThread().getId();
//
//          MQPullConsumer pullConsumer = context.getPullConsumer();
//          //获取消息的offset，指定从store中获取
//          long offset = pullConsumer.fetchConsumeOffset(messageQueue, true);
//          System.out.println(id + " start ------ queue " + messageQueue.getQueueId() + "---offset " + offset);
//
//          //拉取消息
//          try {
//            //阻塞消息
//            PullResult pullResult = pullConsumer.pull(messageQueue, "*", offset, 32);
//            //根据状态进行处理
//            if (pullResult.getPullStatus() != PullStatus.FOUND) {
//              return;
//            }
//            //输出消息
//            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
//            for (MessageExt messageExt : msgFoundList) {
//              System.out.println(id + " 信息内容 ：" + messageExt);
//            }
//
//            //提交消费位点
//            pullConsumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
//
//          } catch (Exception e) {
//            e.printStackTrace();
//          }
//
//
//        }
//      });
//
//      //启动
//      consumerScheduleService.start();
//
//      TimeUnit.SECONDS.sleep(6000);
//
//      //关闭
//      consumerScheduleService.shutdown();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//
//  // 保存上次消费的消息下标
//  private static void putMessageQueueOffset(MessageQueue mq,
//      long nextBeginOffset) {
//    OFFSE_TABLE.put(mq, nextBeginOffset);
//  }
//
//  // 获取上次消费的消息的下标
//  private static Long getMessageQueueOffset(MessageQueue mq, long defaultOffSet) {
//    Long offset = OFFSE_TABLE.get(mq);
//    if (offset != null) {
//      return offset;
//    }
//    return defaultOffSet;
//  }
//
//}
