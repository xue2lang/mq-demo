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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;

public class RocketMQConsumer {

    private static RPCHook getAclRPCHook() {
        return new AclClientRPCHook(new SessionCredentials(MqConfig.ACCESS_KEY, MqConfig.SECRET_KEY));
    }

    public static void main(String[] args) throws MQClientException {

        final AtomicLong atomicLong = new AtomicLong(0);

        /**
         * 创建Consumer，并开启消息轨迹
         * 如果不想开启消息轨迹，可以按照如下方式创建：
         * DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MqConfig.GROUP_ID, getAclRPCHook(), null);
         */
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(MqConfig.GROUP_ID, getAclRPCHook(), new AllocateMessageQueueAveragely(), true, null);

        /**
         * 设置使用接入方式为阿里云，在使用云上消息轨迹的时候，需要设置此项，如果不开启消息轨迹功能，则运行不设置此项.
         */
        consumer.setAccessChannel(AccessChannel.CLOUD);
        consumer.setNamesrvAddr(MqConfig.NAMESRV_ADDR);
        consumer.subscribe(MqConfig.TOPIC, "*");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                long l = atomicLong.incrementAndGet();
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                if (l % 10==0) {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.printf("Consumer Started.%n");

        try {
            TimeUnit.HOURS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
