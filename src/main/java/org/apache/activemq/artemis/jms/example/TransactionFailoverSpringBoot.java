/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.jms.example;

import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.jms.Session;

import org.apache.activemq.artemis.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@EnableScheduling
@SpringBootApplication
public class TransactionFailoverSpringBoot implements CommandLineRunner {

   private static final Logger log = LoggerFactory.getLogger(TransactionFailoverSpringBoot.class);

   private static Process server0;
   private static Process server1;
   public static void main(String[] args) throws Exception{

      try {
         //Start servers - this is needed before the main run as DefaultMessageListenerContainer tries to get a connection during start
         server0 = ServerUtil.startServer(args[0], TransactionFailoverSpringBoot.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], TransactionFailoverSpringBoot.class.getSimpleName() + "1", 1, 5000);

         //Main run
         SpringApplication.run(TransactionFailoverSpringBoot.class, args);

      } finally {
         //Stop server
         log.info("Shut down servers");
         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
         log.info("Done");
      }
   }

   @Autowired
   private JmsTemplate jmsTemplate;

   @Autowired
   JmsListenerEndpointRegistry jmsListenerEndpointRegistry;

   @Autowired
   private ConfigurableApplicationContext applicationContext;

   @Value("${source.queue}")
   String sourceQueue;

   @Value("${target.queue}")
   String targetQueue;

   @Value("${send.count}")
   Integer sendCount;

   @Value("${receive.delayBeforeForward}")
   Integer delayBeforeForward;

   @Value("${receive.delayBeforeDone}")
   Integer delayBeforeDone;

   @Value("${receive.addAmqDuplId}")
   Boolean addAmqDuplId;

   @Value("${receive.throwException}")
   Boolean throwException;

   @Value("${brokerFailover}")
   Boolean brokerFailover;

   @Value("${counterUpdate}")
   Integer counterUpdate;

   @Value("${shutDownDelay}")
   Integer shutDownDelay;

   private AtomicInteger receiveCounter = new AtomicInteger();
   private AtomicInteger sendCounter = new AtomicInteger();
   private AtomicInteger receiveForwardedCounter = new AtomicInteger();
   private int receiveCounterLast = 0;

   //Collect sent ids, so we can observe missing.
   Map<String,String> sentUUIDs = new ConcurrentHashMap<>();

   //Collect received ids, so we can observe duplicates.
   Map<String,String> receivedUUIDs = new ConcurrentHashMap<>();


   @Override
   public void run(String... args) throws Exception {
      try {

         //Send messages to queue, before starting receivers
         for (int i=0; i< sendCount; i++) {
            String uuid = UUID.randomUUID().toString();
            String counter = Integer.toString(sendCounter.incrementAndGet());
            log.debug("Sending: {}", uuid);

            this.jmsTemplate.convertAndSend(sourceQueue, "message: "+uuid, m -> {
               m.setStringProperty("SEND_COUNTER", counter);
               m.setStringProperty("UUID", uuid);
               sentUUIDs.put(uuid,"");
               return m;
            });
            sentUUIDs.put(uuid,counter);

         }
         log.info("Total sent: {} - {}",sendCounter.get(), sentUUIDs.size());


         //Start receiving messages
         Thread.sleep(1000);
         log.info("Start listeners");

         log.info("Message count before listener start - sent: {}, received: {}, forwarded: {}", sendCounter.get(), receiveCounter.get(), receiveForwardedCounter.get());
         jmsListenerEndpointRegistry.start();


         //Wait until we received 10% of messages before trigger broker failover
         while (receiveCounter.get() < sendCount/10) {
            Thread.sleep(100);
         }

         //Broker failover
         if (brokerFailover) {
            ServerUtil.killServer(server0);
            log.info("Message count after failover - sent: {}, received: {}, forwarded: {}", sendCounter.get(), receiveCounter.get(), receiveForwardedCounter.get());
         }


         //Wait until we received all of messages, and no more was incoming in the last second
         while (receiveCounter.get() < sendCount || receiveCounter.get() > receiveCounterLast) {
            Thread.sleep(counterUpdate);
         }


         Thread.sleep(shutDownDelay);
         log.info("Counting queue sizes...");

         jmsTemplate.setReceiveTimeout(1000);
         Message msg;

         int sourceCount = 0;
         while((msg = jmsTemplate.receive(sourceQueue)) != null) {
            sourceCount++;
         }

         //Check successfully arrived messages
         int targetCount = jmsTemplate.browse(targetQueue, (Session session, QueueBrowser browser) ->{
            Enumeration enumeration = browser.getEnumeration();
            int counter = 0;
            while (enumeration.hasMoreElements()) {
               Message message = (Message) enumeration.nextElement();
               counter += 1;
               String uuid = message.getStringProperty("UUID");
               sentUUIDs.remove(uuid);
            }
            return counter;
         });
         sentUUIDs.entrySet().stream()
             .forEach(e->log.info("Message missing: {} - {}",e.getKey(),e.getValue()));


         //Check messages on DLQ
         int DLQCount = jmsTemplate.browse("DLQ", (Session session, QueueBrowser browser) ->{
            Enumeration enumeration = browser.getEnumeration();
            int counter = 0;
            while (enumeration.hasMoreElements()) {
               Message dlqMsg = (Message) enumeration.nextElement();
               log.info("Message in DLQ: {} - {}",dlqMsg.getStringProperty("UUID"), dlqMsg.getStringProperty("SEND_COUNTER"));
               counter += 1;
            }
            return counter;
         });

//         int DLQCount = 0;
//         while((msg = jmsTemplate.receive("DLQ")) != null) {
//            DLQCount++;
//            log.info("message in DLQ queue: {} - {}", msg.getStringProperty("UUID"), msg.getStringProperty("SEND_COUNTER"));
//         }

         log.info("Message count at the end - sent: {}, received: {}, forwarded: {}", sendCounter.get(), receiveCounter.get(), receiveForwardedCounter.get());
         log.info("Message count on source queue: {}", sourceCount);
         log.info("Message count on target queue: {}",targetCount);
         log.warn("Message count on DLQ - duplicates: {}", DLQCount);
         //Number of messages on DLQ should be 0 for a seamless failover

      } finally {
         //Shut down listeners and scheduled tasks
         log.info("Stop applicationContext");
         applicationContext.close();
      }
   }

   /**
    * Log message counters. It also updates receiveCounterLast, which is needed to shut down after all messages are received.
    */
   @Scheduled(fixedDelayString = "${counterUpdate}")
   public void reportCurrentTime() {
      int current = receiveCounter.get();
      int diff = current - receiveCounterLast;
      receiveCounterLast = current;
      log.debug("Method calls: sent: {}, received: {} ({}/s), forwarded: {}", sendCounter.get(), current, diff, receiveForwardedCounter.get());
   }


   /**
    * Process a message and send to target queue - this is used by JmsListeners
    */
   public void doReceiveMessage(String text, Session session, String counter, String UUID) throws InterruptedException {

      //Increase counters and uuid set
      log.debug("Received: {} - {}", UUID, counter);
      receiveCounter.incrementAndGet();
      if (receivedUUIDs.put(UUID,counter) != null) {
         log.warn("Received again: {} - {}", UUID, counter);
      }

      //Send message to target queue
      Thread.sleep(delayBeforeForward);

      jmsTemplate.convertAndSend(targetQueue, text, m -> {
         m.setStringProperty("SEND_COUNTER", counter);
         m.setStringProperty("UUID", UUID);
         if (addAmqDuplId) {
            m.setStringProperty("_AMQ_DUPL_ID", UUID);
         }
         return m;
      });
      log.debug("Forwarded: {} - {}", UUID, counter);
      receiveForwardedCounter.incrementAndGet();

      //Optianally throw exception to test transaction boundaries
      if (throwException) {
         throw new RuntimeException("Exception for uuid:" + UUID);
      }

      Thread.sleep(delayBeforeDone);
      log.debug("Done: {} - {}", UUID, counter);
   }

}
