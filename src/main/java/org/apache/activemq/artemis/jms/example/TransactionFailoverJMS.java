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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.util.ServerUtil;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactionFailoverJMS {

   private static final String CONNECT_URL = "failover:(amqp://localhost:61616,amqp://localhost:61617)?failover.maxReconnectAttempts=16&jms.prefetchPolicy.all=5&jms.forceSyncSend=true";
   public static boolean USE_AMQP = true;
   private static final ConnectionFactory createCF() {
      if (USE_AMQP) {
         return new JmsConnectionFactory(CONNECT_URL);
      } else {
         ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616?ha=true&retryInterval=1000&retryIntervalMultiplier=1.0&reconnectAttempts=-1");
         return factory;
      }
   }

   private static final Logger log = LoggerFactory.getLogger(TransactionFailoverJMS.class);

   private static final AtomicInteger processed = new AtomicInteger(0);

   private static Process server0;
   private static Process server1;

   private static final AtomicBoolean running = new AtomicBoolean(true);

   public static synchronized void killServer() {
      if (server0 != null) {
         try {
            System.out.println(Thread.currentThread().getName() + " is killing the server");
            ServerUtil.killServer(server0);
            server0 = null;
         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }

   static class UserThread implements Runnable {
      private final CyclicBarrier barrier;
      private final int killAt;
      private final CountDownLatch latch;
      private final Connection sharedConnection;

      UserThread(CyclicBarrier barrier, int killAt, CountDownLatch latch, Connection connection) {
         this.barrier = barrier;
         this.killAt = killAt;
         this.latch = latch;
         this.sharedConnection = connection;
      }

      @Override
      public void run() {
         try {
            ConnectionFactory factory = createCF();
            Connection connection  = sharedConnection != null ? sharedConnection : factory.createConnection();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            Queue queueSource = session.createQueue("source");
            Queue targetQueue = session.createQueue("target");

            connection.start();
            MessageProducer producer = session.createProducer(targetQueue);
            MessageConsumer consumer = session.createConsumer(queueSource);
            int received = 0;
            boolean killed = false;
            while (running.get()) {
               try {
                  Message message = consumer.receive(500);
                  if (message != null) {
                     ++received;
                     if (received == killAt) {
                        barrier.await(); // once all threads received a number of messages (defined by killAt), the server will wait all threads aligned here, and the server will then be killed
                        killServer();
                        Thread.sleep(5000); // after the server is killed, we wait some time here, to make sure the session had enough time to reconnect
                        killed = true;
                     }
                     try {
                        TextMessage outMessage = session.createTextMessage("output::" + message.getStringProperty("i"));
                        outMessage.setStringProperty("_AMQ_DUPL_ID", message.getStringProperty("i"));
                        producer.send(outMessage);
                        session.commit();
                        if (killed) {
                           System.err.println("We were supposed to the commit failing here");
                           System.exit(-1);
                        }
                        processed.incrementAndGet();
                        latch.countDown();
                     } catch (Exception e) {
                        killed = false;
                        // In case of a failure
                        e.printStackTrace();
                        try {
                           session.rollback();
                        } catch (Exception ignored) {
                           e.printStackTrace();
                        }
                     }
                  }
               } catch (Exception e) {
                  e.printStackTrace();
                  Thread.sleep(500);
               }
            }

         } catch (Exception e) {
            e.printStackTrace();
         }
      }
   }
   public static void main(String[] args) throws Exception{


      try {
         //Start servers - this is needed before the main run as DefaultMessageListenerContainer tries to get a connection during start
         server0 = ServerUtil.startServer(args[0], TransactionFailoverJMS.class.getSimpleName() + "0", 0, 5000);
         server1 = ServerUtil.startServer(args[1], TransactionFailoverJMS.class.getSimpleName() + "1", 1, 5000);

         ConnectionFactory factory = createCF();
         Connection connection  = factory.createConnection();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

         Queue sourceQueue = session.createQueue("source");
         MessageProducer producerSource = session.createProducer(sourceQueue);

         int messages = 30_000;

         for (int i = 0; i < messages; i++) {
            TextMessage message = session.createTextMessage("message");
            message.setStringProperty("i", "" + i);
            producerSource.send(message);
         }
         session.commit();

         int numberOfThreads = 10;

         CyclicBarrier barrier = new CyclicBarrier(numberOfThreads + 1);
         CountDownLatch latch = new CountDownLatch(messages);

         Thread[] threads = new Thread[numberOfThreads];
         for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(new UserThread(barrier, -1, latch, connection), "UserThread " + i);
            threads[i].start();
         }
         //barrier.await();

         while (processed.get() < 1000) {
            System.out.println("Processed : " + processed);
            Thread.sleep(100);
         }

         ServerUtil.killServer(server0);

         int times = 0;
         while (!latch.await(1, TimeUnit.SECONDS)) {
            System.out.println("Waiting latch to finish : " + processed + " out of " + messages);
            times++;

            if (times > 30) {
               break;
            }
         }

         factory = new JmsConnectionFactory("amqp://localhost:61617");
         connection  = factory.createConnection();
         session = connection.createSession(true, Session.SESSION_TRANSACTED);

         connection.start();

         MessageConsumer consumer = session.createConsumer(session.createQueue("target"));
         for (int i = 0; i < messages; i++) {
            TextMessage receivedMessage = (TextMessage)consumer.receive(5000);
            if (receivedMessage == null) {
               System.err.println("message == null!!!");
               System.exit(-1);
            }
         }

         if (consumer.receiveNoWait() != null) {
            System.err.println("Duplicates were received");
         }

         consumer.close();

         System.out.println("Received " + messages + " messages");

         MessageConsumer dlqConsumer = session.createConsumer(session.createQueue("DLQ"));
         int dlq = 0;
         while (true) {
            Message message = dlqConsumer.receive(1000);
            if (message == null) break;
            dlq ++;

            System.out.println("Ouch!!! received from DLQ " + message);
         }

         if (dlq != 0) {
            System.err.println("Oh no!!! there are messages on the DLQ!");
            System.exit(-1);
         }

         System.out.println("Received " + dlq + " messages from DLQ");

         session.rollback();

         running.set(false);

         for (Thread t : threads) {
            t.join();
         }


      } finally {
         //Stop server
         log.info("Shut down servers");
         ServerUtil.killServer(server0);
         ServerUtil.killServer(server1);
         log.info("Done");
      }
   }

}
