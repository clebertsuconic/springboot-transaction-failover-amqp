package org.apache.activemq.artemis.jms.example.config;

import javax.jms.Session;

import org.apache.activemq.artemis.jms.example.TransactionFailoverSpringBoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Header;

/****
 * In this mode the default DefaultJmsListenerContainerFactory is used which is auto-configured based on the Spring Boot properties
 */
@Configuration
@ConditionalOnProperty(
    value="transaction.mode",
    havingValue = "DEFAULT_MESSAGE_LISTENER_CONTAINER")
@EnableJms
public class ModeDefaultMessageListenerContainer {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    TransactionFailoverSpringBoot app;


   @JmsListener(destination = "${source.queue}", concurrency="${receive.concurrentConsumers}")
   public void receiveMessageWithTransactionManager(String text, Session session, @Header("SEND_COUNTER") String counter, @Header("UUID") String uuid) throws InterruptedException {
      app.doReceiveMessage(text, session, counter, uuid);
   }
}
