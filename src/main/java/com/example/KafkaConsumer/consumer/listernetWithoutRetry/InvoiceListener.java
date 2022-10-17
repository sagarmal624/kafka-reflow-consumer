package com.example.KafkaConsumer.consumer.listernetWithoutRetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class InvoiceListener {
   //@KafkaListener(topics = "invoice", containerFactory = "kafkaListenerContainerFactory")
    public void receivedMessage(@Payload Map<String, Object> message) throws JsonProcessingException {
        log.info("Json message received using Kafka listener 1 ...");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(message);
        log.info(json);
    }
}
