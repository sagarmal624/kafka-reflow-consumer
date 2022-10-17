package com.example.KafkaConsumer.consumer.retry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
@Slf4j
public class InvoiceRetryListener {
//    @RetryableTopic(
//            attempts = "4",
//            backoff = @Backoff(delay = 1000, multiplier = 2.0),
//            autoCreateTopics = "false",
//            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
//    @KafkaListener(topics = "invoice", containerFactory = "kafkaListenerContainerFactory")
    public void receivedMessage(@Payload Map<String, Object> message) throws JsonProcessingException {
        log.info("Json message received using Kafka listener ...");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(message);
        log.info(json);
        if (message.containsKey("exception")) {
            throw new RuntimeException("Faild due to exception occured while proccessing invoice data");
        }
    }

    @DltHandler
    public void dlt(Map<String, Object> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Welcome to Dead Lelter Listener bhai...");
        log.info(message + " from " + topic);
    }
}
