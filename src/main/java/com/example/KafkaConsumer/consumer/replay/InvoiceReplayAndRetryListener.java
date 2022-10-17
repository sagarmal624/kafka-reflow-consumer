package com.example.KafkaConsumer.consumer.replay;

import com.example.KafkaConsumer.dto.ReplayDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class InvoiceReplayAndRetryListener implements ConsumerSeekAware {
    private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

    private static final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();

//    @RetryableTopic(
//            attempts = "4",
//            backoff = @Backoff(delay = 1000, multiplier = 2.0),
//            autoCreateTopics = "false",
//            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE)
//    @KafkaListener(topics = {"invoice"}, containerFactory = "kafkaListenerContainerFactory")
    public void receivedMessage(@Payload Map<String, Object> message) throws JsonProcessingException {
        log.info("Json message received using Kafka listener ...");
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(message);
        log.info(json);
        if (message.containsKey("exception")) {
            throw new RuntimeException("Faild due to exception occured while proccessing invoice data");
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        callbackForThread.set(callback);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.keySet().forEach(tp -> this.callbacks.put(tp, callbackForThread.get()));
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
    }
    @DltHandler
    public void dlt(Map<String, Object> message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Welcome to Dead Lelter Listener.....");
        log.info(message + " from " + topic);
    }
    public void seekToStart() {
        this.callbacks.forEach((tp, callback) -> callback.seekToBeginning(tp.topic(), tp.partition()));
    }

    public void seekToStart(ReplayDto replayDto) {

        if (Objects.isNull(replayDto.getPartition())) {
            this.callbacks.entrySet().stream().filter(it -> it.getKey().topic().equals(replayDto.getTopic()))
                    .forEach(entry -> {
                        TopicPartition topicPartition = entry.getKey();
                        ConsumerSeekCallback consumerSeekCallback = entry.getValue();
                        Timestamp timestamp = Timestamp.valueOf(replayDto.getStartDate());
                        System.out.println(timestamp.getTime());
                        consumerSeekCallback.seekToTimestamp(Collections.singletonList(topicPartition), timestamp.getTime());
                    });
        } else {
            this.callbacks.entrySet().stream().filter(it -> it.getKey().topic().equals(replayDto.getTopic()) && it.getKey().partition() == replayDto.getPartition())
                    .forEach(entry -> {
                        TopicPartition topicPartition = entry.getKey();
                        ConsumerSeekCallback consumerSeekCallback = entry.getValue();
                        Timestamp timestamp = Timestamp.valueOf(replayDto.getStartDate());
                        System.out.println(timestamp.getTime());
                        consumerSeekCallback.seekToTimestamp(Collections.singletonList(topicPartition), timestamp.getTime());
                    });
        }
    }

    public void seekToStart(String topicName, int partition) {
        this.callbacks.entrySet().stream().filter(it -> it.getKey().topic().equals(topicName))
                .forEach(entry -> {
                    TopicPartition topicPartition = entry.getKey();
                    ConsumerSeekCallback consumerSeekCallback = entry.getValue();
                    LocalDateTime now = LocalDateTime.now().minusHours(1);
                    Timestamp timestamp = Timestamp.valueOf(now);
                    System.out.println(timestamp.getTime());
                    consumerSeekCallback.seekToTimestamp(Collections.singletonList(topicPartition), timestamp.getTime());
                });
    }
}
