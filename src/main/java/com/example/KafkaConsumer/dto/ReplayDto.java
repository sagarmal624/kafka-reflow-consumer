package com.example.KafkaConsumer.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ReplayDto {
private String topic;
private LocalDateTime startDate;
private Integer partition;
}
