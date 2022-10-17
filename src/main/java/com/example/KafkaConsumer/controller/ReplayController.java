package com.example.KafkaConsumer.controller;

import com.example.KafkaConsumer.consumer.replay.InvoiceReplayAndRetryListener;
import com.example.KafkaConsumer.consumer.replay.InvoiceReplayListener;
import com.example.KafkaConsumer.dto.ReplayDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/invoice")
public class ReplayController {
    @Autowired
    private InvoiceReplayListener invoiceReplayListener;

    @Autowired
    private InvoiceReplayAndRetryListener invoiceReplayAndRetryListener;

    @PostMapping("/replay")
    public String reflow(@RequestBody ReplayDto replayDto) {
        invoiceReplayListener.seekToStart(replayDto);
        return "Reflow Started...";
    }
}
