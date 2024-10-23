package com.test.Mock;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {
    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/post-message")
    public ResponseEntity<String> sendMessageToKafka(@RequestBody Map<String, String> requestBody) {
        String msgId = requestBody.get("msg_id");
        if (msgId == null || msgId.isEmpty()) {
            return ResponseEntity.badRequest().body("msg_id is missing");
        }
        long timestamp = Instant.now().toEpochMilli();
        String method = "POST";
        String uri = "/post-message";
        
        Map<String, String> message = new LinkedHashMap<String, String>();
        message.put("\"msg_id\"", String.format("\"%s\"", msgId));
        message.put("\"timestamp\"", String.format("\"%d\"", timestamp));
        message.put("\"method\"", String.format("\"%s\"", method));
        message.put("\"uri\"", String.format("\"%s\"", uri));

        try {
            producerService.sendMessage(message.toString());
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
