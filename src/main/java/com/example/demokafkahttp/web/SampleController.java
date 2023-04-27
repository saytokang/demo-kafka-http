package com.example.demokafkahttp.web;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.example.demokafkahttp.config.KafkaConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SampleController {

	private final ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

	@PostMapping("/send")
	public ResponseEntity<?> send(@RequestBody String body) {

		var payload = String.format("%s, send time: %s", body, LocalDateTime.now());
		var pRecord = new ProducerRecord<String, String>(KafkaConfig.REQ_TOPIC, "k1", payload);
		pRecord.headers().add(KafkaHeaders.REPLY_TOPIC, KafkaConfig.RES_TOPIC.getBytes(StandardCharsets.UTF_8));
		var future = replyingKafkaTemplate.sendAndReceive(pRecord);
		try {
			var response = future.get(3, TimeUnit.SECONDS);
			return ResponseEntity.ok().body(response.value());
		}
		catch (TimeoutException te) {
			log.error("{}", te);
			return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body("biz timeout");
		}
		catch (InterruptedException | ExecutionException e) {
			log.error("{}", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}

}
