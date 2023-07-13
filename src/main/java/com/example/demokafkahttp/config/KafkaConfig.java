package com.example.demokafkahttp.config;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.messaging.handler.annotation.SendTo;

@Configuration
public class KafkaConfig {

	public static final String REQ_TOPIC = "srcTopic";

	public static final String RES_TOPIC = "dstTopic";

	@Bean
	public NewTopics topics() {
		return new NewTopics(TopicBuilder.name(REQ_TOPIC).build(), TopicBuilder.name(RES_TOPIC).build());
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(
			ConcurrentKafkaListenerContainerFactory<String, String> factory) {
		factory.setReplyTemplate(kafkaTemplate());
		var replyConsumerContainer = factory.createContainer(RES_TOPIC);
		replyConsumerContainer.getContainerProperties().setGroupId("g10");
		return new ReplyingKafkaTemplate<>(producerFactory(), replyConsumerContainer);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		return new DefaultKafkaProducerFactory<>(configs);
	}

	@KafkaListener(id = "cc1", topics = REQ_TOPIC)
	@SendTo
	public String listen(ConsumerRecord<String, String> record) {

		try {
			TimeUnit.SECONDS.sleep(2);
		}
		catch (Exception e) {
		}
		return String.format("req message: %s, replay time: %s", record.value(), LocalTime.now().toString());
	}

}
