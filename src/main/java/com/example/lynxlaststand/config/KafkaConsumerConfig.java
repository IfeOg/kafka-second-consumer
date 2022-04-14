package com.example.lynxlaststand.config;

import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.example.lynxlaststand.model.Client;

@Configuration
public class KafkaConsumerConfig {

	private String bootstrapServers = "127.0.0.1:9092";

	private String groupId = "group_id";

	private String topicName = "client-2-log";

	public ConsumerFactory<String, Client> clientConsumerFactory() {
		Map<String, Object> properties = new HashMap<>();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(),
				new JsonDeserializer<>(Client.class));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Client> clientKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Client> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(clientConsumerFactory());
		return factory;
	}

}
