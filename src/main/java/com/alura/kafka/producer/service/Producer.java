package com.alura.kafka.producer.service;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	public final static String TOPIC = "TESTE_TOPIC";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// TODO Auto-generated method stub

		KafkaProducer<String, String> producer = new KafkaProducer<>(properties());

		String chave = "infoPedido";
		String valor = " 1, 1213645, R$ 25.00 ";
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, chave, valor);

		producer.send(record, (data, ex) -> {
			
			if(Objects.nonNull(null)) {
				ex.printStackTrace();
				return;
			}
			System.out.println(data.topic() + ":::PARTITION: " + data.partition() + " / OFFSET: "
					+ data.offset() + " / TIMESTAMP" + data.timestamp());
		}).get();
	}


	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	}

}
