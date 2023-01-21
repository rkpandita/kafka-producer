package com.kafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	String TOPIC = "library-events";
	
	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	ObjectMapper objectMapper;

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		final ProducerRecord<Integer, String> record = createProducerRecord(TOPIC, key, value);
		
		// ListenableFuture is deprecated
		CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(record);
		
		completableFuture.whenComplete((sendResult, throwable) -> {
			if (throwable == null) {
				handleSuccess(key, value, sendResult);
			} else {
				handleFailure(key, value, throwable);
			}
		});
	}

	public SendResult<Integer,String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		SendResult<Integer, String> sendResult = null;
		
		try {
			// Don't use .get() with time in PROD as it blocks the Topic.
			sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (InterruptedException | ExecutionException ex) {
			System.err.println("InterruptedException OR ExecutionException while sending the message and the exception is : {}" + ex.getMessage());
		} catch (Exception ex) {
			System.err.println("Exception sending the message and the exception is : {}" + ex.getMessage());
		}

		return sendResult;
		
	}
	
	private ProducerRecord<Integer, String> createProducerRecord(String topic, Integer key, String value) {
		
		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()), 
												new RecordHeader("event-type", "type".getBytes()));
		
		return new ProducerRecord<Integer, String>(topic, null, null, key, value, recordHeaders);
	}
	
	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		System.out.println("Message sent successfully for the key : " + key + "and the value : " + value + " , partition is " + result.getRecordMetadata().partition());
	}
	
	private void handleFailure(Integer key, String value, Throwable ex) {
		System.err.println("Error sending the message and the exception is : {}" + ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			System.err.println("Error in OnFailure : {}" + throwable.getMessage());
		}
	}

}
