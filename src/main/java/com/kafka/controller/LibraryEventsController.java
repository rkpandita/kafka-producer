package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.domain.LibraryEventType;
import com.kafka.producer.LibraryEventProducer;

@RestController
public class LibraryEventsController {
	
	@Autowired
	LibraryEventProducer libraryEventProducer;

    @PostMapping(value = "/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
    	
    	// SendResult<Integer, String> syncEvent = libraryEventProducer.sendLibraryEventSync(libraryEvent);
    	libraryEvent.setEventType(LibraryEventType.NEW);
    	
    	libraryEventProducer.sendLibraryEvent(libraryEvent);
    	//System.out.println("Send result is: " + syncEvent.toString());
    	
    	return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    
    }
    
    @PutMapping(value = "/v1/libraryevent")
    public ResponseEntity<?> putLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
    	
    	if(libraryEvent.getLibraryEventId() == null) {
    		return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the libraryEventId");
    	}

    	libraryEvent.setEventType(LibraryEventType.UPDATE);
    	
    	libraryEventProducer.sendLibraryEvent(libraryEvent);
    	//System.out.println("Send result is: " + syncEvent.toString());
    	
    	return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    
    }

}
