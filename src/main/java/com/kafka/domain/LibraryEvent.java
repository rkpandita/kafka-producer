package com.kafka.domain;

public class LibraryEvent {

    private Integer libraryEventId;
    private LibraryEventType eventType;
    private Book book;
    
	public Integer getLibraryEventId() {
		return libraryEventId;
	}
	public void setLibraryEventId(Integer libraryEventId) {
		this.libraryEventId = libraryEventId;
	}
	public LibraryEventType getEventType() {
		return eventType;
	}
	public void setEventType(LibraryEventType eventType) {
		this.eventType = eventType;
	}
	public Book getBook() {
		return book;
	}
	public void setBook(Book book) {
		this.book = book;
	}

}
