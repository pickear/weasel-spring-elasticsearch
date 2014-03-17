package com.weasel.elasticsearch.exception;

import java.util.Map;

/**
 * @author Dylan
 *
 */
public class ElasticsearchException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -102321165357008474L;
	
	private Map<String, String> failedDocuments;

	public ElasticsearchException(String message) {
		super(message);
	}

	public ElasticsearchException(String message, Throwable cause) {
		super(message, cause);
	}

	public ElasticsearchException(String message, Throwable cause, Map<String, String> failedDocuments) {
		super(message, cause);
		this.failedDocuments = failedDocuments;
	}

	public ElasticsearchException(String message, Map<String, String> failedDocuments) {
		super(message);
		this.failedDocuments = failedDocuments;
	}

	public Map<String, String> getFailedDocuments() {
		return failedDocuments;
	}
}
