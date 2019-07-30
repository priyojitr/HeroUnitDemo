package com.firstdata.collections.services.exception_handlers;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.util.StringUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.fasterxml.jackson.core.JsonParseException;
import com.firstdata.collections.services.data_packets.GenericResponse;
import com.firstdata.collections.services.enums.GenericMessages;
import com.firstdata.collections.services.exceptions.AccountRequestValidationException;
import com.firstdata.collections.services.exceptions.GenericException;
import com.firstdata.collections.services.exceptions.PaymentProcessException;
import com.firstdata.collections.services.exceptions.PaymentRequestValidationException;
import com.firstdata.collections.services.exceptions.Sub43CollectionRecException;
import com.firstdata.collections.services.exceptions.Sub43PaymentPostingRequestValidationException;
import com.firstdata.collections.services.utils.UtilityMethods;

@RestControllerAdvice
public class AppExceptionHandler {

	private static final String ERROR_PACKET_NAME = "error_response";
	private static final Logger logger = LoggerFactory.getLogger(AppExceptionHandler.class);

	/**
	 * Represents exception handler when any bad request data packet is received or
	 * happens to be any generic application exception {@link GenericException}. The
	 * error details that is returned should be formatted as per request @{code
	 * Content-Type}, primarily targeted for message conversion.
	 * 
	 * @param request It is the current request object that is submitted.
	 * @param ex      It is the exception object that is raised.
	 * @return The exception message is returned using {@link GenericResponse}
	 *         packet.
	 */
	@ExceptionHandler({ HttpMessageNotReadableException.class, JsonParseException.class,
			MethodArgumentNotValidException.class, GenericException.class, Sub43CollectionRecException.class,
			PaymentProcessException.class, AccountRequestValidationException.class,
			Sub43PaymentPostingRequestValidationException.class, PaymentRequestValidationException.class })
	public ResponseEntity<String> handleControllerException(final HttpServletRequest request, final Throwable ex) {
		StringBuilder errorMessage = new StringBuilder();
		Errors errors = null;
		if (ex instanceof AccountRequestValidationException
				|| ex instanceof PaymentRequestValidationException
				|| ex instanceof Sub43PaymentPostingRequestValidationException) {
			if (ex instanceof AccountRequestValidationException) {
				AccountRequestValidationException exception = (AccountRequestValidationException) ex;
				errors = exception.getErrors();
			} else if(ex instanceof PaymentRequestValidationException) {
				PaymentRequestValidationException exception = (PaymentRequestValidationException) ex;
				errors = exception.getErrors();
			} else {
				Sub43PaymentPostingRequestValidationException exception = (Sub43PaymentPostingRequestValidationException) ex;
				errors = exception.getErrors();
			}
			errorMessage.append(ex.getMessage());
			for (FieldError error : errors.getFieldErrors()) {
				errorMessage.append(error.getField()).append(" - ").append(error.getDefaultMessage());
			}
		} else if (!(ex instanceof Sub43CollectionRecException || ex instanceof PaymentProcessException)) {
			errorMessage.append(GenericMessages.BAD_REQUEST_DATA.getMessage());
		} else {
			errorMessage.append(ex.getMessage());
		}
		return processReturnPacket(request, ex, errorMessage.toString());
	}

	/**
	 * Represents handler for any exceptions.
	 * 
	 * @param request Current request for which exception is raised.
	 * @param ex      {@code Throwable} type of exception that is thrown.
	 * @return {@code ResponseEntity} that is sent back by this service.
	 */
	@ExceptionHandler(Exception.class)
	public ResponseEntity<String> handlerGenericException(HttpServletRequest request, Throwable ex) {
		final String errorMessage = "An unexpected error occurred.";
		return processReturnPacket(request, ex, errorMessage);
	}

	/**
	 * Process the response data packet that is sent back for any type of exception
	 * that is thrown.
	 * 
	 * @param request      Current request for which error response to be processed.
	 * @param ex           Type of exception raised.
	 * @param errorMessage Details of the error.
	 * @return {@link ResponseEntity} that is responded by the service.
	 */
	private ResponseEntity<String> processReturnPacket(final HttpServletRequest request, final Throwable ex,
			final String errorMessage) {
		logger.error("Exception type: {}", ex.getClass());
		logger.error(ex.getMessage());
		logger.info("Creating error response");
		String contentType = "";
		if (StringUtils.isEmpty(request.getContentType())) {
			contentType = MediaType.APPLICATION_XML_VALUE;
		} else {
			contentType = request.getContentType();
		}
		String responseBody = UtilityMethods.parseDataPacket(contentType,
				GenericResponse.builder().error(Boolean.TRUE).message(errorMessage).build());
		if (MediaType.APPLICATION_XML_VALUE.equalsIgnoreCase(contentType)) {
			logger.info("Updating XML packet root based on request type.");
			responseBody = UtilityMethods.replaceXmlPacketNode(request.getServletPath(), ERROR_PACKET_NAME,
					responseBody);
		}
		logger.info("Response packet creation complete");
		return ResponseEntity.status(HttpStatus.OK).header(HttpHeaders.CONTENT_TYPE, contentType).body(responseBody);
	}
}
