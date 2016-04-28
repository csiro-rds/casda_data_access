package au.csiro.casda.access;

import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.accept.HeaderContentNegotiationStrategy;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;
import au.csiro.casda.services.dto.MessageDTO;

/**
 * Exception Handler to redirect exceptions to the error page, or throw recognised exceptions.
 * <p>
 * Please note that for this class to work as designed, the default ErrorPageFilter created by Spring Boot needs to be
 * disabled.  See {@link DataAccessApplication} for more details.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@ControllerAdvice
class ExceptionHandlers
{

    private static enum ExceptionResponseType
    {
        TEXT
        {
            @Override
            public Object createReponse(HttpStatus statusCode, String message, HttpServletResponse response)
            {
                HttpHeaders headers = new HttpHeaders();
                // Initialise the content type for the response as plain text.
                headers.setContentType(MediaType.TEXT_PLAIN);
                response.setStatus(statusCode.value());
                return new ResponseEntity<String>("FAILURE: " + message, headers, statusCode);
            }

            @Override
            public boolean matchesMediaType(MediaType mediaType)
            {
                return MediaType.TEXT_PLAIN.includes(mediaType);
            }
        },
        JSON
        {
            @Override
            public Object createReponse(HttpStatus statusCode, String message, HttpServletResponse response)
            {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                response.setStatus(statusCode.value());
                return new ResponseEntity<MessageDTO>(new MessageDTO(MessageDTO.MessageCode.FAILURE, message), headers,
                        statusCode);
            }

            @Override
            public boolean matchesMediaType(MediaType mediaType)
            {
                return MediaType.APPLICATION_JSON.includes(mediaType);
            }
        },
        XML
        {
            @Override
            public Object createReponse(HttpStatus statusCode, String message, HttpServletResponse response)
            {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_XML);
                response.setStatus(statusCode.value());
                MessageDTO messageDto = new MessageDTO(MessageDTO.MessageCode.FAILURE, message);
                return new ResponseEntity<MessageDTO>(messageDto, headers, statusCode);
            }

            @Override
            public boolean matchesMediaType(MediaType mediaType)
            {
                return MediaType.APPLICATION_XML.includes(mediaType);
            }
        },
        HTML
        {
            @Override
            public Object createReponse(HttpStatus statusCode, String message, HttpServletResponse response)
            {
                response.setStatus(statusCode.value());
                response.setContentType(MediaType.TEXT_HTML_VALUE);
                ModelAndView mav = new ModelAndView();
                mav.setViewName(DEFAULT_ERROR_VIEW);
                mav.getModel().put("message", message);
                return mav;
            }

            @Override
            public boolean matchesMediaType(MediaType mediaType)
            {
                return MediaType.TEXT_HTML.includes(mediaType) || MediaType.APPLICATION_XHTML_XML.includes(mediaType)
                        || MediaType.ALL.includes(mediaType);
            }
        };

        public abstract Object createReponse(HttpStatus statusCode, String message, HttpServletResponse response);

        public abstract boolean matchesMediaType(MediaType mediaType);
    }

    /**
     * Default view for errors will match associated page *.jsp
     */
    public static final String DEFAULT_ERROR_VIEW = "error";

    private static final Logger logger = LoggerFactory.getLogger(ExceptionHandlers.class);

    /**
     * Exception handler for ServletException that responds with an appropriate status code and details in a
     * JSON-serialised MessageDTO.
     * 
     * @param ex
     *            the exception thrown by the application
     * @param request
     *            the web request
     * @param response
     *            the servlet response
     * @return a ResponseEntity
     */
    @ExceptionHandler({ ServletException.class })
    public Object handleServletException(ServletException ex, NativeWebRequest request, HttpServletResponse response)
    {
        logger.debug("There was a {} processing request: {}", ex.getClass().getName(), request, ex);
        /*
         * Delegating to a ResponseEntityExceptionHandler because it does all the hard work of translating Spring's
         * internal exceptions into the right response status codes and messages.
         */
        ResponseEntityExceptionHandler handler = new ResponseEntityExceptionHandler()
        {
        };
        ResponseEntity<Object> defaultResponse = handler.handleException(ex, request);

        return getExceptionResponseType(ex, request.getContextPath(), getRequestMediaTypes(request))
                .createReponse(defaultResponse.getStatusCode(), ex.getMessage(), response);
    }

    /**
     * Exception handler for Exception that responds with an appropriate status code and details in a JSON-serialised
     * MessageDTO.
     * 
     * @param ex
     *            the exception thrown by the application
     * @param request
     *            the web request
     * @param response
     *            the servlet response
     * @return a ResponseEntity
     */
    @ExceptionHandler({ Exception.class, RuntimeException.class })
    public Object handleRemainingExceptions(Throwable ex, NativeWebRequest request, HttpServletResponse response)
    {
        if (ex instanceof RuntimeException)
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add(String.format("An unexpected exception occured trying to process request '%s'",
                    request.toString()));
            logger.error(builder.toString(), ex);
        }

        HttpStatus returnStatus;
        ResponseStatus status = AnnotationUtils.findAnnotation(ex.getClass(), ResponseStatus.class);
        if (status != null && status.value() != null)
        {
            returnStatus = status.value();
        }
        else
        {
            returnStatus = HttpStatus.INTERNAL_SERVER_ERROR;
        }
        return getExceptionResponseType(ex, request.getContextPath(), getRequestMediaTypes(request))
                .createReponse(returnStatus, ex.getMessage(), response);
    }

    private List<MediaType> getRequestMediaTypes(NativeWebRequest request)
    {
        try
        {
            return new HeaderContentNegotiationStrategy().resolveMediaTypes(request);
        }
        catch (HttpMediaTypeNotAcceptableException e)
        {
            return Arrays.asList(new MediaType("*/*"));
        }
    }

    private ExceptionResponseType getExceptionResponseType(Throwable t, String contextPath, List<MediaType> mediaTypes)
    {
        for (MediaType mediaType : mediaTypes)
        {
            for (ExceptionResponseType exceptionResponseType : ExceptionResponseType.values())
            {
                if (exceptionResponseType.matchesMediaType(mediaType))
                {
                    return exceptionResponseType;
                }
            }
        }
        return ExceptionResponseType.HTML;
    }

}