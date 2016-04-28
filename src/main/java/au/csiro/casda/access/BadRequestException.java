package au.csiro.casda.access;

/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */



import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * An exception for bad service requests (which nevertheless map to a controller method).
 *  Instead of returning a success response (2xx code) and forwarding to an error page
 *  you can throw this exception and return a 400 response code.
 */
@ResponseStatus(value = HttpStatus.BAD_REQUEST)
public class BadRequestException extends RuntimeException
{
    /** Serialisation id */
    private static final long serialVersionUID = -1l;

    /**
     * Create a new BadRequestException with a plain message
     * @param message The description of the cause of the exception.
     */
    public BadRequestException(String message)
    {
        super(message);
    }

    /**
     * Create a new BadRequestException with a cause.
     * @param t The Exception or Error that caused the problem.
     */
    public BadRequestException(Throwable t)
    {
        super(t);
    }
}