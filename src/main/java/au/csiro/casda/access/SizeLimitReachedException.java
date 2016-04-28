package au.csiro.casda.access;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

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


/**
 * An exception for when the request cannot be serviced because the file size is larger than the configured size limit.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
@ResponseStatus(value = HttpStatus.REQUEST_ENTITY_TOO_LARGE)
public class SizeLimitReachedException extends RuntimeException
{

    private static final long serialVersionUID = 1L;

    /**
     * Create a new SizeLimitReachedException with a message and a cause.
     * 
     * @param message
     *            The description of the cause of the exception.
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public SizeLimitReachedException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Create a new SizeLimitReachedException with a plain message
     * 
     * @param message
     *            The description of the cause of the exception.
     */
    public SizeLimitReachedException(String message)
    {
        super(message);
    }

    /**
     * Create a new SizeLimitReachedException with a cause.
     * 
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public SizeLimitReachedException(Throwable cause)
    {
        super(cause);
    }

}
