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
 * An exception for when a resource is no longer available.
 */
@ResponseStatus(value = HttpStatus.GONE)
public class ResourceNoLongerAvailableException extends RuntimeException
{
    private static final long serialVersionUID = 1L;

    /**
     * Create a new ResourceNoLongerAvailableException with a plain message
     * @param message The description of the cause of the exception.
     */
    public ResourceNoLongerAvailableException(String message)
    {
        super(message);
    }

    /**
     * Create a new ResourceNoLongerAvailableException with a cause.
     * @param t The Exception or Error that caused the problem.
     */
    public ResourceNoLongerAvailableException(Throwable t)
    {
        super(t);
    }
}
