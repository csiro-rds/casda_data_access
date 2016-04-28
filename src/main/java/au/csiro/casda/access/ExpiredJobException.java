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
 * An exception for expired data access jobs.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 */
@ResponseStatus(value = HttpStatus.LOCKED)
public class ExpiredJobException extends Exception
{
    private static final long serialVersionUID = -1l;

    /**
     * Create a new ExpiredJobException with a plain message
     * @param message The description of the cause of the exception.
     */
    public ExpiredJobException(String message)
    {
        super(message);
    }

    /**
     * Create a new ExpiredJobException with a cause.
     * @param t The Exception or Error that caused the problem.
     */
    public ExpiredJobException(Throwable t)
    {
        super(t);
    }
}