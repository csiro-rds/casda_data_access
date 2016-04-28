package au.csiro.casda.access.cache;

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
 * An exception for when the cache is full.
 * 
 * Copyright 2015, CSIRO Australia
 * All rights reserved.
 * 
 */
public class CacheFullException extends CacheException
{

    private static final long serialVersionUID = 1L;

    /**
     * Create a new CacheFullException without information.
     */
    public CacheFullException()
    {
        super();
    }

    /**
     * Create a new CacheFullException with a message and a cause.
     * @param message The description of the cause of the exception.
     * @param cause The Exception or Error that caused the problem.
     */
    public CacheFullException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Create a new CacheFullException with a plain message
     * @param message The description of the cause of the exception.
     */
    public CacheFullException(String message)
    {
        super(message);
    }

    /**
     * Create a new CacheFullException with a cause.
     * @param cause The Exception or Error that caused the problem.
     */
    public CacheFullException(Throwable cause)
    {
        super(cause);
    }

    
    
}
