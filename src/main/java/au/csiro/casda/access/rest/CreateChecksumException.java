package au.csiro.casda.access.rest;

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
 * 
 * A general exception for unrecoverable exceptions creating a checksum file
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class CreateChecksumException extends Exception
{
    private static final long serialVersionUID = 1713767086105014692L;

    /**
     * Create a new CacheException without information.
     */
    public CreateChecksumException()
    {
        super();
    }

    /**
     * Create a new CreateChecksumException with a message and a cause.
     * 
     * @param message
     *            The description of the cause of the exception.
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public CreateChecksumException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Create a new CreateChecksumException with a plain message
     * 
     * @param message
     *            The description of the cause of the exception.
     */
    public CreateChecksumException(String message)
    {
        super(message);
    }

    /**
     * Create a new CreateChecksumException with a cause.
     * 
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public CreateChecksumException(Throwable cause)
    {
        super(cause);
    }
}
