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


/**
 * An general exception for unrecoverable exceptions using the inline script service.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class InlineScriptException extends Exception
{

    private static final long serialVersionUID = 1L;

    /**
     * Create a new InlineScriptException without information.
     */
    public InlineScriptException()
    {
        super();
    }

    /**
     * Create a new InlineScriptException with a message and a cause.
     * 
     * @param message
     *            The description of the cause of the exception.
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public InlineScriptException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Create a new InlineScriptException with a plain message
     * 
     * @param message
     *            The description of the cause of the exception.
     */
    public InlineScriptException(String message)
    {
        super(message);
    }

    /**
     * Create a new InlineScriptException with a cause.
     * 
     * @param cause
     *            The Exception or Error that caused the problem.
     */
    public InlineScriptException(Throwable cause)
    {
        super(cause);
    }

}
