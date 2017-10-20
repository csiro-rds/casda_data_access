package au.csiro.casda.access.soda;

import java.util.List;

/*
 * #%L
 * CSIRO Data Access Portal
 * %%
 * Copyright (C) 2010 - 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * An interface defining a processor for a particular type of SIAP parameter.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public interface CutoutParamProcessor
{
    /** Invalid input  */   
    public static final String USAGE_FAULT_MSG = "UsageFault: %s"; 
    
    /**
     * Check the values of a parameter on a SIAP v2 request. Implementations will each deal with a specific type of
     * parameter.
     * 
     * @param values
     *            The values to be validated.
     * @return The error messages, or an empty list if all values are valid.
     */
    List<String> validate(String[] values);
}
