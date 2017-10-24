package au.csiro.casda.access.soda;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/*
 * #%L
 * CSIRO Data Access Portal
 * %%
 * Copyright (C) 2010 - 2016 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * A processor for the spatial Circle SIAP parameter type. These must be a set of 3 values; ra, dec and radius,
 * as defined in section 2.1 of the IVOA Simple Image Access Version 2.0 Recommendation. Multiple sets of values are 
 * also supported.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class CircleParamProcessor extends PositionParamProcessor
{
    private static final String CIRCLE_PATTERN= "^([+-]?[0-9]+([.][0-9]+)?(e[+-]?\\d+)? ){2}[+-]?[0-9]+([.][0-9]+)?$";

    @Override
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            String testParam = StringUtils.trimToEmpty(param);
            if(testParam.matches(CIRCLE_PATTERN))
            {
                validateCircle(testParam, errorList);
            }
            else
            {
                errorList.add(String.format(USAGE_FAULT_MSG, "Invalid CIRCLE value "  + testParam));
            }
        }
        
        return errorList;
    }
 
}
