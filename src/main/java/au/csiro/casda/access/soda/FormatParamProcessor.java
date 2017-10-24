package au.csiro.casda.access.soda;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
/*
 * #%L
 * CSIRO Data Access Portal
 * %%
 * Copyright (C) 2010 - 2017 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

import au.csiro.casda.access.ImageFormat;

/**
 * A processor for 'FORMAT' cutout output parameter. This will be either a mime type or a name of the format to be 
 * produced. 
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
public class FormatParamProcessor implements CutoutParamProcessor
{
    private static final String ERROR_MESSAGE_NOT_ONE_VALUE = "Your query contained an invalid format. "
            + "This query accepts only one format value e.g. application/fits";
    
    /**
     * {@inheritDoc}
     */
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        if (values.length != 1 || StringUtils.isBlank(values[0]))
        {
            errorList.add(String.format(USAGE_FAULT_MSG, ERROR_MESSAGE_NOT_ONE_VALUE));
        }
        else
        {
            ImageFormat imageFormat = ImageFormat.findMatchingFormat(values[0]);
            if (imageFormat == null)
            {
                errorList.add(String.format(USAGE_FAULT_MSG, "Your query contained an invalid format: " + values[0]
                        + ". Valid formats include fits, image/png"));
            }
        }

        return errorList;
    }

}
