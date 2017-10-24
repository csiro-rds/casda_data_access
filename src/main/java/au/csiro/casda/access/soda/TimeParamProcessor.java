package au.csiro.casda.access.soda;

import java.util.ArrayList;
import java.util.Arrays;
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
 * A processor for 'Time' cutout filtering parameter.These must be an interval value. 
 * <p>
 * Copyright 2016, CSIRO Australia. All rights reserved.
 */
public class TimeParamProcessor implements CutoutParamProcessor
{
    private static final String NUMBER = "\\d+(\\.\\d+)?";
    private static final String INF = "[+-]{1}Inf";
    private static final String ERROR_MESSAGE_NOT_TWO_VALUES = "Your query contained an invalid time format. "
            + "This query accepts exactly two time values in MJD format, e.g.\t55678.123456 55690.654321";
    private static final String ERROR_MESSAGE_DATE_ORDER = 
            "The first date in your query must be earlier (chronologically) than the second";
    
    /**
     * {@inheritDoc}
     */
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            if (StringUtils.isBlank(param))
            {
                errorList.add(String.format(USAGE_FAULT_MSG, ERROR_MESSAGE_NOT_TWO_VALUES));
            }
            else
            {
                for (String date : param.trim().split(" +"))
                {
                    if (!date.matches(NUMBER) && !date.matches(INF))
                    {
                        errorList.add(String.format(USAGE_FAULT_MSG, "Your query contained an invalid time format: " 
                                + Arrays.toString(values) 
                                + ". This query accepts +/-Inf and the MJD format, e.g. -Inf 55690.654321"));
                        break;
                    }

                }
                if (errorList.isEmpty())
                {
                    String[] dates = param.trim().split(" +");
                    if (dates.length != 2)
                    {
                        errorList.add(String.format(USAGE_FAULT_MSG, ERROR_MESSAGE_NOT_TWO_VALUES));
                    }
                    else
                    {
                        Double val1 = paramValue(dates[0]);
                        Double val2 = paramValue(dates[1]);
                        if (!val1.equals(Double.NaN) && !val2.equals(Double.NaN) && val1 > val2)
                        {
                            errorList.add(String.format(USAGE_FAULT_MSG, ERROR_MESSAGE_DATE_ORDER));
                        }
                    }
                }
            }
        }

        return errorList;
    }
    
    private static double paramValue(String value)
    {
        if (StringUtils.isBlank(value))
        {
            return Double.NaN;
        }
        if (value.matches(INF))
        {
            return Double.NaN;
        }
        return Double.valueOf(value);
    }

}
