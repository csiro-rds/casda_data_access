package au.csiro.casda.access.siap2;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import au.csiro.casda.access.soda.ValueRange;

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
 * A processor for 'COORD' cutout filtering parameter.This param includes the name of the axis and either a numeric
 * value or a numeric range
 * <P>
 * eg: 'COORD=myAxis 23 40'
 * 
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */

public class CoordParamProcessor implements CutoutParamProcessor
{
    private static final String VALID_PARAM =
            "^[a-zA-Z-_]+ [0-9]+(\\.[0-9]+)?(E[+-]?[0-9]+)?( [0-9]+(\\.[0-9]+)?(E[+-]?[0-9]+)?)?$";

    @Override
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            String testParam = StringUtils.trimToEmpty(param).toUpperCase();
            if (!testParam.matches(VALID_PARAM))
            {
                errorList.add(String.format(USAGE_FAULT_MSG, "Invalid COORD value " + param));
            }

        }
        return errorList;
    }

    /**
     * Extract the cutout ranges specified in COORD parameters for the named axis.
     * 
     * @param coordParams
     *            The coord criteria to be checked.
     * @param name
     *            The name of the axis.
     * @return A list of relevant value ranges, will not be null.
     */
    public static List<ValueRange> getRangesForAxis(String[] coordParams, String name)
    {
        List<ValueRange> coordRangeList = new ArrayList<>();
        if (ArrayUtils.isEmpty(coordParams))
        {
            return coordRangeList;
        }

        for (String range : coordParams)
        {
            String[] parts = range.split(" ");
            if (parts[0].toUpperCase().equals(name.toUpperCase()))
            {
                double minVal = Double.parseDouble(parts[1]);
                double maxVal = Double.parseDouble(parts.length > 2 ? parts[2] : parts[1]);
                coordRangeList.add(new ValueRange(minVal, maxVal));
            }

        }
        return coordRangeList;
    }

}
