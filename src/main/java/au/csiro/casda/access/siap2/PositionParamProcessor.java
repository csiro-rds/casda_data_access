package au.csiro.casda.access.siap2;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

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
 * A processor for the spatial position (POS) cutout filtering parameter. These may be either a single value or a range,
 * as defined in section 3.2.1 of the IVOA Access Data Version 1.0 Working Draft 20151021. Multiple sets of values are
 * also supported. All values are in degrees.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class PositionParamProcessor implements CutoutParamProcessor
{

    private static final String SIGNED_DECIMAL_NUMBER_PATTERN = "[\\+\\-]?\\d+(\\.\\d+)?(e[+-]?\\d+)?";

    /** The text to indicate an open ended lower bound in a range. */
    public static final String NEGATIVE_INFINTY = "-Inf";
    /** The text to indicate an open ended upper bound in a range.*/
    public static final String POSITIVE_INFINTY = "+Inf";

    private static final String OPTIONAL_MIN_NUMERIC_PATTERN =
            "(" + NEGATIVE_INFINTY + ")|(" + SIGNED_DECIMAL_NUMBER_PATTERN + ")";

    private static final String OPTIONAL_MAX_NUMERIC_PATTERN =
            "(\\" + POSITIVE_INFINTY + ")|(" + SIGNED_DECIMAL_NUMBER_PATTERN + ")";

    private static final String NUMERIC_RANGE_PATTERN =
            "(" + OPTIONAL_MIN_NUMERIC_PATTERN + ") +(" + OPTIONAL_MAX_NUMERIC_PATTERN + ")";

    /** CIRCLE takes a centre latitude and longitude and a radius. */
    private static final String CIRCLE_PATTERN = "CIRCLE( +" + SIGNED_DECIMAL_NUMBER_PATTERN + "){3}";

    /** RANGE takes a top left latitude and longitude and a bottom right latitude and longitude. */
    private static final String RANGE_PATTERN = "RANGE( +(" + NUMERIC_RANGE_PATTERN + ")){2}";

    /** POLYGON takes a top left latitude and longitude and a bottom right latitude and longitude. */
    private static final String POLYGON_PATTERN =
            "POLYGON( +" + SIGNED_DECIMAL_NUMBER_PATTERN + " +" + SIGNED_DECIMAL_NUMBER_PATTERN + "){3,}";

    /** The index of the first right ascension value in a range. */
    private static final int FIRST_RA_INDEX = 1;

    /** The index of the first declination value in a range. */
    private static final int FIRST_DEC_INDEX = 3;

    /** The index of the radius parameter in a CIRCLE position definition. */
    private static final int RADIUS_INDEX = 3;

    /**
     * Check the values of a POS parameter on a SODA request. 
     * 
     * @param values
     *            The values to be validated.
     * @return The error messages, or an empty list if all values are valid.
     */
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            String testParam = StringUtils.trimToEmpty(param);
            if (testParam.matches(CIRCLE_PATTERN))
            {
                String[] parts = testParam.split(" ");
                String error = verifyLongitude(parts[1], false);
                if (StringUtils.isNotBlank(error))
                {
                    errorList.add(error);
                    continue;
                }
                error = verifyLatitude(parts[2], false);
                if (StringUtils.isNotBlank(error))
                {
                    errorList.add(error);
                    continue;
                }
                error = verifyRadius(parts[RADIUS_INDEX]);
                if (StringUtils.isNotBlank(error))
                {
                    errorList.add(error);
                    continue;
                }
            }
            else if (testParam.matches(RANGE_PATTERN))
            {
                String[] parts = testParam.split(" ");
                for (int i = FIRST_RA_INDEX; i <= FIRST_RA_INDEX + 1; i++)
                {
                    String error = verifyLongitude(parts[i], true);
                    if (StringUtils.isNotBlank(error))
                    {
                        errorList.add(error);
                        continue;
                    }
                }
                for (int i = FIRST_DEC_INDEX; i <= FIRST_DEC_INDEX + 1; i++)
                {
                    String error = verifyLatitude(parts[i], true);
                    if (StringUtils.isNotBlank(error))
                    {
                        errorList.add(error);
                        continue;
                    }
                }
            }
            else if (testParam.matches(POLYGON_PATTERN))
            {
                String[] parts = testParam.split(" ");
                for (int i = 1; i < parts.length; i += 2)
                {
                    String error = verifyLongitude(parts[i], false);
                    if (StringUtils.isNotBlank(error))
                    {
                        errorList.add(error);
                        continue;
                    }
                    error = verifyLatitude(parts[i + 1], false);
                    if (StringUtils.isNotBlank(error))
                    {
                        errorList.add(error);
                        continue;
                    }
                }
            }
            else
            {                
                errorList.add(String.format(USAGE_FAULT_MSG, "Invalid POS value "  + testParam));
                continue;
            }
        }
        return errorList;
    }

    /**
     * Validate the longitude (Right Ascension).
     * 
     * @param value
     *            The value (in degrees) to be validated.
     * @param optional
     *            Is a missing value (e.g. NaN) allowed?
     * @return An error message, or null if the value is valid.
     */
    private String verifyLongitude(String value, boolean optional)
    {
        if (optional && (NEGATIVE_INFINTY.equals(value) || POSITIVE_INFINTY.equals(value)))
        {
            return null;
        }

        final String errMsg = String.format(USAGE_FAULT_MSG, "Invalid longitude value. Valid range is [0,360]");
        final double maxRa = 360d;
        try
        {
            double ra = Double.parseDouble(value);
            if (ra < 0d || ra > maxRa)
            {
                return errMsg;
            }
        }
        catch (NumberFormatException e)
        {
            return errMsg;
        }
        return null;
    }

    /**
     * Validate the latitude (Declination).
     * 
     * @param value
     *            The value (in degrees) to be validated.
     * @param optional
     *            Is a missing value (e.g. NaN) allowed?
     * @return An error message, or null if the value is valid.
     */
    private String verifyLatitude(String value, boolean optional)
    {
        if (optional && (NEGATIVE_INFINTY.equals(value) || POSITIVE_INFINTY.equals(value)))
        {
            return null;
        }

        final String errMsg = String.format(USAGE_FAULT_MSG, "Invalid latitude value. Valid range is [-90,90]");
        final double minDec = -90d;
        final double maxDec = 90d;
        try
        {
            double dec = Double.parseDouble(value);
            if (dec < minDec || dec > maxDec)
            {
                return errMsg;
            }
        }
        catch (NumberFormatException e)
        {
            return errMsg;
        }
        return null;
    }

    /**
     * Validate the search radius.
     * 
     * @param value
     *            The value (in degrees) to be validated.
     * @return An error message, or null if the value is valid.
     */
    private String verifyRadius(String value)
    {
        final String errMsg = String.format(USAGE_FAULT_MSG, "Invalid radius value. Valid range is [0,10]");
        final double maxRadius = 10d;
        try
        {
            double radius = Double.parseDouble(value);
            if (radius < 0 || radius > maxRadius)
            {
                return errMsg;
            }
        }
        catch (NumberFormatException e)
        {
            return errMsg;
        }
        return null;
    }

}
