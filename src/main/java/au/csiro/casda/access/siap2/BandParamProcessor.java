package au.csiro.casda.access.siap2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import au.csiro.casda.access.soda.ValueRange;
import au.csiro.util.AstroConversion;

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
 * A processor for 'BAND' cutout filtering parameter.These may be either a single value or 
 * interval value. 
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class BandParamProcessor implements CutoutParamProcessor
{
    private static final String NEGATIVE_INFINTY = "-Inf";
    private static final String POSITIVE_INFINTY = "+Inf";

    private static final String SINGLE_NUMERIC_VALUE_PATTERN = "\\d+(\\.\\d+)?(e[+-]?\\d+)?";

    private static final String OPTIONAL_MIN_NUMERIC_PATTERN =
            "(" + NEGATIVE_INFINTY + ")|(" + SINGLE_NUMERIC_VALUE_PATTERN + ")";

    private static final String OPTIONAL_MAX_NUMERIC_PATTERN =
            "(\\" + POSITIVE_INFINTY + ")|(" + SINGLE_NUMERIC_VALUE_PATTERN + ")";

    private static final String NUMERIC_RANGE_PATTERN =
            "(" + OPTIONAL_MIN_NUMERIC_PATTERN + ") +(" + OPTIONAL_MAX_NUMERIC_PATTERN + ")";

    /** The minimum frequency value in Hertz - frequencies must be positive and non zero. */ 
    static final double MIN_FREQ_VALUE = 0.000000001d;

    @Override
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            String testParam = StringUtils.trimToEmpty(param);
            // May be either empty, a single value e.g. 3e5 or a range e.g. NaN 1.76e-5
            if (testParam.length() != 0 && !Pattern.matches(SINGLE_NUMERIC_VALUE_PATTERN, testParam)
                    && !Pattern.matches(NUMERIC_RANGE_PATTERN, testParam))
            {
                errorList.add(String.format(USAGE_FAULT_MSG, "Invalid BAND value "  + param));
            }
            else
            {
                // check min < max
                String[] range = param.trim().split(" +");
                if (range.length == 2)
                {
                    Double val1 = paramValue(range[0]);
                    Double val2 = paramValue(range[1]);
                    if (!val1.equals(Double.NaN) && !val2.equals(Double.NaN) && val1 > val2)
                    {
                        errorList.add(String.format(USAGE_FAULT_MSG, "Invalid BAND value "  + param));
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
        if (NEGATIVE_INFINTY.equals(value) || POSITIVE_INFINTY.equals(value))
        {
            return Double.NaN;
        }
        return Double.valueOf(value);
    }

    /**
     * Convert the provided band params (in units of metres) to ValueRanges in frequency units (Hz).
     * 
     * @param bandParams
     *            The band criteria to be converted.
     * @return A list of ValueRange objects describing frequency ranges.
     */
    public static List<ValueRange> getFreqRanges(String[] bandParams)
    {
        List<ValueRange> freqRangeList = new ArrayList<>();
        if (ArrayUtils.isEmpty(bandParams))
        {
            return freqRangeList;
        }

        for (String range : bandParams)
        {
            String[] wavelengths = range.split(" ");

            double minFreq = AstroConversion.wavelengthToFrequency(
                    paramValue(wavelengths.length > 1 ? wavelengths[1] : wavelengths[0]));
            if (Double.isNaN(minFreq))
            {
                minFreq = MIN_FREQ_VALUE;
            }
            double maxFreq = AstroConversion.wavelengthToFrequency(paramValue(wavelengths[0]));
            if (Double.isNaN(maxFreq))
            {
                maxFreq = Double.MAX_VALUE;
            }
            freqRangeList.add(new ValueRange(minFreq, maxFreq));
        }

        return freqRangeList;
    }
}
