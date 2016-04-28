package au.csiro.casda.access.siap2;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.EnumUtils;
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
 * A processor for 'POL' cutout filtering parameter.These may be either a single value or multiple, and includes ONLY
 * symbols from the {I Q U V} set.
 * 
 * eg: POL=I&POL=Q
 * 
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class PolParamProcessor implements CutoutParamProcessor
{

    /**
     * @param values the values to validate
     * @return a list including any errors, or an empty list if values are valid
     */
    public List<String> validate(String[] values)
    {
        List<String> errorList = new ArrayList<String>();

        for (String param : values)
        {
            String testParam = StringUtils.trimToEmpty(param).toUpperCase();
            if (testParam.length() != 0 && !EnumUtils.isValidEnum(PolarizationStateType.class, testParam))
            {
                errorList.add(String.format(USAGE_FAULT_MSG, "Invalid POL value "  + param));
            }
        }
        return errorList;
    }
    
    
    /**
     * The type of Polarization State.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public enum PolarizationStateType
    {
        /** Stokes parameters : Total intensity */
        I(1), //
        /** Stokes parameters : Linear polarization (P cos(2x)) */
        Q(2), //
        /** Stokes parameters : Linear polarization (P sin(2x)) */
        U(3), //
        /** Stokes parameters : Circular polarization */
        V(4);

        private final int fitsValue;

        private PolarizationStateType(int fitsValue)
        {
            this.fitsValue = fitsValue;
        }

        public int getFitsValue()
        {
            return fitsValue;
        }

    }

    /**
     * Extract the requested FITS value ranges from an array of pol states.
     * 
     * @param polParams
     *            An array of Stokes polarisation string (I, Q etc)
     * @return A list of value ranges, will not be null.
     */
    public static List<ValueRange> getPolRanges(String[] polParams)
    {
        List<ValueRange> polRangeList = new ArrayList<>();
        if (ArrayUtils.isEmpty(polParams))
        {
            return polRangeList;
        }

        EnumSet<PolarizationStateType> polStates = EnumSet.noneOf(PolarizationStateType.class);
        for (String polString : polParams)
        {
            polString = StringUtils.trimToEmpty(polString).toUpperCase();
            if (polString.length() > 0)
            {
                PolarizationStateType stokesPol = PolarizationStateType.valueOf(polString);
                polStates.add(stokesPol);
            }
        }

        ValueRange currRange = null;
        for (PolarizationStateType pol : PolarizationStateType.values())
        {
            if (polStates.contains(pol))
            {
                if (currRange == null)
                {
                    currRange = new ValueRange(pol.fitsValue, pol.fitsValue);
                }
                else
                {
                    currRange = new ValueRange(currRange.getMinValue(), pol.fitsValue);
                }
            }
            else if (currRange != null)
            {
                polRangeList.add(currRange);
                currRange = null;
            }
        }
        if (currRange != null)
        {
            polRangeList.add(currRange);
        }

        return polRangeList;
    }
}
