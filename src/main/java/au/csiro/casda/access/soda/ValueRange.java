package au.csiro.casda.access.soda;

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
 * Contains the minimum and maximum values for an axis range specification. e.g. may contain 8.0E8, 9.0E8 to specify a
 * 800MHz - 900MHz range on a FREQ axis.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class ValueRange
{
    private final double minValue;
    private final double maxValue;

    /**
     * Create a new ValueRange instance.
     * 
     * @param minValue
     *            The smallest value in the range.
     * @param maxValue
     *            The largest value in the range.
     */
    public ValueRange(double minValue, double maxValue)
    {
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public double getMinValue()
    {
        return minValue;
    }

    public double getMaxValue()
    {
        return maxValue;
    }

    @Override
    public String toString()
    {
        return "ValueRange [minValue=" + minValue + ", maxValue=" + maxValue + "]";
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(maxValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minValue);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        ValueRange other = (ValueRange) obj;
        if (Double.doubleToLongBits(maxValue) != Double.doubleToLongBits(other.maxValue))
        {
            return false;
        }
        if (Double.doubleToLongBits(minValue) != Double.doubleToLongBits(other.minValue))
        {
            return false;
        }
        return true;
    }

}
