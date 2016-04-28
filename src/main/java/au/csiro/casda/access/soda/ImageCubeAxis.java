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
 * An axis of a multi dimensional FITS image cube (e.g. FREQ). The positional axes (e.g. right ascension/declination)
 * are not expected to be recorded here as the axis is assumed to be linear.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class ImageCubeAxis
{
    private String name;
    private int index;
    private int size;
    private double minVal;
    private double maxVal;
    private double delta;
    private int planeSpan;

    /**
     * Create a new ImageCubeAxis instance.
     * 
     * @param index
     *            The order of this axis in the image cube.
     * @param name
     *            The name of the axis.
     * @param size
     *            The number of pixels in the axis.
     * @param minVal
     *            The lower bound of the smallest value in the axis' range.
     * @param maxVal
     *            The upper bound of the largest value in the axis' range.
     * @param delta
     *            The step value per pixel.
     */
    public ImageCubeAxis(int index, String name, int size, double minVal, double maxVal, double delta)
    {
        this.index = index;
        this.name = name;
        this.size = size;
        this.minVal = minVal;
        this.maxVal = maxVal;
        this.delta = delta;

    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public int getIndex()
    {
        return index;
    }

    public void setIndex(int index)
    {
        this.index = index;
    }

    public int getSize()
    {
        return size;
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public double getMinVal()
    {
        return minVal;
    }

    public void setMinVal(double minVal)
    {
        this.minVal = minVal;
    }

    public double getMaxVal()
    {
        return maxVal;
    }

    public void setMaxVal(double maxVal)
    {
        this.maxVal = maxVal;
    }

    public double getDelta()
    {
        return delta;
    }

    public void setDelta(double delta)
    {
        this.delta = delta;
    }

    /**
     * A plane span is the number of planes each value in this axis will span. Effectively it is the total size of all
     * later axes. e.g. for a cube with 6 frequencies (axis 3: FREQ) and 2 stokes polarisations (axis 4: STOKES), the
     * FREQ span would be 2 being the number of polarisations recorded for each frequency.
     * 
     * @return The number of planes spanned per value of this axis.
     */
    public int getPlaneSpan()
    {
        return planeSpan;
    }

    public void setPlaneSpan(int planeSpan)
    {
        this.planeSpan = planeSpan;
    }

    @Override
    public String toString()
    {
        return "ImageCubeAxis [name=" + name + ", size=" + size + "]";
    }
}
