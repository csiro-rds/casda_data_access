package au.csiro.casda.access.soda;

import java.math.BigDecimal;

/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2010 - 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * A data transfer object for the equatorial bounds of a generated file, such as a cutout or spectrum.
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
public class GeneratedFileBounds
{
    private String ra;
    private String dec;
    private String xSize;
    private String ySize;
    private int minPlane = 1;
    private int maxPlane = 1;
    private String[] dimBounds = new String[3];
    private int numPlanes = 1;
    private String params;

    /**
     * Create a new GeneratedFileBounds instance without any details.
     */
    public GeneratedFileBounds()
    {
    }

    /**
     * Create a new GeneratedFileBounds instance without ySize.
     * 
     * @param ra
     *            The right ascension of the centre of the cutout image, in decimal degrees.
     * @param dec
     *            The declination of the centre of the cutout image, in decimal degrees.
     * @param xSize
     *            The width, in decimal degrees, of the cutout image.
     */
    public GeneratedFileBounds(String ra, String dec, double xSize)
    {
        this.ra = ra;
        this.dec = dec;
        this.xSize = String.format("%.6f", xSize);
    }

    /**
     * Create a new GeneratedFileBounds instance using BigDecimal values
     * 
     * @param ra
     *            The right ascension of the centre of the cutout image, in decimal degrees.
     * @param dec
     *            The declination of the centre of the cutout image, in decimal degrees.
     * @param xSize
     *            The width, in decimal degrees, of the cutout image.
     * @param ySize
     *            The height, in decimal degrees, of the cutout image.
     */
    public GeneratedFileBounds(BigDecimal ra, BigDecimal dec, BigDecimal xSize, BigDecimal ySize)
    {
        this.ra = ra.toPlainString();
        this.dec = dec.toPlainString();
        this.xSize = xSize.toPlainString();
        this.ySize = ySize.toPlainString();
    }

    /**
     * Create a new GeneratedFileBounds instance based on an existing GeneratedFileBounds.
     * 
     * @param bounds
     *            The GeneratedFileBounds to be copied.
     */
    public GeneratedFileBounds(GeneratedFileBounds bounds)
    {
        this(bounds.ra, bounds.dec, bounds.xSize, bounds.ySize);
        this.minPlane = bounds.minPlane;
        this.maxPlane = bounds.maxPlane;
        for (int i = 0; i < bounds.dimBounds.length; i++)
        {
            dimBounds[i] = bounds.dimBounds[i];
        }
    }

    /**
     * Create a new GeneratedFileBounds instance using string values
     * 
     * @param ra
     *            The right ascension of the centre of the cutout image, in decimal degrees.
     * @param dec
     *            The declination of the centre of the cutout image, in decimal degrees.
     * @param xSize
     *            The width, in decimal degrees, of the cutout image.
     * @param ySize
     *            The height, in decimal degrees, of the cutout image.
     */
    public GeneratedFileBounds(String ra, String dec, String xSize, String ySize)
    {
        this.ra = ra;
        this.dec = dec;
        this.xSize = xSize;
        this.ySize = ySize;
    }

    /**
     * Creates a new GeneratedFileBounds instance from a single string value matching the output of the toString methd.
     * 
     * @param input
     *            the string representation of the cutout bounds
     */
    public GeneratedFileBounds(String input)
    {
        final int raIndex = 0;
        final int decIndex = 1;
        final int xSizeIndex = 2;
        final int ySizeIndex = 3;
        final int dimMarkerIndex = 4;
        String[] vals = input.split(" ");
        this.ra = vals[raIndex];
        this.dec = vals[decIndex];
        this.xSize = vals[xSizeIndex];
        this.ySize = vals.length > ySizeIndex ? vals[ySizeIndex] : null;
        int currentIdx = dimMarkerIndex;
        if (vals.length > currentIdx && "D".equals(vals[currentIdx]))
        {
            currentIdx++;
            for (int i = 0; i < dimBounds.length; i++)
            {
                if (vals.length > currentIdx)
                {
                    String nextVal = vals[currentIdx];
                    if (!"N".equals(nextVal))
                    {
                        this.setDimBounds(i, "null".equals(nextVal) ? null : nextVal);
                        currentIdx++;
                    }
                }
            }
        }
        if (vals.length > currentIdx && "N".equals(vals[currentIdx]))
        {
            currentIdx++;
            this.numPlanes = vals.length > currentIdx ? Integer.parseInt(vals[currentIdx]) : 1;
        }
        
    }

    public String getRa()
    {
        return ra;
    }

    public void setRa(String ra)
    {
        this.ra = ra;
    }

    public String getDec()
    {
        return dec;
    }

    public void setDec(String dec)
    {
        this.dec = dec;
    }

    public String getXSize()
    {
        return xSize;
    }

    public void setXSize(String xSize)
    {
        this.xSize = xSize;
    }

    public String getYSize()
    {
        return ySize;
    }

    public void setYSize(String ySize)
    {
        this.ySize = ySize;
    }

    public int getMinPlane()
    {
        return minPlane;
    }

    public void setMinPlane(int minPlane)
    {
        this.minPlane = minPlane;
    }

    public int getMaxPlane()
    {
        return maxPlane;
    }

    public void setMaxPlane(int maxPlane)
    {
        this.maxPlane = maxPlane;
    }

    public String[] getDimBounds()
    {
        return this.dimBounds;
    }

    /**
     * Retrieve the bounds for a specific non spatial dimension (aka axis)
     * @param index The index of the dimension
     * @return The bounds string
     */
    public String getDimBounds(int index)
    {
        if (index < 0 || index >= this.dimBounds.length)
        {
            throw new IllegalArgumentException("index for bounds must be between 0 and " + (this.dimBounds.length-1));
        }
        return this.dimBounds[index];
    }

    /**
     * Set the bounds for a specific non spatial dimension (aka axis)
     * @param index The index of the dimension
     * @param bounds The bounds string
     */
    public void setDimBounds(int index, String bounds)
    {
        if (index < 0 || index >= this.dimBounds.length)
        {
            throw new IllegalArgumentException("index for bounds must be between 0 and " + (this.dimBounds.length-1));
        }
        this.dimBounds[index] = bounds;
    }

    public int getNumPlanes()
    {
        return numPlanes;
    }

    public void setNumPlanes(int numPlanes)
    {
        this.numPlanes = numPlanes;
    }

    public String getParams()
    {
        return params;
    }

    public void setParams(String params)
    {
        this.params = params;
    }

    /**
     * Calculates the field of view.
     * 
     * @return the estimate of the field of view
     */
    public double calculateFovEstimate()
    {
        double result = Double.parseDouble(xSize);
        if (ySize != null)
        {
            result *= Double.parseDouble(ySize);
        }
        else
        {
            result *= result;
        }
        return result;
    }

    /** {@inheritDoc} */
    @Override
    public String toString()
    {
        return ra + " " + dec + " " + xSize + " " + (ySize != null ? ySize : xSize) + " D " + dimBounds[0] + " "
                + dimBounds[1] + " N " + numPlanes;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dec == null) ? 0 : dec.hashCode());
        result = prime * result + ((ra == null) ? 0 : ra.hashCode());
        result = prime * result + ((xSize == null) ? 0 : xSize.hashCode());
        result = prime * result + ((ySize == null) ? 0 : ySize.hashCode());
        return result;
    }

    /** {@inheritDoc} */
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
        GeneratedFileBounds other = (GeneratedFileBounds) obj;
        if (dec == null)
        {
            if (other.dec != null)
            {
                return false;
            }
        }
        else if (!dec.equals(other.dec))
        {
            return false;
        }
        if (ra == null)
        {
            if (other.ra != null)
            {
                return false;
            }
        }
        else if (!ra.equals(other.ra))
        {
            return false;
        }
        if (xSize == null)
        {
            if (other.xSize != null)
            {
                return false;
            }
        }
        else if (!xSize.equals(other.xSize))
        {
            return false;
        }
        if (ySize == null)
        {
            if (other.ySize != null)
            {
                return false;
            }
        }
        else if (!ySize.equals(other.ySize))
        {
            return false;
        }
        for (int i = 0; i < dimBounds.length; i++)
        {
            String string = dimBounds[i];
            if (string == null) 
            {
                if (other.dimBounds[i] != null)
                {
                    return false;
                }
            }
            else if (!string.equals(other.dimBounds[i]))
            {
                return false;
            }
        }
        return true;
    }
}
