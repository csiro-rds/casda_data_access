package au.csiro.casda.access.siap2;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
 * Validate the CutoutBounds class.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class CutoutBoundsTest
{
    @Test
    public void testConstructorSingleString()
    {
        CutoutBounds boundOne = new CutoutBounds("1", "2", 7.7);
        CutoutBounds boundOneFull = new CutoutBounds("1", "2", "7.7", "7.7");
        assertEquals(boundOneFull, new CutoutBounds(boundOne.toString()));

        CutoutBounds boundTwo = new CutoutBounds("1", "2", "7.7", "11");
        assertEquals(boundTwo, new CutoutBounds(boundTwo.toString()));
    }

    @Test
    public void testCalculateFovEstimate()
    {
        CutoutBounds boundOne = new CutoutBounds("1", "2", 7.7);
        assertEquals(59.29, boundOne.calculateFovEstimate(), 0.0000001);
        
        CutoutBounds boundTwo = new CutoutBounds("1", "2", "7.7", "11");
        assertEquals(84.7, boundTwo.calculateFovEstimate(), 0.0000001);
    }
}
