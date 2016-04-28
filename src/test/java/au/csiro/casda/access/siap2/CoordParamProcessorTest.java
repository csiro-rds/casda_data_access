package au.csiro.casda.access.siap2;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
 * Verify the workings of CoordParamProcessor.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@RunWith(Enclosed.class)
public class CoordParamProcessorTest
{
    /**
     * Check the validateCoord method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCoordValidTest
    {
        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "axis 20 40" }, { " AXIS 20 " }, { "FREQ 1.5 2.5" },
                    { "FREQ 1e9 2e9" }, { "FREQ 1.5e9 2.5e9" }, { "FREQ 1.5e-2 2.5e-2" }, { "FREQ 1.5e+9 2.5e+9" }, 
                    { "FREQ 1.5" }, { "FREQ 1e9" }, { "FREQ 1.5E9" }, { "FREQ 1.5e-2" }, { "FREQ 1.5e+9" } });
            
        }
        
        private String[] validParamValues;

        private CoordParamProcessor processor;

        public ValidateCoordValidTest(Object validValue) throws Exception
        {
            processor = new CoordParamProcessor();

            if (validValue instanceof String[])
            {
                validParamValues = (String[]) validValue;
            }
            else if (validValue instanceof String)
            {
                validParamValues = new String[] { (String) validValue };
            }
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.siap2.CoordParamProcessor#validate(java.lang.String[])}.
         */
        @Test
        public void testValidateCoord()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the validate method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCoordInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "20 30" }, { "myaxis 20 30 40" }, { "myaxis 30 axis" }});
        }

        private CoordParamProcessor processor;

        private String invalidValue;

        public ValidateCoordInvalidTest(String invalidValue) throws Exception
        {
            this.invalidValue = invalidValue;
            processor = new CoordParamProcessor();
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.siap2.CoordParamProcessor#validate(java.lang.String[])}.
         */
        @Test
        public void testValidate()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidValue) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid COORD value " + invalidValue),
                    processor.validate(new String[] { invalidValue }));
        }
    }

    /**
     * Check the getRangesForAxis method.
     */
    public static class GetRangesForAxisTest
    {
        @Test
        public void testEmpty()
        {
            assertThat(CoordParamProcessor.getRangesForAxis(null, "foo"), is(empty()));
            assertThat(CoordParamProcessor.getRangesForAxis(new String[0], "foo"), is(empty()));
        }

        @Test
        public void testSingle()
        {
            String[] coordParams = new String[] { "VEL-HELO 17.5 18.3" };
            List<ValueRange> velRanges = CoordParamProcessor.getRangesForAxis(coordParams, "vel-helo");
            assertThat(velRanges, contains(new ValueRange(17.5, 18.3)));
        }

        @Test
        public void testSingleDifferentAxis()
        {
            String[] coordParams = new String[] { "VEL-HELO 17.5 18.3" };
            List<ValueRange> velRanges = CoordParamProcessor.getRangesForAxis(coordParams, "band");
            assertThat(velRanges, is(empty()));
        }

        @Test
        public void testMultiple()
        {
            String[] coordParams = new String[] { "VEL-HELO 17.5 18.3",  "VEL-HELO 35 40"};
            List<ValueRange> velRanges = CoordParamProcessor.getRangesForAxis(coordParams, "vel-helo");
            assertThat(velRanges, contains(new ValueRange(17.5, 18.3), new ValueRange(35, 40)));
        }

        @Test
        public void testMultipleVariousAxes()
        {
            String[] coordParams = new String[] { "VEL-HELO 17.5 18.3",  "VEL-HELO 35 40", "FREQ 800E6 850E6"};
            List<ValueRange> velRanges = CoordParamProcessor.getRangesForAxis(coordParams, "vel-helo");
            assertThat(velRanges, contains(new ValueRange(17.5, 18.3), new ValueRange(35, 40)));

            velRanges = CoordParamProcessor.getRangesForAxis(coordParams, "FREQ");
            assertThat(velRanges, contains(new ValueRange(800E6, 850E6)));
        
        }
    }
    
}
