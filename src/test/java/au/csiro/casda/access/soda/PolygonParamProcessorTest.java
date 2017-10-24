package au.csiro.casda.access.soda;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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
 * Validate the PolygonParamProcessor class.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class PolygonParamProcessorTest
{

    /**
     * Check the validateDouble method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidatePolygonValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] {
                    { "12.0 34.0 14.0 35.0 14.0 36.0 12.0 35.0" },
                    { "112.0 34.0 118.0 36 118.0 -10.0 112.0 -10.0 89.0 0" }});
        }

        private String[] validParamValues;

        private PolygonParamProcessor processor;

        public ValidatePolygonValidTest(Object validValue) throws Exception
        {
            processor = new PolygonParamProcessor();

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
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidatePolygon()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the PolygonParamProcessor's handling of values in the wrong format.
     */
    @RunWith(Parameterized.class)
    public static class ValidatePolygonInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] {{ "12.0 34.0 14.0 35.0" },
                    { "112.0 34.0 118.0 36 118.0 -10.0 112.0 -10.0 89.0" },
                    { "12.0 34.0 14.0 35.0 NaN 17" }, { "12.0 34.0 14.0 35.0 17 NaN" } });
        }

        private String invalidParamValues;

        private PolygonParamProcessor processor;

        public ValidatePolygonInvalidTest(Object validValue) throws Exception
        {
            processor = new PolygonParamProcessor();

            invalidParamValues = (String) validValue;
        }

        /**
         * Test method for
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidatePolygon()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid POLYGON value " + invalidParamValues),
                    processor.validate(new String[] { invalidParamValues }));
        }
    }

    /**
     * Check the PolygonParamProcessor's handling of invalid longitude (ra) values.
     */
    @RunWith(Parameterized.class)
    public static class ValidatePolygonLongitudeTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] { 
                    { "412.0 34.0 14.0 35.0 5 5" }, { "112.0 34.0 -0.001 36 118.0 -10.0 112.0 -10.0" },
                    { "112.0 34.0 0.001 36 361.0 -10.0 112.0 -10.0" } });
        }

        private String invalidParamValues;

        private PolygonParamProcessor processor;

        public ValidatePolygonLongitudeTest(Object validValue) throws Exception
        {
            processor = new PolygonParamProcessor();

            invalidParamValues = (String) validValue;
        }

        /**
         * Test method for
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidateLongitude()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid longitude value. Valid range is [0,360]"),
                    processor.validate(new String[] { invalidParamValues }));
        }
    }

    /**
     * Check the PolygonParamProcessor's handling of invalid latitude (dec) values.
     */
    @RunWith(Parameterized.class)
    public static class ValidatePolygonLatitudeTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] {{ "12.0 134.0 14.0 35.0 14.0 36.0 12.0 35.0" },
                    { "112.0 34.0 118.0 -98 118.0 -10.0 112.0 -10.0 89.0 0" } });
        }

        private String invalidParamValues;

        private PolygonParamProcessor processor;

        public ValidatePolygonLatitudeTest(Object validValue) throws Exception
        {
            processor = new PolygonParamProcessor();

            invalidParamValues = (String) validValue;
        }

        /**
         * Test method for
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidateLatitude()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid latitude value. Valid range is [-90,90]"),
                    processor.validate(new String[] { invalidParamValues }));
        }
    }

}
