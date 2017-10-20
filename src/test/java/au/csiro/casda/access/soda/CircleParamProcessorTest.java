package au.csiro.casda.access.soda;

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
 * Validate the CircleParamProcessor class.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class CircleParamProcessorTest
{

    /**
     * Check the validateDouble method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCircleValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            List<Object[]> temp = Arrays.asList(new Object[][] { { "12.0 34.0 0.5" }, { "+182.5 -34.0 7.5" },
                    { "1.9461733726797e02 -4.9419355920530e01 0.05" },
                    { "1.9461733726797e+02 -4.9419355920530e+01 0.05" },
                    { new String[] { "276.7 -61.9 3.1667", "297 35 5" } } });
            return temp;
        }

        private String[] validParamValues;

        private CircleParamProcessor processor;

        public ValidateCircleValidTest(Object validValue) throws Exception
        {
            processor = new CircleParamProcessor();

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
        public void testValidateCircle()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the CircleParamProcessor's handling of values in the wrong format.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCircleInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] { { "12.0 34.0 0.5 7" }, { "+182.5 -34.0" },
                    { "-Inf 34.0 0.5" }, { "34.0 +Inf 0.5" }, { "34.0 0.5 NaN" }});
        }

        private String invalidParamValues;

        private CircleParamProcessor processor;

        public ValidateCircleInvalidTest(Object validValue) throws Exception
        {
            processor = new CircleParamProcessor();

            invalidParamValues = (String) validValue;
        }

        /**
         * Test method for
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidateCircle()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid CIRCLE value " + invalidParamValues),
                    processor.validate(new String[] { invalidParamValues }));
        }
    }

    /**
     * Check the CircleParamProcessor's handling of invalid longitude (ra) values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCircleLongitudeTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] { { "-1.0 34.0 0.5" }, { "360.001 -34.0 1" }});
        }

        private String invalidParamValues;

        private CircleParamProcessor processor;

        public ValidateCircleLongitudeTest(Object validValue) throws Exception
        {
            processor = new CircleParamProcessor();

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
     * Check the CircleParamProcessor's handling of invalid latitude (dec) values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCircleLatitudeTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] { { "12.0 102.0 0.5" }, { "+182.5 -98.0 7.5" } });
        }

        private String invalidParamValues;

        private CircleParamProcessor processor;

        public ValidateCircleLatitudeTest(Object validValue) throws Exception
        {
            processor = new CircleParamProcessor();

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

    /**
     * Check the CircleParamProcessor's handling of invalid circle radius values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateCircleRadiusTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (only singles for this test)
            return Arrays.asList(new Object[][] { { "12.0 34.0 -0.5" }, { "+182.5 -34.0 17.5" } });
        }

        private String invalidParamValues;

        private CircleParamProcessor processor;

        public ValidateCircleRadiusTest(Object validValue) throws Exception
        {
            processor = new CircleParamProcessor();

            invalidParamValues = (String) validValue;
        }

        /**
         * Test method for
         * {@link au.csiro.casda.votools.siap2.Siapv2Service#validateDouble(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidateRadius()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid radius value. Valid range is [0,10]"),
                    processor.validate(new String[] { invalidParamValues }));
        }
    }

}
