
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
 * Copyright (C) 2010 - 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * Verify the workings of TimeParamProcessor.
 * <p>
 * Copyright 2016, CSIRO Australia All rights reserved.
 */
@RunWith(Enclosed.class)
public class TimeParamProcessorTest
{
    /**
     * Check the validate method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateTimeValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] {{ "55678.123456 55778.123456" },  { "55678 55778" }, 
                { "55678.1 55778.1234" }, { "55678.123456 55778" }, { "-Inf 55778.123456" }, { "55678.123456 +Inf" },
                { new String[]{ "55678.123456 55778", "-Inf 55999"}}});
        }

        private String[] validParamValues;

        private TimeParamProcessor processor;

        public ValidateTimeValidTest(Object validValue) throws Exception
        {
            processor = new TimeParamProcessor();

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
         * {@link au.csiro.casda.access.soda.TimeParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testValidateTime()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the validate method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateTimeInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "2012-01-01.345" }, { "2012-01-01.345 2012-01-02.123" }, 
                { "55678.123456a 55778.123456" }, { "55678.123456 55778.123456a" }, 
                { new String[]{ "55678.123456 55778", "-Inf 55999.1.2", "55678.123456 +Inf"}} });
        }

        private String[] invalidParamValues;

        private TimeParamProcessor processor;

        public ValidateTimeInvalidTest(Object invalidValue) throws Exception
        {
            processor = new TimeParamProcessor();

            if (invalidValue instanceof String[])
            {
                invalidParamValues = (String[]) invalidValue;
            }
            else if (invalidValue instanceof String)
            {
                invalidParamValues = new String[] { (String) invalidValue };
            }
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.soda.TimeParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testInValidateTime()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Your query contained an invalid time format: " 
                            + Arrays.toString(invalidParamValues)
                            + ". This query accepts +/-Inf and the MJD format, e.g. -Inf 55690.654321"),
                    processor.validate(invalidParamValues));
        }
    }
    
    /**
     * Check the validate method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateTimeInvalidAmountTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "55778.123456" }, {"55661.12 55786.34 55799.67"}});
        }

        private String[] invalidParamValues;

        private TimeParamProcessor processor;

        public ValidateTimeInvalidAmountTest(Object invalidValue) throws Exception
        {
            processor = new TimeParamProcessor();

            if (invalidValue instanceof String[])
            {
                invalidParamValues = (String[]) invalidValue;
            }
            else if (invalidValue instanceof String)
            {
                invalidParamValues = new String[] { (String) invalidValue };
            }
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.soda.TimeParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testInValidateTime()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Your query contained an invalid time format. This query accepts "
                            + "exactly two time values in MJD format, e.g.\t55678.123456 55690.654321"),
                    processor.validate(invalidParamValues));
        }
    }

    /**
     * Check the validate method's handling of invalid date range.
     */
    @RunWith(Parameterized.class)
    public static class ValidateTimeRangeInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "65678.123456 55778.123456" } });
        }

        private String[] invalidParamValues;

        private TimeParamProcessor processor;

        public ValidateTimeRangeInvalidTest(Object invalidValue) throws Exception
        {
            processor = new TimeParamProcessor();

            if (invalidValue instanceof String[])
            {
                invalidParamValues = (String[]) invalidValue;
            }
            else if (invalidValue instanceof String)
            {
                invalidParamValues = new String[] { (String) invalidValue };
            }
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.soda.TimeParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testInValidateTimeRange()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList(
                            "UsageFault: The first date in your query must be earlier (chronologically) than the second"),
                    processor.validate(invalidParamValues));
        }
    }

    /**
     * Check the validate method's handling of invalid date range.
     */
    @RunWith(Parameterized.class)
    public static class ValidateTimeRangeValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "45678.123456 55778.123456" }, { "45678.123456 45678.123456" } });
        }

        private String[] validParamValues;

        private TimeParamProcessor processor;

        public ValidateTimeRangeValidTest(Object validValue) throws Exception
        {
            processor = new TimeParamProcessor();

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
         * {@link au.csiro.casda.access.soda.TimeParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testInValidateTimeRange()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }
}
