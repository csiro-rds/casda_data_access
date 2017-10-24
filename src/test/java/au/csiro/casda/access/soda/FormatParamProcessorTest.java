
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
 * Verify the workings of FormatParamProcessor.
 * <p>
 * Copyright 2017, CSIRO Australia All rights reserved.
 */
@RunWith(Enclosed.class)
public class FormatParamProcessorTest
{
    /**
     * Check the validate method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateFormatValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] {{ "PNG" },  { "png" }, { "image/png" }, 
                { "fits" }, { "FitS" }, { "application/fits" }, {"image/fits"}
                });
        }

        private String[] validParamValues;

        private FormatParamProcessor processor;

        public ValidateFormatValidTest(Object validValue) throws Exception
        {
            processor = new FormatParamProcessor();

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
        public void testValidateFormat()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the validate method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateFormatInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "image/bmp" }, { "BMP" }, 
                { "image/jpg" }, { "jpg" } });
        }

        private String[] invalidParamValues;

        private FormatParamProcessor processor;

        public ValidateFormatInvalidTest(Object invalidValue) throws Exception
        {
            processor = new FormatParamProcessor();

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
         * {@link au.csiro.casda.access.soda.FormatParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testInvalidFomat()
        {
            assertEquals("Expected '" + invalidParamValues[0] + "' to be invalid.",
                    Arrays.asList("UsageFault: Your query contained an invalid format: " 
                            + invalidParamValues[0]
                            + ". Valid formats include fits, image/png"),
                    processor.validate(invalidParamValues));
        }
    }

    /**
     * Check the validate method's handling of invalid number of values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateMultipleFormatInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { new String[]{"image/fits", "image/png"} } });
        }

        private String[] invalidParamValues;

        private FormatParamProcessor processor;

        public ValidateMultipleFormatInvalidTest(Object invalidValue) throws Exception
        {
            processor = new FormatParamProcessor();

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
         * {@link au.csiro.casda.access.soda.FormatParamProcessor#validate(java.lang.String, java.lang.String[])}.
         */
        @Test
        public void testMultipleFormatValues()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidParamValues) + "' to be invalid.",
                    Arrays.asList("UsageFault: Your query contained an invalid format. "
                            + "This query accepts only one format value e.g. application/fits"),
                    processor.validate(invalidParamValues));
        }
    }
}
