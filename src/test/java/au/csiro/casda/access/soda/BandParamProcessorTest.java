package au.csiro.casda.access.soda;

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
 * Verify the workings of BandParamProcessor.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@RunWith(Enclosed.class)
public class BandParamProcessorTest
{
    /**
     * Check the validateDouble method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "300 600" }, { "300 +Inf" }, { "-Inf 600" }, { "-Inf +Inf" },
                    { "3.5e-1 4e-1" }, { "3.5e-1   4e-1" } });
        }

        private String[] validParamValues;

        private BandParamProcessor processor;

        public ValidTest(Object validValue) throws Exception
        {
            processor = new BandParamProcessor();

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
         * {@link au.csiro.casda.access.soda.votools.siap2.BandParamProcessor#validate(java.lang.String[])}.
         */
        @Test
        public void testValidate()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the validateDouble method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class InvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "a 600" },
                    { "300 NaN" }, { "NaN 600" }, { "NaN NaN" }, { "+Inf 600" }, { "300 -Inf" }, { "+Inf -Inf" },
                    { "-inf 600" }, { "300 +inf" }, { "inf 600" }, { "300 inf" }, { "2.3e 600" }, 
                    { "2.3e 6" }, { "2.3 e-5" } });
        }

        private BandParamProcessor processor;

        private String invalidValue;

        public InvalidTest(String invalidValue) throws Exception
        {
            this.invalidValue = invalidValue;
            processor = new BandParamProcessor();
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.soda.votools.siap2.BandParamProcessor#validate(java.lang.String[])}.
         */
        @Test
        public void testValidate()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidValue) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid BAND value " + invalidValue),
                    processor.validate(new String[] { invalidValue }));
        }
    }
    
    /**
     * Check the validateDouble method's handling of too many or too few values.
     */
    @RunWith(Parameterized.class)
    public static class InvalidBandAmountTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { "300" }, { "0.21" }, { "3.5e-1" }, { "3.5e1" }, { "3.5e+1" }, 
                { " 0.21" }, { "0.21 " }, { " 0.21 " }, { "" }, { "0.21 0.35 0.56" }, { "300\\600" }, { "300//" },
                { "300 600 1300" }, { "2.3-01" }, { "2." }, { "/600/" }});
       }

        private BandParamProcessor processor;

        private String invalidValue;

        public InvalidBandAmountTest(String invalidValue) throws Exception
        {
            this.invalidValue = invalidValue;
            processor = new BandParamProcessor();
        }

        /**
         * Test method for
         * {@link au.csiro.casda.access.soda.votools.siap2.BandParamProcessor#validate(java.lang.String[])}.
         */
        @Test
        public void testValidate()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidValue) + "' to be invalid.",
                    Arrays.asList("UsageFault: Your query contained an invalid band format. "
                            + "This query accepts exactly two band values"),
                    processor.validate(new String[] { invalidValue }));
        }
    }

    /**
     * Check the getFreqRanges method.
     */
    public static class GetFreqRangesTest
    {
        @Test
        public void testEmpty()
        {
            assertThat(BandParamProcessor.getFreqRanges(null), is(empty()));
            assertThat(BandParamProcessor.getFreqRanges(new String[0]), is(empty()));
        }

        @Test
        public void testSingle()
        {
            String[] bandParams = new String[] { "0.205 0.215" };
            List<ValueRange> bandRanges = BandParamProcessor.getFreqRanges(bandParams);
            assertThat(bandRanges, contains(new ValueRange(1.3943835255813954E9, 1.4624022341463416E9)));
        }

        @Test
        public void testOpenLow()
        {
            String[] bandParams = new String[] { "-Inf 0.215" };
            List<ValueRange> bandRanges = BandParamProcessor.getFreqRanges(bandParams);
            assertThat(bandRanges, contains(new ValueRange(1.3943835255813954E9, Double.MAX_VALUE)));
        }

        @Test
        public void testOpenHigh()
        {
            String[] bandParams = new String[] { "0.215 +Inf" };
            List<ValueRange> bandRanges = BandParamProcessor.getFreqRanges(bandParams);
            assertThat(bandRanges, contains(new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 1.3943835255813954E9)));
        }

        @Test
        public void testMultiple()
        {
            String[] bandParams = new String[] { "0.205 0.215",  "0.13"};
            List<ValueRange> velRanges = BandParamProcessor.getFreqRanges(bandParams);
            assertThat(velRanges, contains(new ValueRange(1.3943835255813954E9, 1.4624022341463416E9),
                    new ValueRange(2.306095830769231E9, 2.306095830769231E9)));
        }
    }
}
