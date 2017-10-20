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
 * Verify the workings of PolParamProcessor.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@RunWith(Enclosed.class)
public class PolParamProcessorTest
{
    /**
     * Check the validateState method's handling of valid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateStateValidTest
    {
        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "" }, { "Q" }, { " U " }, { "       " },
                    { new String[] { "I", "q" } }, { new String[] { "I", "", "q" } } });

        }

        private String[] validParamValues;

        private PolParamProcessor processor;

        public ValidateStateValidTest(Object validValue) throws Exception
        {
            processor = new PolParamProcessor();

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
         * Test method for {@link au.csiro.casda.access.soda.PolParamProcessor#validateState(java.lang.String[])}.
         */
        @Test
        public void testValidateState()
        {
            assertThat("Expected '" + ArrayUtils.toString(validParamValues) + "' to be valid.",
                    processor.validate(validParamValues), is(empty()));
        }
    }

    /**
     * Check the validateState method's handling of invalid values.
     */
    @RunWith(Parameterized.class)
    public static class ValidateStateInvalidTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays.asList(new Object[][] { { "/" }, { "/I/" }, { "//" }, { "UU" }, { "X X" }, { "POL Q" } });
        }

        private PolParamProcessor processor;

        private String invalidValue;

        public ValidateStateInvalidTest(String invalidValue) throws Exception
        {
            this.invalidValue = invalidValue;
            processor = new PolParamProcessor();
        }

        /**
         * Test method for {@link au.csiro.casda.access.soda.PolParamProcessor#validateState(java.lang.String[])}.
         */
        @Test
        public void testValidateState()
        {
            assertEquals("Expected '" + ArrayUtils.toString(invalidValue) + "' to be invalid.",
                    Arrays.asList("UsageFault: Invalid POL value " + invalidValue),
                    processor.validate(new String[] { invalidValue }));
        }
    }

    /**
     * Check the getPolRanges method.
     */
    public static class GetPolRangesTest
    {
        @Test
        public void testEmpty()
        {
            assertThat(PolParamProcessor.getPolRanges(null), is(empty()));
            assertThat(PolParamProcessor.getPolRanges(new String[0]), is(empty()));
        }

        @Test
        public void testSingle()
        {
            String[] polStrings = new String[] { "I" };
            List<ValueRange> polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(1, 1)));
        }

        @Test
        public void testSingleRange()
        {
            String[] polStrings = new String[] { "Q", "I" };
            List<ValueRange> polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(1, 2)));

            polStrings = new String[] { "Q", "U" };
            polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(2, 3)));

            polStrings = new String[] { "Q", "V", "U" };
            polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(2, 4)));
        }

        @Test
        public void testMultipleRange()
        {
            String[] polStrings = new String[] { "U", "I" };
            List<ValueRange> polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(1, 1), new ValueRange(3, 3)));

            polStrings = new String[] { "Q", "V", "I" };
            polRanges = PolParamProcessor.getPolRanges(polStrings);
            assertThat(polRanges, contains(new ValueRange(1, 2), new ValueRange(4, 4)));
        }
    }
}
