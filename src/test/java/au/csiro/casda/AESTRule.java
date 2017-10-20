package au.csiro.casda;

import java.util.TimeZone;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

/**
 * Rule to set the timezone for the tests to Canberra, Australia time as some test data is time zone dependent.
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
public class AESTRule extends TestWatcher
{

    private TimeZone origDefault = TimeZone.getDefault();

    @Override
    protected void starting(Description description)
    {
        TimeZone.setDefault(TimeZone.getTimeZone("Australia/Canberra"));
    }

    @Override
    protected void finished(Description description)
    {
        TimeZone.setDefault(origDefault);
    }
}