package au.csiro.casda.access.services;

/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import au.csiro.casda.access.InlineScriptException;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.jobmanager.SingleJobMonitor;

/**
 * Tests the pawsey hpc service.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class InlineScriptServiceTest
{
    @Mock
    DataAccessJobRepository dataAccessJobRepository;

    @Mock
    JobManager jobManager;

    InlineScriptService inlineScriptService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        JavaProcessJobFactory processJobFactory = new JavaProcessJobFactory();
        inlineScriptService = spy(new InlineScriptService(processJobFactory));

        dataAccessJobRepository.deleteAll();
    }

    @Test
    public void testCallScriptInline() throws Exception
    {
        String os = System.getProperty("os.name");
        if (os.toLowerCase().contains("win"))
        {
            String response = inlineScriptService.callScriptInline("src/test/resources/script/hello_world.bat");
            assertThat(response, containsString("Hello world"));
        }
        else
        {
            String response = inlineScriptService.callScriptInline("src/test/resources/script/hello_world.sh");
            assertThat(response, containsString("hello world"));
        }
    }

    @Test
    public void testCallScriptInlineCantCreate() throws Exception
    {
        String os = System.getProperty("os.name");

        thrown.expect(InlineScriptException.class);
        thrown.expectMessage("Error executing the command");
        thrown.expectMessage("java.io.IOException: Cannot run program");

        if (os.toLowerCase().contains("win"))
        {
            inlineScriptService.callScriptInline("src/test/resources/script/invalid.bat");
        }
        else
        {
            inlineScriptService.callScriptInline("src/test/resources/script/invalid.sh");
        }
    }

    @Test
    public void testCallScriptInlineJobFails() throws Exception
    {
        SingleJobMonitor mockJobMonitor = mock(SingleJobMonitor.class);
        doReturn(mockJobMonitor).when(inlineScriptService).createInlineJobMonitor(any());

        doReturn(true).when(mockJobMonitor).isJobCreated();
        doReturn(false).doReturn(true).when(mockJobMonitor).isJobCreated();

        doReturn(true).when(mockJobMonitor).isJobFailed();

        doReturn("This is the output").when(mockJobMonitor).getJobOutput();

        thrown.expect(InlineScriptException.class);
        thrown.expectMessage("Error executing the command");
        thrown.expectMessage("This is the output");

        inlineScriptService.callScriptInline("script");

    }
}
