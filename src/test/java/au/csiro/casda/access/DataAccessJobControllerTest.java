package au.csiro.casda.access;

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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Arrays;

import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.fasterxml.jackson.databind.ObjectMapper;

import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;

/**
 * Tests for the DataAccessJob Controller
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class DataAccessJobControllerTest
{

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private AccessJobManager accessJobManager;

    @InjectMocks
    private DataAccessJobController controller;

    private Log4JTestAppender testAppender;

    private MockMvc mockMvc;

    /**
     * Set up the controller before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        testAppender = Log4JTestAppender.createAppender();
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    /**
     * Basic test to create new job
     * 
     * @throws Exception
     *             from performing post request
     */
    @Test
    public void testJobCreationOK() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("generated_UUID");
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(accessJobManager.createDataAccessJob(any())).thenReturn(job);
        this.mockMvc
                .perform(post("/requests").param("recordType", "image_cube").param("ids", "1").param("ids", "5")
                        .param("userName", "bob").param("downloadMode", "WEB")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isCreated())
                .andExpect(header().string("location", "http://localhost/requests/" + job.getRequestId()))
                .andExpect(content().string(new ObjectMapper().writeValueAsString(Arrays.asList(job.getRequestId()))));
        verify(accessJobManager).createDataAccessJob(any());

        testAppender.verifyLogMessage(Level.INFO, "Hit the controller ");
    }

    @Test
    public void testJobCreationButQueueFailsStillReturnsOk() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("generated_UUID");
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(accessJobManager.createDataAccessJob(any())).thenReturn(job);
        doThrow(new AccessJobManager.ScheduleJobException("some error")).when(accessJobManager).scheduleJob(any());
        this.mockMvc
                .perform(post("/requests").param("recordType", "image_cube").param("ids", "1").param("ids", "5")
                        .param("userName", "bob").param("downloadMode", "WEB")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isCreated())
                .andExpect(header().string("location", "http://localhost/requests/" + job.getRequestId()))
                .andExpect(content().string(new ObjectMapper().writeValueAsString(Arrays.asList(job.getRequestId()))));
        verify(accessJobManager).createDataAccessJob(any());

        testAppender.verifyLogMessage(Level.INFO, "Hit the controller ");
    }

    /**
     * Basic test to reject new job request - missing mandatory parameter username
     * 
     * @throws Exception
     *             from performing post request
     */
    @Test
    public void testJobCreationInvalid() throws Exception
    {
        // test at least one is required
        this.mockMvc.perform(post("/requests").param("recordType", "image_cube").param("userName", "bob")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)).andExpect(status().isBadRequest());

        // test invalid record type is rejected
        this.mockMvc
                .perform(post("/requests").param("recordType", "invalidType").param("userName", "bob").param("ids", "1")
                        .param("ids", "5").contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());

        // test missing record type is rejected
        this.mockMvc
                .perform(post("/requests").param("recordType", "").param("userName", "bob").param("ids", "1")
                        .param("ids", "5").contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());
        this.mockMvc.perform(post("/requests").param("userName", "bob").param("ids", "1").param("ids", "5")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)).andExpect(status().isBadRequest());

        verify(accessJobManager, never()).createDataAccessJob(any());

    }

}
