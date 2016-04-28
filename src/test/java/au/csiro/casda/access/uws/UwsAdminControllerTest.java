package au.csiro.casda.access.uws;

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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.junit.internal.ThrowableMessageMatcher.hasMessage;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.hamcrest.junit.ExpectedException;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver;
import org.springframework.web.util.NestedServletException;

import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.uws.UwsAdminController.Queue;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.services.dto.Message.MessageCode;

/**
 * Tests the UWS Admin Controller
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class UwsAdminControllerTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private AccessJobManager accessJobManager;

    @Mock
    private DataAccessJobRepository dataAccessJobRepository;

    @InjectMocks
    private UwsAdminController controller;

    private MockMvc mockMvc;

    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobQueue() throws Exception
    {
        List<DataAccessJobDto> runningJobs = mock(List.class);
        List<DataAccessJobDto> availableJobs = mock(List.class);
        List<DataAccessJobDto> failedJobs = mock(List.class);

        when(accessJobManager.getRunningJobList()).thenReturn(runningJobs);
        when(accessJobManager.getRecentlyCompletedJobList()).thenReturn(availableJobs);
        when(accessJobManager.getRecentlyFailedJobList()).thenReturn(failedJobs);
        when(accessJobManager.getDisplayDaysOfFailedJobs()).thenReturn(2);
        when(accessJobManager.getDisplayDaysOfAvailableJobs()).thenReturn(4);
        when(accessJobManager.isQueuePaused(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)).thenReturn(true);
        when(accessJobManager.isQueuePaused(AccessJobManager.CATEGORY_B_JOB_LIST_NAME)).thenReturn(false);

        ModelAndView modelAndView =
                this.mockMvc.perform(get("/jobs")).andExpect(status().isOk()).andReturn().getModelAndView();
        assertEquals("jobQueue", modelAndView.getViewName());
        assertEquals(runningJobs, modelAndView.getModel().get("jobList"));
        assertEquals(availableJobs, modelAndView.getModel().get("available"));
        assertEquals(failedJobs, modelAndView.getModel().get("failed"));
        assertEquals(2, modelAndView.getModel().get("faileddays"));
        assertEquals(4, modelAndView.getModel().get("availabledays"));
        List<Queue> pausableQueues = (List<Queue>) modelAndView.getModel().get("pausableQueues");
        assertEquals(2, pausableQueues.size());
        assertEquals(AccessJobManager.CATEGORY_A_JOB_LIST_NAME, pausableQueues.get(0).getName());
        assertEquals(true, pausableQueues.get(0).isPaused());
        assertEquals(AccessJobManager.CATEGORY_B_JOB_LIST_NAME, pausableQueues.get(1).getName());
        assertEquals(false, pausableQueues.get(1).isPaused());
    }

    @Test
    public void testRetryJobSuccess() throws Exception
    {
        ResultActions result = this.mockMvc.perform(put("/requests/123-abc/retry").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString(" 123-abc submitted for retry")));
    }

    @Test
    public void testRetryJobFailure() throws Exception
    {
        doThrow(new AccessJobManager.ScheduleJobException("Gak!")).when(accessJobManager).retryJob("123-abc");

        exception.expect(NestedServletException.class);
        exception.expectCause(is(instanceOf(AccessJobManager.ScheduleJobException.class)));
        exception.expectCause(hasMessage(equalTo("Gak!")));

        this.mockMvc.perform(put("/requests/123-abc/retry").accept(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testPrioritiseJobSuccess() throws Exception
    {
        when(accessJobManager.prioritise("123-abc", 12)).thenReturn(true);

        ResultActions result =
                this.mockMvc.perform(put("/requests/123-abc/prioritise?position=12").accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString(" 123-abc priority set to 12")));
    }

    @Test
    public void testPrioritiseJobFailure() throws Exception
    {
        when(accessJobManager.prioritise("123-abc", 0)).thenReturn(false);

        ResultActions result =
                this.mockMvc.perform(put("/requests/123-abc/prioritise?position=0").accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isNotFound());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.FAILURE.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("No suitable job found")));
    }

    @Test
    public void testCancelJobSuccess() throws Exception
    {
        ResultActions result = this.mockMvc.perform(put("/requests/123-abc/cancel").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString(" 123-abc cancelled")));
    }

    @Test
    public void testCancelJobFailure() throws Exception
    {
        doThrow(new ResourceNotFoundException("Gak!")).when(accessJobManager).cancelJob(Mockito.eq("123-abc"),
                Mockito.any());

        exception.expect(NestedServletException.class);
        exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
        exception.expectCause(hasMessage(equalTo("Gak!")));

        this.mockMvc.perform(put("/requests/123-abc/cancel").accept(MediaType.APPLICATION_JSON));
    }

    @Test
    public void testPauseJobSuccess() throws Exception
    {
        ResultActions result = this.mockMvc.perform(put("/requests/123-abc/pause").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString(" 123-abc paused")));
    }

    @Test
    public void testPauseQueueSuccess() throws Exception
    {
        when(accessJobManager.pauseQueue("queuename")).thenReturn(true);

        ResultActions result = this.mockMvc.perform(put("/queues/queuename/pause").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("Queue queuename paused")));
    }

    @Test
    public void testPauseQueueFailure() throws Exception
    {
        when(accessJobManager.pauseQueue("queuename")).thenReturn(false);

        ResultActions result = this.mockMvc.perform(put("/queues/queuename/pause").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.FAILURE.toString()));
    }

    @Test
    public void testUnpauseQueueSuccess() throws Exception
    {
        when(accessJobManager.unpauseQueue("queuename")).thenReturn(true);

        ResultActions result = this.mockMvc.perform(put("/queues/queuename/resume").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("Queue queuename resumed")));
    }

    @Test
    public void testUnpauseQueueFailure() throws Exception
    {
        when(accessJobManager.unpauseQueue("queuename")).thenReturn(false);

        ResultActions result = this.mockMvc.perform(put("/queues/queuename/resume").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNotFound());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.FAILURE.toString()));
    }

    @Test
    public void testResumeJobSuccess() throws Exception
    {
        ResultActions result = this.mockMvc.perform(put("/requests/123-abc/resume").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString(" 123-abc resumed")));
    }

    @Test
    public void testResumeAllFailedSuccess() throws Exception
    {
        ResultActions result = this.mockMvc.perform(put("/requests/failed/resume").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("All failed data access jobs were resumed")));
    }

    @Test
    public void testResumeAllFailedFailure() throws Exception
    {
        when(dataAccessJobRepository.findFailedJobsAfterTime(Mockito.any(DateTime.class)))
                .thenReturn(Arrays.asList(mock(DataAccessJob.class), mock(DataAccessJob.class)));
        doThrow(RuntimeException.class).when(accessJobManager).retryJob(Mockito.any(String.class));

        ResultActions result = this.mockMvc.perform(put("/requests/failed/resume").accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isInternalServerError());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.FAILURE.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("Unable to resume")));
    }

    @Test
    public void testResumeAllPreparingSuccess() throws Exception
    {
        ResultActions result =
                this.mockMvc.perform(put("/requests/preparing/resume").accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isOk());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.SUCCESS.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("All preparing data access jobs were resumed")));
    }

    @Test
    public void testResumeAllPreparingFailure() throws Exception
    {
        when(dataAccessJobRepository.findPreparingJobs())
                .thenReturn(Arrays.asList(mock(DataAccessJob.class), mock(DataAccessJob.class)));
        doThrow(RuntimeException.class).when(accessJobManager).retryJob(Mockito.any(String.class));

        ResultActions result =
                this.mockMvc.perform(put("/requests/preparing/resume").accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isInternalServerError());

        result.andExpect(jsonPath("$").value(isA(Map.class)));
        result.andExpect(jsonPath("$.messageCode").value(MessageCode.FAILURE.toString()));
        result.andExpect(jsonPath("$.message").value(containsString("Unable to resume")));
    }

    @Test
    public void testPauseJobFailure() throws Exception
    {
        doThrow(new ResourceIllegalStateException("Gak!")).when(accessJobManager).pauseJob(Mockito.eq("123-abc"));

        exception.expect(NestedServletException.class);
        exception.expectCause(is(instanceOf(ResourceIllegalStateException.class)));
        exception.expectCause(hasMessage(equalTo("Gak!")));

        this.mockMvc.perform(put("/requests/123-abc/pause").accept(MediaType.APPLICATION_JSON));
    }

}
