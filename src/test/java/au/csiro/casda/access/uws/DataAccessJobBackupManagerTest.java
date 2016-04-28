package au.csiro.casda.access.uws;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.Charsets;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import uws.job.ExecutionPhase;
import uws.job.JobList;
import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;
import uws.service.UWS;
import uws.service.file.UWSFileManager;
import uws.service.log.UWSLog;
import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.services.DataAccessService;

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

/**
 * Tests the custom CASDA Data Access methods for backing up the jobs in the UWS queue
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class DataAccessJobBackupManagerTest
{

    @Mock
    private UWS uws;

    @Mock
    private JobList mockJobList;

    @Mock
    private UWSLog uwsLog;

    private DataAccessJobBackupManager backupManager;
    private PriorityQueueExecutionManager executionManager;
    private PriorityQueueExecutionManager executionManagerB;

    /**
     * Set up the service before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        backupManager = new DataAccessJobBackupManager(uws);
        executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));
        executionManagerB = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));
        JobList jobList = new JobList(AccessJobManager.CATEGORY_A_JOB_LIST_NAME, executionManager);
        jobList.setUWS(uws);
        JobList jobListB = new JobList(AccessJobManager.CATEGORY_B_JOB_LIST_NAME, executionManagerB);
        jobListB.setUWS(uws);
        
        doReturn(Arrays.asList(jobList, jobListB).iterator()).when(uws).iterator();
        doReturn(jobList).when(uws).getJobList(AccessJobManager.CATEGORY_A_JOB_LIST_NAME);
        doReturn(jobListB).when(uws).getJobList(AccessJobManager.CATEGORY_B_JOB_LIST_NAME);
        doReturn(uwsLog).when(uws).getLogger();
        doReturn(new AccessUwsFactory(mock(DataAccessService.class), mock(Packager.class), 12, 1)).when(uws)
                .getFactory();
    }

    @Test
    public void testRestorePauseJobCatA() throws Exception
    {
        JSONObject json = new JSONObject();
        json.put(UWSJob.PARAM_JOB_ID, PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX + 0);
        json.put("jobListName", AccessJobManager.CATEGORY_A_JOB_LIST_NAME);

        boolean response = backupManager.restoreJob(json, null);
        assertFalse(response);
        verify(executionManager).pauseQueue();
    }

    @Test
    public void testRestorePauseJobCatB() throws Exception
    {
        JSONObject json = new JSONObject();
        json.put(UWSJob.PARAM_JOB_ID, PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX + 0);
        json.put("jobListName", AccessJobManager.CATEGORY_B_JOB_LIST_NAME);

        boolean response = backupManager.restoreJob(json, null);
        assertFalse(response);
        verify(executionManagerB).pauseQueue();
    }
    
    @Test
    public void testRestoreRegularJob() throws Exception
    {
        String sampleJob =
                "{\"phase\":\"EXECUTING\"," + "\"jobId\":\"1f5415e4-2bb4-4c79-bf5c-d058303690d0\",\"quote\":-1,\""
                        + "startTime\":\"2015-08-27T16:01:26.356+1000\",\"executionDuration\":0,\"error\":{},"
                        + "\"parameters\":{\"SIZE_KB\":\"12\",\"USER_ID\":\"none|hel06j\","
                        + "\"REQUEST_ID\":\"1f5415e4-2bb4-4c79-bf5c-d058303690d0\",\"USER_NAME\":\"Amanda Helliwell\""
                        + ",\"REQUESTED_DATE\":\"2015-07-16T05:11:34.044Z\"},\"results\":[],\"jobListName\":\""
                        + AccessJobManager.CATEGORY_A_JOB_LIST_NAME + "\"}";

        JSONObject json = new JSONObject(sampleJob);

        boolean response = backupManager.restoreJob(json, new HashMap<>());
        assertTrue(response);
        verify(executionManager, never()).pauseQueue();

    }

    @Test
    public void testSaveAll() throws Exception
    {
        UWSFileManager mockFileManager = mock(UWSFileManager.class);
        when(uws.getFileManager()).thenReturn(mockFileManager);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        when(mockFileManager.getBackupOutput()).thenReturn(outputStream);

        UWSParameters paramsOne = new UWSParameters();
        paramsOne.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobOne = new CasdaUwsJob(paramsOne);

        UWSParameters paramsTwo = new UWSParameters();
        paramsTwo.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobTwo = new CasdaUwsJob(paramsTwo);
        List<UWSJob> orderedJobs = new ArrayList<UWSJob>();
        orderedJobs.add(jobTwo);

        when(executionManager.getOrderedJobList()).thenReturn(orderedJobs);

        uws.getJobList(AccessJobManager.CATEGORY_A_JOB_LIST_NAME).addNewJob(jobOne);

        backupManager.saveAll();

        String expected = "\",\"jobs\":[{\"phase\":\"PENDING\",\"jobId\":\"" + jobTwo.getJobId()
                + "\",\"quote\":-1,\"executionDuration\":0,\"error\":{},\"parameters\":{\"REQUEST_ID\":\""
                + jobTwo.getJobId() + "\"},\"results\":[],\"jobListName\":\""
                + AccessJobManager.CATEGORY_A_JOB_LIST_NAME + "\"}," + "{\"phase\":\"PENDING\",\"jobId\":\""
                + jobOne.getJobId() + "\",\"quote\":-1," + "\"executionDuration\":0,\"error\":{},\"parameters\":{"
                + "\"REQUEST_ID\":\"" + jobOne.getJobId() + "\"},\"results\":[],\"jobListName\":\""
                + AccessJobManager.CATEGORY_A_JOB_LIST_NAME + "\"}]}";
        assertThat(outputStream.toString(Charsets.UTF_8.name()),
                allOf(containsString(expected),
                        matchesPattern("\\{\"date\":\"[A-Z][a-z]{2} [A-Z][a-z]{2} [0-3][0-9] [0-2][0-9]:"
                                + "[0-5][0-9]:[0-5][0-9] .* [0-9]{4}\",\"jobs\":.*$")));

    }

    @Test
    public void testRestoreOtherJobParams() throws Exception
    {
        for (ExecutionPhase phase : ExecutionPhase.values())
        {
            String sampleJob = "{\"phase\":\"" + phase.name() + "\","
                    + "\"jobId\":\"1f5415e4-2bb4-4c79-bf5c-d058303690d0\",\"quote\":-1,\""
                    + "startTime\":\"2015-08-27T16:01:26.356+1000\",\"executionDuration\":0,\"error\":{},"
                    + "\"parameters\":{\"SIZE_KB\":\"12\",\"USER_ID\":\"none|hel06j\","
                    + "\"REQUEST_ID\":\"1f5415e4-2bb4-4c79-bf5c-d058303690d0\",\"USER_NAME\":\"Amanda Helliwell\""
                    + ",\"REQUESTED_DATE\":\"2015-07-16T05:11:34.044Z\"},\"results\":[],\"jobListName\":\"Slow\"}";

            JSONObject json = new JSONObject(sampleJob);
            UWSJob job = new UWSJob(new UWSParameters());
            backupManager.restoreOtherJobParams(json, job);
            if (Arrays.asList(ExecutionPhase.PENDING, ExecutionPhase.EXECUTING, ExecutionPhase.QUEUED).contains(phase))
            {
                assertEquals("PENDING", job.getPhase().name());
            }
            else
            {
                assertEquals(phase, job.getPhase());
            }
        }
    }
}
