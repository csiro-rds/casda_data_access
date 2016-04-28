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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;
import uws.job.user.DefaultJobOwner;
import uws.service.log.UWSLog;

/**
 * Tests the Priority Queue execution manager for UWS
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class PriorityQueueExecutionManagerTest
{

    private static final Logger logger = LoggerFactory.getLogger(PriorityQueueExecutionManagerTest.class);

    @Test
    public void testPrioritise() throws Exception
    {
        PriorityQueueExecutionManager executionManager = new PriorityQueueExecutionManager(mock(UWSLog.class), 1);
        UWSParameters paramsOne = new UWSParameters();
        paramsOne.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobOne = new CasdaUwsJob(paramsOne);
        UWSParameters paramsTwo = new UWSParameters();
        paramsTwo.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobTwo = new CasdaUwsJob(paramsTwo);
        UWSParameters paramsThree = new UWSParameters();
        paramsThree.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobThree = new CasdaUwsJob(paramsThree);

        logger.info("{} {} {}", jobOne.getParameter(AccessJobManager.REQUEST_ID),
                jobTwo.getParameter(AccessJobManager.REQUEST_ID), jobThree.getParameter(AccessJobManager.REQUEST_ID));
        logger.info("{} {} {}", jobOne.getJobId(), jobTwo.getJobId(), jobThree.getJobId());

        executionManager.sendToPosition(jobOne, 0, false);
        assertEquals(1, executionManager.getNbQueuedJobs());
        assertEquals(jobOne, executionManager.getQueuedJobs().next());

        executionManager.sendToPosition(jobOne, 0, false);
        assertEquals(1, executionManager.getNbQueuedJobs());
        assertEquals(jobOne, executionManager.getQueuedJobs().next());

        executionManager.sendToPosition(jobTwo, 0, false);
        assertEquals(2, executionManager.getNbQueuedJobs());
        Iterator<UWSJob> jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobTwo, jobsIt.next());
        assertEquals(jobOne, jobsIt.next());

        assertTrue(executionManager.sendToPosition(jobThree, 10, false));
        assertEquals(3, executionManager.getNbQueuedJobs());
        jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobTwo, jobsIt.next());
        assertEquals(jobOne, jobsIt.next());
        assertEquals(jobThree, jobsIt.next());

        executionManager.sendToPosition(jobThree, 0, false);
        assertEquals(3, executionManager.getNbQueuedJobs());
        jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobThree, jobsIt.next());
        assertEquals(jobTwo, jobsIt.next());
        assertEquals(jobOne, jobsIt.next());

        executionManager.sendToPosition(jobTwo, 2, false);
        assertEquals(3, executionManager.getNbQueuedJobs());
        jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobThree, jobsIt.next());
        assertEquals(jobOne, jobsIt.next());
        assertEquals(jobTwo, jobsIt.next());
    }

    @Test
    public void testSendToPosition() throws Exception
    {
        PriorityQueueExecutionManager executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));

        UWSParameters paramsOne = new UWSParameters();
        paramsOne.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobOne = new CasdaUwsJob(paramsOne);
        UWSParameters paramsTwo = new UWSParameters();
        paramsTwo.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobTwo = new CasdaUwsJob(paramsTwo);
        UWSParameters paramsThree = new UWSParameters();
        paramsThree.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobThree = new CasdaUwsJob(paramsThree);

        executionManager.sendToPosition(jobOne, 0, false);
        executionManager.sendToPosition(jobTwo, 1, false);
        executionManager.sendToPosition(jobThree, 2, false);

        assertEquals(3, executionManager.getNbQueuedJobs());
        Iterator<UWSJob> jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobOne, jobsIt.next());
        assertEquals(jobTwo, jobsIt.next());
        assertEquals(jobThree, jobsIt.next());

        executionManager.sendToPosition(jobOne.getJobId(), 12);
        verify(executionManager).sendToPosition(jobOne, 12, true);
        assertEquals(3, executionManager.getNbQueuedJobs());
        jobsIt = executionManager.getQueuedJobs();
        assertEquals(jobTwo, jobsIt.next());
        assertEquals(jobThree, jobsIt.next());
        assertEquals(jobOne, jobsIt.next());
    }

    @Test
    public void testGetOrderedJobList() throws Exception
    {
        DateTime now = DateTime.now();

        UWSJob jobOne =
                new UWSJob("one", new DefaultJobOwner("name"), new UWSParameters(), 1, now.getMillis(), -1, null, null);
        UWSJob jobTwo =
                new UWSJob("two", new DefaultJobOwner("name"), new UWSParameters(), 1, now.minus(1000).getMillis(), -1,
                        null, null);
        UWSJob jobThree =
                new UWSJob("three", new DefaultJobOwner("name"), new UWSParameters(), 1, now.minus(500).getMillis(),
                        -1, null, null);
        UWSJob jobFour = new UWSJob("four", new DefaultJobOwner("name"), new UWSParameters(), 1, -1, -1, null, null);
        UWSJob jobFive = new UWSJob("five", new DefaultJobOwner("name"), new UWSParameters(), 1, -1, -1, null, null);
        UWSJob jobSix = new UWSJob("six", new DefaultJobOwner("name"), new UWSParameters(), 1, -1, -1, null, null);

        PriorityQueueExecutionManager executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));
        executionManager.addRunningJob(jobOne);
        executionManager.addRunningJob(jobTwo);
        executionManager.addRunningJob(jobThree);

        executionManager.sendToPosition(jobFour, 0, false);
        executionManager.sendToPosition(jobFive, 1, false);
        executionManager.sendToPosition(jobSix, 2, false);

        List<UWSJob> orderedJobs = executionManager.getOrderedJobList();
        assertEquals(6, orderedJobs.size());
        assertEquals("two", orderedJobs.get(0).getJobId());
        assertEquals("three", orderedJobs.get(1).getJobId());
        assertEquals("one", orderedJobs.get(2).getJobId());
        assertEquals("four", orderedJobs.get(3).getJobId());
        assertEquals("five", orderedJobs.get(4).getJobId());
        assertEquals("six", orderedJobs.get(5).getJobId());
    }

    @Test
    public void testIsQueuePaused() throws Exception
    {
        PriorityQueueExecutionManager executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 3));
        assertFalse(executionManager.isQueuePaused());
        executionManager.pauseQueue();
        assertTrue(executionManager.isQueuePaused());
        executionManager.unpauseQueue();
        assertFalse(executionManager.isQueuePaused());
    }

    @Test
    public void testPauseQueue() throws Exception
    {
        PriorityQueueExecutionManager executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));
        assertEquals(0, executionManager.getNbRunningJobs());

        UWSParameters paramsOne = new UWSParameters();
        paramsOne.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobOne = new CasdaUwsJob(paramsOne);
        executionManager.addRunningJob(jobOne);
        assertEquals(1, executionManager.getNbRunningJobs());

        executionManager.pauseQueue();
        assertEquals(2, executionManager.getNbRunningJobs());
        Iterator<UWSJob> jobs = executionManager.getRunningJobs();
        assertEquals(jobOne.getJobId(), jobs.next().getJobId());
        assertEquals(PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX + 0, jobs.next().getJobId());

        executionManager.pauseQueue();
        assertEquals(2, executionManager.getNbRunningJobs());
        jobs = executionManager.getRunningJobs();
        assertEquals(jobOne.getJobId(), jobs.next().getJobId());
        assertEquals(PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX + 0, jobs.next().getJobId());
    }

    @Test
    public void testUnpauseQueue() throws Exception
    {
        PriorityQueueExecutionManager executionManager = spy(new PriorityQueueExecutionManager(mock(UWSLog.class), 1));
        assertEquals(0, executionManager.getNbRunningJobs());

        UWSParameters paramsOne = new UWSParameters();
        paramsOne.set(AccessJobManager.REQUEST_ID, UUID.randomUUID().toString());
        UWSJob jobOne = new CasdaUwsJob(paramsOne);
        executionManager.addRunningJob(jobOne);
        assertEquals(1, executionManager.getNbRunningJobs());

        executionManager.pauseQueue();
        assertEquals(2, executionManager.getNbRunningJobs());
        Iterator<UWSJob> jobs = executionManager.getRunningJobs();
        assertEquals(jobOne.getJobId(), jobs.next().getJobId());
        assertEquals(PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX + 0, jobs.next().getJobId());

        executionManager.unpauseQueue();
        assertEquals(1, executionManager.getNbRunningJobs());
        assertEquals(jobOne.getJobId(), executionManager.getRunningJobs().next().getJobId());
    }
}
