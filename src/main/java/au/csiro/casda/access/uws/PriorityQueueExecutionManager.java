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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uws.UWSException;
import uws.job.UWSJob;
import uws.job.manager.QueuedExecutionManager;
import uws.job.parameters.UWSParameters;
import uws.service.log.UWSLog;

/**
 * Implements a priority queue to order jobs for UWS.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class PriorityQueueExecutionManager extends QueuedExecutionManager
{
    private static final Logger logger = LoggerFactory.getLogger(PriorityQueueExecutionManager.class);
    /** Job id prefix for the jobs used to pause the queue */
    protected static final String PAUSE_QUEUE_JOB_PREFIX = "PAUSE_QUEUE_JOB_";

    /**
     * Constructor
     * 
     * @param logger
     *            UWS logger
     * @param maxRunningJobs
     *            the maximum number of allowed running jobs
     */
    public PriorityQueueExecutionManager(UWSLog logger, int maxRunningJobs)
    {
        super(logger, maxRunningJobs);
    }

    /**
     * Whether the queue is currently locked.
     */
    private boolean locked = false;

    /**
     * Lock the queue
     * 
     * @return false if it is already locked, true if this method locked the queue.
     */
    private synchronized boolean lockQueue()
    {
        if (locked)
        {
            return false;
        }
        else
        {
            locked = true;
            return locked;
        }
    }

    /**
     * Unlock the queue
     */
    private synchronized void unlockQueue()
    {
        locked = false;
    }

    /**
     * Moves the UWS job matching the request id (in the DataAccessJob table) to the given index in the queue. This does
     * not include jobs that are already running.
     * 
     * @param requestId
     *            the request id in the DataAccessJob table
     * @param position
     *            the new index in the queue
     * @return true if the job could be put in that position, false if there is no matching job
     */
    public synchronized boolean sendToPosition(String requestId, int position)
    {
        logger.info("Moving {} to {} ", requestId, position);
        while (!lockQueue())
        {
            logger.debug("Waiting for lock to move job");
        }
        try
        {
            boolean result = false;
            Optional<UWSJob> job =
                    this.queuedJobs.stream()
                            .filter(queuedJob -> requestId.equals(queuedJob.getParameter(AccessJobManager.REQUEST_ID)))
                            .findFirst();
            if (job.isPresent())
            {
                result = sendToPosition(job.get(), position, true);
            }
            return result;
        }
        finally
        {
            this.unlockQueue();
        }
    }

    /**
     * Moves the UWS job to the given index in the queue. This does not include jobs that are already running.
     * 
     * @param job
     *            the UWS job to move
     * @param position
     *            the new index in the queue
     * @param hasLock
     *            if the method already has the lock on the queue (so it doesn't need to be locked)
     * @return true if the job could be put in that position, false if there is no matching job
     */
    protected synchronized boolean sendToPosition(UWSJob job, int position, boolean hasLock)
    {
        if (!hasLock)
        {
            while (!lockQueue())
            {
                logger.debug("Waiting for lock to move job");
            }
        }
        try
        {
            // as long as it isn't a running job, you can reposition (or add) the uws job
            boolean result = false;
            if (!this.runningJobs.containsKey(job.getJobId()))
            {
                logger.info("Current index: " + this.queuedJobs.indexOf(job));
                this.queuedJobs.remove(job);
                if (position > this.queuedJobs.size())
                {
                    position = this.queuedJobs.size();
                }
                this.queuedJobs.add(position, job);
                logger.info("Index of job {} is {}", job.getParameter(AccessJobManager.REQUEST_ID),
                        this.queuedJobs.indexOf(job));
                result = true;
            }
            else
            {
                logger.info("Can't reprioritise {} ({})", job.getParameter(AccessJobManager.REQUEST_ID), job.getJobId());
            }
            return result;
        }
        finally
        {
            if (!hasLock)
            {
                this.unlockQueue();
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasQueue()
    {
        if (!lockQueue())
        {
            return false;
        }
        try
        {
            boolean result = super.hasQueue();
            return result;
        }
        finally
        {
            unlockQueue();
        }

    }

    /**
     * Returns an ordered list of jobs being run by UWS (with running jobs first order by start time, and then queued
     * jobs in the order they are in the queue).
     * 
     * @return the ordered list of jobs
     */
    public List<UWSJob> getOrderedJobList()
    {
        List<UWSJob> jobs = new ArrayList<>();
        while (!lockQueue())
        {
            logger.debug("Waiting for lock to get job list");
        }
        try
        {
            CollectionUtils.addAll(jobs, this.getRunningJobs());
            // order the running jobs by start time
            Collections.sort(jobs, new Comparator<UWSJob>()
            {
                @Override
                public int compare(UWSJob a, UWSJob b)
                {
                    if (a.getStartTime() == null && b.getStartTime() == null)
                    {
                        return 0;
                    }
                    else if (a.getStartTime() == null || b.getStartTime() == null)
                    {
                        return a.getStartTime() == null ? -1 : 1;
                    }
                    return a.getStartTime().compareTo(b.getStartTime());
                }

            });
            // this adds all the queued jobs in the order that they will be run
            CollectionUtils.addAll(jobs, this.getQueuedJobs());
            return jobs;
        }
        finally
        {
            unlockQueue();
        }

    }

    /**
     * WARNING: this adds a running job to the list of running jobs but doesn't start it running. This is used for the
     * pause functionality and for testing purposes.
     * 
     * @param job
     *            the uws job to add to the list of running jobs
     */
    protected void addRunningJob(UWSJob job)
    {
        this.runningJobs.put(job.getJobId(), job);
    }
    
    /**
     * WARNING: this adds a job to the list of queued. This is used for the for testing purposes.
     * 
     * @param job
     *            the uws job to add to the list of running jobs
     */
    protected void addQueuedJob(UWSJob job)
    {
        this.queuedJobs.add(job);
    }

    /**
     * WARNING: this removes a job to from list of running jobs but doesn't stop it running. This is used for the
     * pause functionality.
     * 
     * @param job
     *            the uws job to remove from the list of running jobs
     */
    private void removeRunningJob(UWSJob job)
    {
        logger.debug("Removing running job {}", job.getJobId());
        this.runningJobs.remove(job.getJobId());
    }

    /**
     * Checks whether the queue is paused - ie does it have any jobs in the list of running jobs that start with the
     * pause_queue_job_prefix
     * 
     * @return true if the queue is paused and will not begin executing any new jobs
     */
    public boolean isQueuePaused()
    {
        for (Iterator<UWSJob> runningJobs = this.getRunningJobs(); runningJobs.hasNext();)
        {
            UWSJob job = runningJobs.next();
            if (job.getJobId().startsWith(PAUSE_QUEUE_JOB_PREFIX))
            {
                logger.info("Paused job " + job.getJobId());
                return true;
            }
        }
        return false;
    }

    /**
     * Pause the queue by filling the list of running jobs with pause jobs. The currently executing jobs will be allowed
     * to complete but no more jobs will be picked up until the paused jobs are removed.
     * 
     * @throws UWSException
     *             if there is a problem pausing the queue
     */
    public void pauseQueue() throws UWSException
    {
        if (!this.isQueuePaused())
        {
            for (int i = 0; i < this.getMaxRunningJobs(); i++)
            {
                UWSParameters uwsParameters = new UWSParameters();
                uwsParameters.set(AccessJobManager.REQUEST_ID, PAUSE_QUEUE_JOB_PREFIX + i);
                // Set status as ready to start.
                uwsParameters.set(UWSJob.PARAM_PHASE, UWSJob.PHASE_RUN);
                CasdaUwsJob pauseQueueJob = new CasdaUwsJob(uwsParameters);
                logger.debug("Adding pause queue job {}", pauseQueueJob.getJobId());
                this.addRunningJob(pauseQueueJob);
            }
        }
    }

    /**
     * Unpause the queue by removing the pause jobs from the list of running jobs.
     * 
     * @throws UWSException
     *             if there is a problem unpausing the queue
     */
    public void unpauseQueue() throws UWSException
    {
        logger.debug("Number of running jobs {} with keys {}", this.getNbRunningJobs(), this.runningJobs.keySet());
        List<UWSJob> pausedJobs = new ArrayList<>();
        for (Iterator<UWSJob> runningJobs = this.getRunningJobs(); runningJobs.hasNext();)
        {
            UWSJob job = runningJobs.next();
            if (job.getJobId().startsWith(PAUSE_QUEUE_JOB_PREFIX))
            {
                pausedJobs.add(job);
            }
        }
        for (UWSJob pausedJob : pausedJobs)
        {
            removeRunningJob(pausedJob);
        }

        // this will initiate the queue to pick up a new job to run
        this.refresh();
    }
}
