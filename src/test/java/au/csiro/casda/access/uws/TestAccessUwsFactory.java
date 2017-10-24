package au.csiro.casda.access.uws;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.services.DataAccessService;
import uws.UWSException;
import uws.job.JobThread;
import uws.job.UWSJob;

/**
 * An AccessUwsFactory subclass to make testing with UWS easier. This class extends AccessUwsFactory to create a
 * subclass of DataAccessThread whose operation can be controlled and monitored. <add description here>
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class TestAccessUwsFactory extends AccessUwsFactory
{

    /**
     * A DataAccessThread subclass that wraps the {@link #jobWork()} method to control when the job actually runs. <add
     * description here>
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static final class NotifyingDataAccessThread extends DataAccessThread
    {
        private Semaphore startJob = new Semaphore(0);

        private Semaphore pause = new Semaphore(0);

        private Semaphore jobStarted = new Semaphore(0);

        private Semaphore jobEnded = new Semaphore(0);

        private NotifyingDataAccessThread(UWSJob uwsJob, DataAccessService dataAccessService, Packager packager,
                int hoursToExpiryDefault, int hoursToExpirySodaSync) throws UWSException
        {
            super(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySodaSync, "");
        }

        @Override
        protected void jobWork() throws UWSException, InterruptedException
        {
            startJob.acquire();
            jobStarted.release();
            pause.acquire();
            try
            {
                super.jobWork();
            }
            finally
            {
                jobEnded.release();
            }
        }

        /**
         * Once the job has been queued with UWS it will not start until this method is called.
         * 
         * @throws InterruptedException
         *             if the thread is interrupted.
         */
        public void startJobThread() throws InterruptedException
        {
            this.startAndPauseJobThread();
            this.pause.release();
        }

        /**
         * Once the job has been queued with UWS it will not start until this method is called. The difference between
         * this method and {@link #startJobThread()} is that this method will pause the job just before the normal
         * {@link #jobWork()} is performed.
         * 
         * @throws InterruptedException
         *             if the thread is interrupted.
         */
        public void startAndPauseJobThread() throws InterruptedException
        {
            this.startJob.release();
            this.jobStarted.acquire();
        }

        /**
         * Unpauses the job if it was started with {@link #startAndPauseJobThread()} (otherwise this will have no
         * effect).
         * 
         * @throws InterruptedException
         *             if the thread is interrupted.
         */
        public void unpauseJobThread() throws InterruptedException
        {
            this.pause.release();
        }

        /**
         * Blocks the calling thread until the job thread is finished.
         * 
         * @throws InterruptedException
         *             if the thread is interrupted.
         */
        public void waitForJobThread() throws InterruptedException
        {
            this.jobEnded.acquire();
            this.join();
        }
    }

    /**
     * Constructor
     * 
     * @param dataAccessService
     *            The service instance managing data access objects for the job.
     * @param packager
     *            The packager instance which will be doing the work for each job.
     * @param hoursToExpiryDefault
     *            the default number of hours until a job will expire
     * @param hoursToExpirySodaSync
     *            the number of hours to expiry for a SIAP sync job
     */
    public TestAccessUwsFactory(DataAccessService dataAccessService, Packager packager, int hoursToExpiryDefault,
            int hoursToExpirySodaSync)
    {
        super(dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySodaSync, "");
    }

    private ArrayList<TestAccessUwsFactory.NotifyingDataAccessThread> jobThreads = new ArrayList<>();

    @Override
    public JobThread createJobThread(UWSJob jobDescription) throws UWSException
    {
        TestAccessUwsFactory.NotifyingDataAccessThread myJobThread = new NotifyingDataAccessThread(jobDescription,
                this.dataAccessService, this.packager, this.hoursToExpiryDefault, this.hoursToExpirySodaSync);
        jobThreads.add(myJobThread);
        return myJobThread;
    }

    /**
     * @return the list of NotifyingDataAccessThread created by this factory
     */
    public List<TestAccessUwsFactory.NotifyingDataAccessThread> getJobThreads()
    {
        return jobThreads;
    }
}