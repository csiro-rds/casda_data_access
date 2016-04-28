package au.csiro.casda.access.uws;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.transaction.Transactional;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.JobDto;
import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.SizeLimitReachedException;
import au.csiro.casda.access.cache.CacheManagerInterface;
import au.csiro.casda.access.jpa.CatalogueRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.siap2.CutoutBounds;
import au.csiro.casda.access.siap2.CutoutService;
import au.csiro.casda.access.soda.ImageCubeAxis;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.jobmanager.JobManager;
import uws.UWSException;
import uws.job.ErrorSummary;
import uws.job.ErrorType;
import uws.job.ExecutionPhase;
import uws.job.JobList;
import uws.job.Result;
import uws.job.UWSJob;
import uws.job.manager.QueuedExecutionManager;
import uws.job.parameters.UWSParameters;
import uws.service.UWSFactory;
import uws.service.UWSService;
import uws.service.UWSUrl;
import uws.service.file.UWSFileManager;

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
 * Provides facilities to create and manage a job to retrieve a set of data products from the archive. The data products
 * are specified in a {@link JobDto}. A job is created by calling the
 * {@link #createDataAccessJob(JobDto, Long, boolean)} method and the status of the job is obtained using the
 * {@link #getJobStatus(DataAccessJob)} method.
 * <p>
 * That method returns the job's status in the form of a UWSJob object. The UWSJob represents the concept of a job as
 * defined in the Universal Work Service specification: http://www.ivoa.net/documents/UWS/20101010/index.html Jobs have
 * the following states and transitions:
 * <ul>
 * <li>PENDING - the job creation/initial state (set by {@link #createDataAccessJob(JobDto)}</li>
 * <li>QUEUED - the job has been submitted for processing but is not yet being processed. Jobs can transition to this
 * state from:
 * <ul>
 * <li>PENDING - using {@link #scheduleJob(DataAccessJob)}</li>
 * <li>ERROR - using {@link #retryJob(String)}</li>
 * <li>ABORTED - using {@link #retryJob(String)}</li>
 * <li>COMPLETED - using {@link #retryJob(String)}</li>
 * <li>HELD - using {@link #retryJob(String)}</li>
 * </ul>
 * </li>
 * <li>EXECUTING - the job execution state. Jobs are automatically transitioned to this state from PENDING as processing
 * resources become available (see below).</li>
 * <li>COMPLETED - jobs automatically transition to this state when they successfully complete.</li>
 * <li>ERROR - jobs automatically transition to this state when they fail whilst EXECUTING.</li>
 * <li>ABORTED - jobs that have not finished (COMPLETED or ERROR) can be aborted using
 * {@link #cancelJob(String, DateTime)}.</li>
 * <li>HELD - jobs that have not finished (COMPLETED or ERROR) or been ABORTED can be paused using
 * {@link #pauseJob(String, DateTime)}.</li>
 * </ul>
 * The UWS spec also has a SUSPENDED and UNKNOWN state but these states are not used by this system.
 * <p>
 * When jobs are submitted to be run then they will be run as processing resources allow. Each job is placed on one of
 * three queues depending on whether the data products are immediately available:
 * <ul>
 * <li>Immediate - if all data products in the job are immediately available then the job will be placed on this queue.
 * All jobs on the Immediate queue will run concurrently.</li>
 * <li>Category A - if any of the data products in the job are not immediately available and if the total size of all
 * the data products is not greater than the Category A maximum (as specifed by categoryAJobMaxSize in the constructor)
 * then the job will be placed on this queue. Jobs on this queue run sequentially in a FIFO order.</li>
 * <li>Category B - as per Category A but for jobs where the total size of all data products is greater than the
 * Category A maximum.</li>
 * </ul>
 * <p>
 * COMPLETED jobs have a series of 'results' available in the UWSJob object. These results provide URLs for clients to
 * use to access the requested data produts. FAILED jobs contain an 'errorSummary' in the UWSJob object. All
 * non-PENDING/non-QUEUED/non-EXECUTING jobs have an expiry time. This can be used to control the visibility of old jobs
 * by end-users.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Service
@Scope("singleton")
public class AccessJobManager
{
    /*
     * Implementation Notes:
     * 
     * This class does indeed use UWS under the hood to manage and run jobs. The getJobStatus method does *NOT* return
     * this underlying job object. In UWS there is the possibility to 'delete' a job, however there is no underlying UWS
     * phase for such a state. Similarly, our DataAccessJobs only have a CANCELLED state which corresponds to the
     * ABORTED state in UWS. Deleted jobs are therefore just CANCELLED/ABORTED jobs with a destruction time set to some
     * time in the past.
     */

    /**
     * An exception used to indicate that a job could not be scheduled.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class ScheduleJobException extends Exception
    {
        private static final long serialVersionUID = 1L;

        /**
         * Constructor
         * 
         * @param message
         *            the reason for the exception
         */
        public ScheduleJobException(String message)
        {
            super(message);
        }
    }

    /**
     * Set of ExectionPhase enumerables that represent a 'finished' UWSJob
     */
    public static final EnumSet<ExecutionPhase> UWS_JOB_FINISHED_EXECUTION_PHASES =
            EnumSet.of(ExecutionPhase.COMPLETED, ExecutionPhase.ABORTED, ExecutionPhase.ERROR);

    /** Param map key for the request id. */
    public static final String REQUEST_ID = "REQUEST_ID";

    /**
     * Name of the list of category A (size <= category.a.job.max.size.kb, files not in cache when job was created) data
     * access jobs in UWS.
     */
    public static final String CATEGORY_A_JOB_LIST_NAME = "Category A";

    /**
     * Name of the list of category A (size > category.a.job.max.size.kb, files not in cache when job was created) data
     * access jobs in UWS.
     */
    public static final String CATEGORY_B_JOB_LIST_NAME = "Category B";

    /**
     * Name of the list of data access jobs that get run immediately in UWS because files are already in cache or on
     * disk.
     */
    public static final String IMMEDIATE_JOB_LIST_NAME = "Immediate";

    private static final String XLINK_SIMPLE_TYPE = "simple";

    private static final Logger logger = LoggerFactory.getLogger(AccessJobManager.class);

    private final String baseUrl;

    private final long categoryAJobMaxSize;

    private final int categoryAMaxRunningJobs;

    private final int categoryBMaxRunningJobs;

    private JobList categoryAJobList;

    private JobList categoryBJobList;

    private JobList immediateJobList;

    private final DataAccessJobRepository dataAccessJobRepository;

    private final ImageCubeRepository imageCubeRepository;

    private final CatalogueRepository catalogueRepository;

    private final MeasurementSetRepository measurementSetRepository;

    private final CacheManagerInterface cacheManager;

    private final int displayDaysOfAvailableJobs;

    private final int displayDaysOfFailedJobs;

    private String fileDownloadBaseUrl;

    private UWSFileManager uwsFileManager;

    private UWSFactory uwsFactory;

    private UWSService uws;

    private CutoutService cutoutService;

    private final EntityManagerFactory emf;
    
    private JobManager slurmJobManager;
     

    /**
     * Constructor
     * 
     * @param emf
     *            an EntityManagerFactory
     * @param dataAccessJobRepository
     *            the data access job repository
     * @param imageCubeRepository
     *            an ImageCubeRepository used to access ImageCube data products
     * @param catalogueRepository
     *            a CatalogueCubeRepository used to access Catalogue data products
     * @param measurementSetRepository
     *            a MeasurementSetRepository used to access MeasurementSet (visibility) data products
     * @param cacheManager
     *            the cache manager
     * @param uwsFactory
     *            a UWSFactory that will be used to create an appropriate JobThread to process a DataAccessJob
     * @param uwsFileManager
     *            a UWSFileManager that will be used to manage the persistence of of the job queues
     * @param cutoutService
     *            The service for calculating cutout bounds.
     * @param slurmJobManager
     *            The Slurm job manager.
     * @param baseUrl
     *            the uws base url
     * @param categoryAMaxRunningJobs
     *            the maximum number of category A jobs allowed to run concurrently
     * @param categoryBMaxRunningJobs
     *            the maximum number of category B jobs allowed to run concurrently
     * @param displayDaysOfAvailableJobs
     *            the number of days worth of available jobs to retrieve for display
     * @param displayDaysOfFailedJobs
     *            the number of days worth of failed jobs to retrieve for display
     * @param categoryAJobMaxSize
     *            the maximum size of a category A job, in KB
     * @param fileDownloadBaseUrl
     *            the base URL used to obtain result files
     */
    @Autowired
    public AccessJobManager(EntityManagerFactory emf, DataAccessJobRepository dataAccessJobRepository,
            ImageCubeRepository imageCubeRepository, CatalogueRepository catalogueRepository,
            MeasurementSetRepository measurementSetRepository, CacheManagerInterface cacheManager,
            UWSFactory uwsFactory, UWSFileManager uwsFileManager, CutoutService cutoutService,
            JobManager slurmJobManager,
            @Value("${uws.baseurl}") String baseUrl,
            @Value("${uws.category.a.maxrunningjobs}") int categoryAMaxRunningJobs,
            @Value("${uws.category.b.maxrunningjobs}") int categoryBMaxRunningJobs,
            @Value("${admin.ui.availablejobs.days}") int displayDaysOfAvailableJobs,
            @Value("${admin.ui.failedjobs.days}") int displayDaysOfFailedJobs,
            @Value("${category.a.job.max.size.kb}") long categoryAJobMaxSize,
            @Value("${application.base.url}") String fileDownloadBaseUrl)
    {
        this.emf = emf;
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.imageCubeRepository = imageCubeRepository;
        this.catalogueRepository = catalogueRepository;
        this.measurementSetRepository = measurementSetRepository;
        this.cacheManager = cacheManager;
        this.uwsFactory = uwsFactory;
        this.uwsFileManager = uwsFileManager;
        this.cutoutService = cutoutService;
        this.baseUrl = baseUrl;
        this.categoryAMaxRunningJobs = categoryAMaxRunningJobs;
        this.categoryBMaxRunningJobs = categoryBMaxRunningJobs;
        this.displayDaysOfAvailableJobs = displayDaysOfAvailableJobs;
        this.displayDaysOfFailedJobs = displayDaysOfFailedJobs;
        this.categoryAJobMaxSize = categoryAJobMaxSize;
        this.fileDownloadBaseUrl = fileDownloadBaseUrl;
        this.slurmJobManager = slurmJobManager;
    }

    /**
     * Internal helper method used to get the appropriate JobList for the given DataAccessJob. Protected for testing
     * purposes only.
     * 
     * @param dataAccessJob
     *            a DataAccessJob
     * @return a JobList
     */
    JobList getJobList(DataAccessJob dataAccessJob)
    {
        UWSJob uwsJob = getJob(dataAccessJob.getRequestId());
        if (uwsJob != null)
        {
            return uwsJob.getJobList();
        }
        // if all the files are already available in the cache, run the job immediately
        boolean runImmediately = cacheManager.allFilesAvailableInCache(DataAccessUtil
                .getDataAccessJobDownloadFiles(dataAccessJob, cacheManager.getJobDirectory(dataAccessJob)));

        if (runImmediately)
        {
            // this job list doesn't have a queue, so will start jobs immediately
            return immediateJobList;
        }
        else if (dataAccessJob.getSizeKb() <= categoryAJobMaxSize)
        {
            // this job list is queued, for category A jobs
            return categoryAJobList;
        }
        else
        {
            // this job list is queued, for category B jobs
            return categoryBJobList;
        }
    }

    /**
     * Create and store a new DataAccessJob for the access request. Will associate the data access job record with the
     * requested entities (image cube, measurement set, catalogues) and create the cutouts as required.
     * 
     * @param job
     *            attributes for new job
     * @return created dataAccessJob
     */
    public DataAccessJob createDataAccessJob(JobDto job)
    {
        return createDataAccessJob(job, null, true);
    }

    /**
     * Create and store a new DataAccessJob for the access request, as long as it is less than the size limit.
     * 
     * @param job
     *            the data access job details
     * @param sizeLimit
     *            ignored if null, otherwise will only create a job if it is less than or equal to the size limit
     * @param createDataProducts
     *            if true, will associate the data access job record with the requested entities (image cube,
     *            measurement set, catalogues) and create the cutouts as required. If false, this will save the
     *            requested ids and filters in the param map.
     * @return created data access job
     * @throws SizeLimitReachedException
     *             if the size limit is not null, and the job size is greater than the size limit
     */
    public DataAccessJob createDataAccessJob(JobDto job, Long sizeLimit, boolean createDataProducts)
            throws SizeLimitReachedException
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setUserIdent(job.getUserIdent());
        dataAccessJob.setUserLoginSystem(job.getUserLoginSystem());
        dataAccessJob.setUserName(job.getUserName());
        dataAccessJob.setUserEmail(job.getUserEmail());
        ParamMap dataAccessJobParams = dataAccessJob.getParamMap();
        dataAccessJobParams.addAll(job.getParams());
        if (createDataProducts)
        {
            dataAccessJob = createDataAccessProducts(job.getIds(), dataAccessJob, sizeLimit);
        }
        dataAccessJob.setDownloadFormat(job.getDownloadFormat());
        dataAccessJob.setDownloadMode(job.getDownloadMode());

        DateTime now = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC);
        dataAccessJob.setCreatedTimestamp(now);
        dataAccessJob.setLastModified(now);
        dataAccessJob.setRequestId(UUID.randomUUID().toString());
        dataAccessJob.setStatus(DataAccessJobStatus.PREPARING);
        dataAccessJob = dataAccessJobRepository.save(dataAccessJob);

        logger.info("{}", CasdaDataAccessEvents.E037.messageBuilder().add(dataAccessJob.getDownloadMode())
                .add(dataAccessJob.getRequestId()));

        return dataAccessJob;
    }

    /**
     * Will associate the data access job record with the requested entities (image cube, measurement set, catalogues)
     * and create the cutouts as required. Also updates the job size. NOTE: This doesn't save the job.
     * 
     * @param ids
     *            the data product ids (not encoded)
     * @param dataAccessJob
     *            the data access job
     * @param sizeLimit
     *            ignored if null, otherwise will only create a job if it is less than or equal to the size limit
     * @return updated data access job
     * @throws SizeLimitReachedException
     *             if the size limit is not null, and the job size is greater than the size limit
     */
    private DataAccessJob createDataAccessProducts(String[] ids, DataAccessJob dataAccessJob, Long sizeLimit)
    {
        long fileSizeKb = 0l;
        if (ids != null)
        {
            for (String id : ids)
            {
                DataAccessDataProduct dataAccessProduct = new DataAccessDataProduct(id);
                switch (dataAccessProduct.getDataAccessProductType())
                {
                case cube:
                    if (DataAccessUtil.imageCutoutsShouldBeCreated(dataAccessJob))
                    {
                        fileSizeKb += addImageCutouts(dataAccessJob, dataAccessProduct, dataAccessJob.getParamMap());
                    }
                    else
                    {
                        fileSizeKb += addImageCube(dataAccessJob, dataAccessProduct);
                    }
                    break;
                case visibility:
                    fileSizeKb += addMeasurementSet(dataAccessJob, dataAccessProduct);
                    break;
                case catalogue:
                    addCatalogue(dataAccessJob, dataAccessProduct);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Record type not supported: " + dataAccessProduct.getDataAccessProductType());
                }
            }
        }
        dataAccessJob.setSizeKb(fileSizeKb);

        logger.debug("Size {} limit {}", fileSizeKb, sizeLimit);

        if (sizeLimit != null && fileSizeKb > sizeLimit)
        {
            throw new SizeLimitReachedException("Data products accessed through this method are limited to "
                    + DataAccessUtil.convertBytesToGb(sizeLimit * FileUtils.ONE_KB) + "GB. "
                    + "Your requested data exceeded this limit at "
                    + DataAccessUtil.convertBytesToGb(fileSizeKb * FileUtils.ONE_KB) + "GB");
        }

        return dataAccessJob;
    }

    /**
     * Prepares an async job to start, by reading the request information from the data access job's param map, and
     * either calculating the cutouts that will be generated (if relevant). It will link to the requested entities
     * (image cube, measurement set, catalogues) and create the cutouts (if required).
     * 
     * @param jobId
     *            the job request id
     * @param dataProductIds
     *            the list of unencoded data product ids
     * @param sizeLimit
     *            the size limit for the job, null is unlimited.
     * @return the updated data access job
     */
    public DataAccessJob prepareAsyncJobToStart(String jobId, String[] dataProductIds, Long sizeLimit)
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(jobId);
        createDataAccessProducts(dataProductIds, dataAccessJob, sizeLimit);
        return dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Internal helper method used to actually execute a UWSJob
     * 
     * @param paramMap
     *            The map of job parameters.
     * @param uwsJobQueue
     *            the UWS job queue that the job will be run on
     */
    // TODO: If the residual test cases refering to this are removed then inline it.
    void executeUWSJob(Map<String, Object> paramMap, JobList uwsJobQueue)
    {
        try
        {
            // Create UWSParameters
            UWSParameters uwsParameters = new UWSParameters(paramMap);

            // Set status as ready to start.
            uwsParameters.set(UWSJob.PARAM_PHASE, UWSJob.PHASE_RUN);

            // Create job
            UWSJob job = new CasdaUwsJob(uwsParameters);

            logger.debug("Putting job id {} on queue {}", job.getJobId(), uwsJobQueue.getName());

            uwsJobQueue.addNewJob(job);
        }
        catch (UWSException ex)
        {
            // Unexpected exception
            throw new RuntimeException(ex);
        }
    }

    /**
     * Prioritises a job by setting its position in its queue as requested. This position does not include the currently
     * running jobs.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @param position
     *            the index in its queue to move the job to
     * @return if the prioritisation was successful
     */
    public boolean prioritise(String requestId, int position)
    {
        UWSJob job = getJob(requestId);
        if (job != null && job.getJobList() != null
                && job.getJobList().getExecutionManager() instanceof PriorityQueueExecutionManager)
        {
            return ((PriorityQueueExecutionManager) job.getJobList().getExecutionManager()).sendToPosition(requestId,
                    position);
        }
        else
        {
            return false;
        }
    }

    /**
     * Resubmit a job for processing after it has succeeded, failed, been aborted, or paused (ie: held) . If the job is
     * unknown then a ResourceNotFoundException will be thrown. If the job is not one of the expected states then a
     * ResourceIllegalStateException will be thrown.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     * @throws ScheduleJobException
     *             if the (re)scheduling of the job failed
     */
    public void retryJob(String requestId)
            throws ResourceNotFoundException, ResourceIllegalStateException, ScheduleJobException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }

        EnumSet<ExecutionPhase> restartablePhases =
                EnumSet.of(ExecutionPhase.ABORTED, ExecutionPhase.ERROR, ExecutionPhase.COMPLETED, ExecutionPhase.HELD);

        ExecutionPhase executionPhase = getUWSJobPhaseForDataAccessJob(dataAccessJob);
        if (!restartablePhases.contains(executionPhase))
        {
            throw new ResourceIllegalStateException("Job with requestId '" + requestId
                    + "' cannot be retried because it is in state '" + executionPhase + "'");
        }

        UWSJob uwsJob = getJob(requestId);
        // if the job is already on a uws queue, we need to remove it before we can retry it
        if (uwsJob != null)
        {
            for (JobList jobList : this.getJobLists())
            {
                if (jobList.getJob(requestId) != null)
                {
                    jobList.destroyJob(requestId);
                    if (jobList.getJob(requestId) != null)
                    {
                        throw new RuntimeException("Job with requestId '" + requestId + "' on queue '"
                                + jobList.getName() + "' could not be destroyed.");
                    }
                    break;

                }
            }
        }

        dataAccessJob.setStatus(DataAccessJobStatus.PREPARING);
        dataAccessJobRepository.save(dataAccessJob);

        // at this point there should be no usw job on the queue with this job id, so we can retry the job
        doScheduleJob(dataAccessJob);
    }

    private UWSJob tryAbortJob(String requestId)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        UWSJob job = getJob(requestId);
        if (job == null)
        {
            return null;
        }

        PriorityQueueExecutionManager priorityQueueExecutionManager =
                (PriorityQueueExecutionManager) job.getJobList().getExecutionManager();
        boolean alreadyPaused = priorityQueueExecutionManager.isQueuePaused();

        if (!alreadyPaused)
        {
            try
            {
                /*
                 * Pause the queue to remove the race condition between the job scheduler and this abort action.
                 */
                priorityQueueExecutionManager.pauseQueue();
            }
            catch (UWSException e)
            {
                throw new RuntimeException("Could not pause job list '" + job.getJobList().getName()
                        + "'. System may be in an unstable state!", e);
            }
        }
        try
        {
            try
            {
                job.abort();
            }
            catch (UWSException ex)
            {
                logger.debug("UWSException received attempting to abort job '" + requestId + "'");
                /*
                 * Ignore this as this method is just trying to abort. A failure to abort will be reflected in the
                 * returned UWSJob's phase.
                 */
            }
            // Try again if the thread is still not interrupted. See the abort documentation for why we might do this.
            if (job.isRunning())
            {
                try
                {
                    job.abort();
                }
                catch (UWSException ex)
                {
                    logger.debug("UWSException received attempting to abort job '" + requestId + "'");
                    /*
                     * Ignore this as this method is just trying to abort. A failure to abort will be reflected in the
                     * returned UWSJob's phase.
                     */
                }
            }
        }
        finally
        {
            if (!alreadyPaused)
            {
                try
                {
                    priorityQueueExecutionManager.unpauseQueue();
                }
                catch (UWSException e)
                {
                    throw new RuntimeException("Could not unpause job list '" + job.getJobList().getName()
                            + "'. System may be in an unstable state!", e);
                }
            }
        }

        ExecutionPhase postAbortPhase = job.getPhase();
        if (postAbortPhase != ExecutionPhase.ABORTED)
        {
            logger.debug("Final state for job '" + requestId + "' after abort is '" + postAbortPhase + "'");
        }

        return job;
    }
    
    private void killSlurmJobsWherePossible(Collection<DownloadFile> files, DataAccessJob dataAccessJob)
    {
        for (DownloadFile file : files)
        {
            CachedFile cachedFile = cacheManager.getCachedFile(file.getFileId());
            if (isSlurmJobSafeToKill(cachedFile))
            {                
                slurmJobManager.cancelJob(cachedFile.getDownloadJobId());
                cacheManager.clearCacheIfPossible(cachedFile);
            }
        }
    }
    
    private boolean isSlurmJobSafeToKill(CachedFile cachedFile)
    {        
        if(cachedFile == null)
        {
            return false;
        }
        
        //only kill running slurm jobs that are not referred in any other DataAccessJobs 
        return !cachedFile.isFileAvailableFlag() && cachedFile.getDataAccessJobs().size() < 2;
    }

    /**
     * Pauses a job that is preparing. If the job is unknown then a ResourceNotFoundException will be thrown. If the job
     * is not one of the expected states then a ResourceIllegalStateException will be thrown.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    public void pauseJob(String requestId) throws ResourceNotFoundException, ResourceIllegalStateException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }
        if (IMMEDIATE_JOB_LIST_NAME.equals(getJobList(dataAccessJob).getName()))
        {
            throw new ResourceNotFoundException(
                    "Job with requestId '" + requestId + "' could not be paused as it is an immediate job");
        }

        /*
         * We try to abort the job regardless of the state of the DataAccessJob. This is because the job is running on
         * its own thread and potentially updating the state of the DataAccessJob. The state of the returned UWSJob will
         * let us determine if this succeeded or not.
         */
        UWSJob uwsJob = tryAbortJob(requestId);
        if (uwsJob != null && ExecutionPhase.ABORTED != uwsJob.getPhase()
                || uwsJob == null && DataAccessJobStatus.PREPARING != dataAccessJob.getStatus())
        {
            throw new ResourceIllegalStateException("Job with requestId '" + requestId
                    + "' cannot be paused as it is in phase '" + getUWSJobPhaseForDataAccessJob(dataAccessJob) + "'");
        }
        dataAccessJob.setStatus(DataAccessJobStatus.PAUSED);
        dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Find out whether the given job is pausable.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @return true if the job is pausable
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    public boolean isPausable(String requestId) throws ResourceNotFoundException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }

        return !IMMEDIATE_JOB_LIST_NAME.equals(getJobList(dataAccessJob).getName()) && EnumSet
                .of(ExecutionPhase.PENDING, ExecutionPhase.QUEUED).contains(getJobStatus(requestId).getPhase());
    }

    /**
     * Cancels a job in any state (though calling this for an already cancelled job will have no effect). If the job is
     * unknown then a ResourceNotFoundException will be thrown. If for some reason the job could not be cancelled (eg:
     * because it is running and does not support cancellation) then a ResourceIllegalStateException will be thrown.
     * Passing an expiryTime into this method which is 'now' or sometime in the past effectively marks the job as
     * 'deleted' (there is no corresponding UWSJob ExecutionPhase to indicate that a job has been deleted).
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @param expiryTime
     *            the expiry time of the cancelled job
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job could not be cancelled
     */
    @Transactional
    public void cancelJob(String requestId, DateTime expiryTime)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }

        /*
         * We try to abort the job regardless of the state of the DataAccessJob. This is because the job is running on
         * its own thread and potentially updating the state of the DataAccessJob. The state of the returned UWSJob will
         * let us determine if this succeeded or not.
         */
        UWSJob uwsJob = tryAbortJob(dataAccessJob.getRequestId());
        if (uwsJob != null && !UWS_JOB_FINISHED_EXECUTION_PHASES.contains(uwsJob.getPhase()))
        {
            throw new ResourceIllegalStateException("Job with requestId '" + requestId
                    + "' cannot be cancelled at this time as it is still in phase '" + uwsJob.getPhase() + "'");
        }
        dataAccessJob.setExpiredTimestamp(expiryTime);
        dataAccessJob.setStatus(DataAccessJobStatus.CANCELLED);
        dataAccessJobRepository.save(dataAccessJob);
        Collection<DownloadFile> files = DataAccessUtil.getDataAccessJobDownloadFiles(dataAccessJob,
                cacheManager.getJobDirectory(dataAccessJob));
        cacheManager.updateUnlockForFiles(files, expiryTime);
                
        killSlurmJobsWherePossible(files, dataAccessJob);
    }

    /**
     * Schedule a job to be executed. If all the files are already available in the cache, run the job immediately.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     * @throws ScheduleJobException
     *             if the job could not be scheduled
     */
    public void scheduleJob(String requestId)
            throws ResourceNotFoundException, ResourceIllegalStateException, ScheduleJobException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }

        UWSJob job = getJobStatus(requestId);
        if (ExecutionPhase.PENDING != job.getPhase())
        {
            throw new ResourceIllegalStateException("Job with requestId '" + requestId
                    + "' cannot be scheduled because it is in state '" + job.getPhase() + "'");
        }

        doScheduleJob(dataAccessJob);
    }

    private void doScheduleJob(DataAccessJob dataAccessJob) throws ScheduleJobException
    {
        Map<String, Object> paramMap = assembleJobParams(dataAccessJob);

        JobList jobList = getJobList(dataAccessJob);
        if (dataAccessJob.getDownloadMode() == CasdaDownloadMode.SIAP_SYNC
                && !IMMEDIATE_JOB_LIST_NAME.equals(jobList.getName()))
        {
            throw new ScheduleJobException("Not all files for the sync job are available in the cache.");
        }

        executeUWSJob(paramMap, jobList);
    }

    /**
     * Populate a job parameters map for the data access job.
     * 
     * @param dataAccessJob
     *            The data access job.
     * @return The map of relevant parameters from the job.
     */
    private Map<String, Object> assembleJobParams(DataAccessJob dataAccessJob)
    {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put(AccessJobManager.REQUEST_ID, dataAccessJob.getRequestId());
        return params;
    }

    /**
     * Retrieves a list of data access jobs with the status "PREPARING" or "PAUSED". These are new jobs for which data
     * is being, or needs to be, downloaded.
     * 
     * @return A list of preparing jobs.
     */
    public List<DataAccessJobDto> getRunningJobList()
    {
        List<DataAccessJob> preparingJobs = dataAccessJobRepository.findPreparingJobs();
        List<DataAccessJobDto> runningJobs = combineRecordsWithUWSJobDataOrderByUWS(preparingJobs);
        List<DataAccessJob> pausedJobs = dataAccessJobRepository.findPausedJobs();
        runningJobs.addAll(combineRecordsWithUWSJobDataOrderByList(pausedJobs));
        return runningJobs;
    }

    /**
     * Retrieves a list of data access jobs that recently became available for users to download. The number of days
     * worth of records to be displayed is configured with the property admin.ui.availablejobs.days
     * 
     * @return A list of recently available jobs.
     */
    public List<DataAccessJobDto> getRecentlyCompletedJobList()
    {
        List<DataAccessJob> recentlyAvailableJobs = dataAccessJobRepository
                .findCompletedJobsAfterTime(new DateTime(DateTimeZone.UTC).minusDays(this.displayDaysOfAvailableJobs));
        return combineRecordsWithUWSJobDataOrderByList(recentlyAvailableJobs);
    }

    /**
     * Retrieves a list of recently failed data access jobs. The number of days worth of records to be displayed is
     * configured with the property admin.ui.failedjobs.days
     * 
     * @return A list of recent jobs.
     */
    public List<DataAccessJobDto> getRecentlyFailedJobList()
    {
        List<DataAccessJob> recentlyFailedJobs = dataAccessJobRepository
                .findFailedJobsAfterTime(new DateTime(DateTimeZone.UTC).minusDays(this.displayDaysOfFailedJobs));
        return combineRecordsWithUWSJobDataOrderByList(recentlyFailedJobs);
    }

    /**
     * Creates a list of DataAccessJobDtos whose order matches the given list, and combines information from the given
     * list with information from the UWS job list.
     * 
     * @param dataAccessJobs
     *            the given list of records
     * @return list of DataAccessJobDtos
     */
    private List<DataAccessJobDto> combineRecordsWithUWSJobDataOrderByList(List<DataAccessJob> dataAccessJobs)
    {
        List<DataAccessJobDto> jobs = new ArrayList<>();
        for (DataAccessJob failedJob : dataAccessJobs)
        {
            UWSJob uwsJob = getJob(failedJob.getRequestId());
            jobs.add(new DataAccessJobDto(failedJob, uwsJob, uwsJob == null ? null : uwsJob.getJobList().getName()));
        }
        return jobs;
    }

    /**
     * Creates a list of DataAccessJobDtos whose order matches the order in the UWS queue (executing first, then queued
     * jobs), followed by any jobs in the UWS queue that are not in the list (should never happen) and finally followed
     * by any jobs in the given list that are not in the UWS queue
     * 
     * @param dataAccessJobs
     *            the given list
     * @return list of DataAccessJobDtos
     */
    private List<DataAccessJobDto> combineRecordsWithUWSJobDataOrderByUWS(List<DataAccessJob> dataAccessJobs)
    {
        // create a hash map of the data access jobs for fast lookup
        LinkedHashMap<String, DataAccessJob> dataAccessJobsCopy =
                new LinkedHashMap<String, DataAccessJob>(dataAccessJobs.size());
        for (DataAccessJob job : dataAccessJobs)
        {
            dataAccessJobsCopy.put(job.getRequestId(), job);
        }

        // add all the currently running and queued jobs
        List<DataAccessJobDto> jobs = new ArrayList<>();
        int totalJobCount = categoryAJobList.getNbJobs() + immediateJobList.getNbJobs() + categoryBJobList.getNbJobs();
        List<UWSJob> uwsRunningAndQueuedJobs = new ArrayList<>();

        Iterator<UWSJob> uwsJobsRunningImmediately = immediateJobList.getExecutionManager().getRunningJobs();
        while (uwsJobsRunningImmediately.hasNext())
        {
            uwsRunningAndQueuedJobs.add(uwsJobsRunningImmediately.next());
        }
        uwsRunningAndQueuedJobs
                .addAll(((PriorityQueueExecutionManager) categoryAJobList.getExecutionManager()).getOrderedJobList());
        uwsRunningAndQueuedJobs
                .addAll(((PriorityQueueExecutionManager) categoryBJobList.getExecutionManager()).getOrderedJobList());

        for (UWSJob uwsjob : uwsRunningAndQueuedJobs)
        {
            String requestId = (String) uwsjob.getParameter(REQUEST_ID);
            if (dataAccessJobsCopy.containsKey(requestId))
            {
                DataAccessJobDto jobDto =
                        new DataAccessJobDto(dataAccessJobsCopy.get(requestId), uwsjob, uwsjob.getJobList().getName());
                jobs.add(jobDto);
                dataAccessJobsCopy.remove(requestId);
                if (dataAccessJobsCopy.size() == 0)
                {
                    break;
                }
            }
        }

        // add the other jobs that uws knows about, sorted by phase
        List<DataAccessJobDto> uwsJobs = new ArrayList<>();
        if (dataAccessJobsCopy.keySet().size() > 0 && totalJobCount > jobs.size())
        {
            List<String> removeKeys = new ArrayList<>();
            for (DataAccessJob job : dataAccessJobsCopy.values())
            {
                UWSJob queuingJob = getJob(job.getRequestId());
                if (queuingJob != null)
                {
                    DataAccessJobDto jobDto = new DataAccessJobDto(job, queuingJob, queuingJob.getJobList().getName());
                    uwsJobs.add(jobDto);
                    removeKeys.add(job.getRequestId());
                }
            }
            for (String key : removeKeys)
            {
                dataAccessJobsCopy.remove(key);
            }
        }
        Collections.sort(uwsJobs);
        jobs.addAll(uwsJobs);

        // add all the jobs from the database that uws doesn't know about
        if (dataAccessJobsCopy != null)
        {
            for (DataAccessJob job : dataAccessJobsCopy.values())
            {
                jobs.add(new DataAccessJobDto(job, null, null));
            }
        }

        return jobs;

    }

    /**
     * Complete initialisation of the class once Spring has injected all required parameter values.
     * 
     * @throws UWSException
     *             If the UWS instance cannot be initialised
     */
    @PostConstruct
    public void init() throws UWSException
    {
        setupNewUws();
    }

    /**
     * Ensure the job queue is backed up when shutting the server down.
     */
    @PreDestroy
    public void backupJobQueue()
    {
        uws.getBackupManager().saveAll();
        logger.info("Backed up UWS job queue.");
    }

    /**
     * Create the Universal Worker Service instance to be used for data access queues. This will pick up any jobs which
     * were not completed when the system was stopped.
     * 
     * @throws UWSException
     *             If the UWS instance cannot be initialised
     */
    private void setupNewUws() throws UWSException
    {
        uws = new UWSService(this.uwsFactory, this.uwsFileManager, new UWSUrl(baseUrl));
        categoryAJobList = new JobList(CATEGORY_A_JOB_LIST_NAME,
                new PriorityQueueExecutionManager(uws.getLogger(), categoryAMaxRunningJobs));
        uws.addJobList(categoryAJobList);
        categoryBJobList = new JobList(CATEGORY_B_JOB_LIST_NAME,
                new PriorityQueueExecutionManager(uws.getLogger(), categoryBMaxRunningJobs));
        uws.addJobList(categoryBJobList);

        // Set the immediate job list to run with no queue - ie all get started immediately
        immediateJobList = new JobList(IMMEDIATE_JOB_LIST_NAME, new QueuedExecutionManager(uws.getLogger()));
        uws.addJobList(immediateJobList);

        uws.setBackupManager(new DataAccessJobBackupManager(uws));
        // try and restore previous jobs
        uws.getBackupManager().restoreAll();
    }

    // TODO: Remove if the corresponding residual test cases are removed
    void setCategoryAJobList(JobList jobList)
    {
        this.categoryAJobList = jobList;
    }

    // TODO: Remove if the corresponding residual test cases are removed
    void setCategoryBJobList(JobList jobList)
    {
        this.categoryBJobList = jobList;
    }

    // TODO: Remove if the corresponding residual test cases are removed
    void setImmediateJobList(JobList jobList)
    {
        this.immediateJobList = jobList;
    }

    /**
     * Gets the uws queue with this name
     * 
     * @param queue
     *            the uws queue name
     * @return the uws queue
     */
    JobList getQueue(String queue)
    {
        return uws.getJobList(queue);
    }

    List<JobList> getJobLists()
    {
        return Arrays.asList(this.categoryAJobList, this.categoryBJobList, this.immediateJobList);
    }

    public int getDisplayDaysOfAvailableJobs()
    {
        return displayDaysOfAvailableJobs;
    }

    public int getDisplayDaysOfFailedJobs()
    {
        return displayDaysOfFailedJobs;
    }

    /**
     * Get current uwsjob based on requestId
     * 
     * @param requestId
     *            The id of the request to be retreive.
     * @return null for not found.
     */
    private UWSJob getJob(String requestId)
    {
        for (JobList joblist : getJobLists())
        {
            UWSJob job = joblist.getJob(requestId);
            if (job != null)
            {
                return job;
            }
        }
        return null;
    }

    /**
     * Pause the given queue. This will let the currently executing job finish, but no more jobs will be started.
     * 
     * @param queue
     *            the name of the queue to pause
     * @return true if the queue was successfully paused
     * @throws UWSException
     *             if there is a problem pausing the queue
     */
    public boolean pauseQueue(String queue) throws UWSException
    {
        if (StringUtils.isBlank(queue))
        {
            return false;
        }
        JobList jobList = getQueue(queue);
        if (jobList != null && jobList.getExecutionManager() instanceof PriorityQueueExecutionManager)
        {
            ((PriorityQueueExecutionManager) jobList.getExecutionManager()).pauseQueue();
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Unpause the given queue
     * 
     * @param queue
     *            the name of the queue to unpause
     * @return true if the queue was successfully unpaused
     * @throws UWSException
     *             if there is a problem unpausing the queue
     */
    public boolean unpauseQueue(String queue) throws UWSException
    {
        if (StringUtils.isBlank(queue))
        {
            return false;
        }
        JobList jobList = getQueue(queue);
        if (jobList != null && jobList.getExecutionManager() instanceof PriorityQueueExecutionManager)
        {
            ((PriorityQueueExecutionManager) jobList.getExecutionManager()).unpauseQueue();
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * Checks whether the job queue is paused.
     * 
     * @param queue
     *            the name of the queue to check
     * @return true if the queue is paused, false otherwise.
     */
    public boolean isQueuePaused(String queue)
    {
        if (StringUtils.isBlank(queue))
        {
            return false;
        }
        JobList jobList = getQueue(queue);
        if (jobList != null && jobList.getExecutionManager() instanceof PriorityQueueExecutionManager)
        {
            return ((PriorityQueueExecutionManager) jobList.getExecutionManager()).isQueuePaused();
        }
        else
        {
            return false;
        }
    }

    public long getCategoryAJobMaxSize()
    {
        return categoryAJobMaxSize;
    }

    private long addImageCube(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;
        ImageCube ic = imageCubeRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (ic != null)
        {
            dataAccessJob.addImageCube(ic);
            fileSizeKb += ic.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("image_cube").add(dataAccessProduct.getId())
                    .toString());
        }
        return fileSizeKb;
    }

    private long addImageCutouts(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct,
            ParamMap dataAccessJobParams)
    {
        long fileSizeKb = 0;
        ImageCube ic = imageCubeRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (ic != null)
        {
            List<CutoutBounds> cutoutBounds = cutoutService.calcCutoutBounds(dataAccessJobParams, ic);
            for (CutoutBounds cutoutBound : cutoutBounds)
            {
                ImageCutout cutout = new ImageCutout();
                cutout.setImageCube(ic);
                cutout.setBounds(cutoutBound.toString());

                List<ImageCubeAxis> imageCubeAxes = cutoutService.getAxisList(ic);
                ImageCubeAxis imageCubeAxis = imageCubeAxes.isEmpty() ? null : imageCubeAxes.get(0);
                // estimate file size
                double sizeOneLayer = imageCubeAxis == null
                        ? (double) ic.getFilesize() : ((double) ic.getFilesize()) / ((double) 
                                imageCubeAxis.getSize() * imageCubeAxis.getPlaneSpan());
                final double cutoutPercentageOfOriginal = cutoutBound.calculateFovEstimate() / ((double) ic.getSFov());
                int numPlanes = cutoutBound.getNumPlanes();
                cutout.setFilesize(Math.round(Math.min(cutoutPercentageOfOriginal, 1) * sizeOneLayer * numPlanes));

                dataAccessJob.addImageCutout(cutout);
                fileSizeKb = cutout.getFilesize();
            }
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("image_cube").add(dataAccessProduct.getId())
                    .toString());
        }
        return fileSizeKb;
    }

    private long addMeasurementSet(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        MeasurementSet measurementSet = measurementSetRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (measurementSet != null)
        {
            dataAccessJob.addMeasurementSet(measurementSet);
            fileSizeKb = measurementSet.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("measurement_set")
                    .add(dataAccessProduct.getId()).toString());
        }
        return fileSizeKb;
    }

    private void addCatalogue(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        Catalogue catalogue = catalogueRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (catalogue != null)
        {
            dataAccessJob.addCatalogue(catalogue);
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("catalogue").add(dataAccessProduct.getId())
                    .toString());
        }

    }

    /**
     * Returns the status of a job as a UWSJob record. Please note that the returned record is an independent object
     * from any particular implementation of 'job running'. Returns a UWSJob representation of a DataAccessJob.
     * 
     * @param requestId
     *            the request id (DataAccessJob table)
     * @return a UWSJob representation of the given job
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    public UWSJob getJobStatus(String requestId) throws ResourceNotFoundException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with requestId '" + requestId + "' could not be found");
        }

        /*
         * Force a reload of the DataAccessJob to ensure it is the most up-to-date value in the database.
         */
        EntityManager entityManager = emf.createEntityManager();
        try
        {
            dataAccessJob = entityManager.find(DataAccessJob.class, dataAccessJob.getId());
        }
        finally
        {
            entityManager.close();
        }

        ExecutionPhase phase = getUWSJobPhaseForDataAccessJob(dataAccessJob);
        /*
         * Using the DataAccessJob's creation time as the startTime isn't as accurate as using a real UWSJob's start
         * time but gives us consistent results.
         */
        long startTime = dataAccessJob.getCreatedTimestamp().getMillis();
        long endTime;
        if (EnumSet.of(ExecutionPhase.ABORTED, ExecutionPhase.ERROR).contains(phase))
        {
            endTime = dataAccessJob.getLastModified().getMillis();
        }
        else if (ExecutionPhase.COMPLETED == phase)
        {
            endTime = dataAccessJob.getAvailableTimestamp().getMillis();
        }
        else
        {
            endTime = 0; // USWJob will treat this as if it was a 'null'
        }
        ErrorSummary errorSummary;
        if (ExecutionPhase.ERROR == phase)
        {
            String errMsg = dataAccessJob.getErrorMessage();
            if (StringUtils.isBlank(errMsg))
            {
                errMsg = "Error: A problem occured obtaining access to the requested item(s)";
            }
            errorSummary = new ErrorSummary(errMsg, ErrorType.FATAL);
        }
        else
        {
            errorSummary = null;
        }
        ArrayList<Result> results = new ArrayList<>();
        /*
         * TODO: The UWS spec states that:
         * 
         * Image results are added to the results list, and to the "table" result, as they are generated. Hence, a
         * client that polls the service can discover, download and use some of the images before the job is finished.
         * If the client is satisfied with these early images, the client can cancel the rest of the job by destroying
         * the job. However, destroying the job deletes the cached images so the client has to download them first.
         * 
         * This isn't an issue for us because we only support one image request at a time but if we ever support
         * multiple then this is something we will have to think about.
         */
        if (phase == ExecutionPhase.COMPLETED)
        {
            for (ImageCube imageCube : dataAccessJob.getImageCubes())
            {
                results.add(new Result(DataAccessProductType.cube + "-" + imageCube.getId(), XLINK_SIMPLE_TYPE,
                        fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), imageCube.getFileId()),
                        false));
                results.add(new Result(DataAccessProductType.cube + "-" + imageCube.getId() + ".checksum",
                        XLINK_SIMPLE_TYPE,
                        fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFileChecksum(
                                dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), imageCube.getFileId()),
                        false));
            }
            for (ImageCutout cutout : dataAccessJob.getImageCutouts())
            {
                String cutoutId = "cutout-" + cutout.getId();
                results.add(new Result(cutoutId, XLINK_SIMPLE_TYPE,
                        fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), cutout.getFileId()),
                        false));
                results.add(new Result(cutoutId + ".checksum",
                        XLINK_SIMPLE_TYPE,
                        fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFileChecksum(
                                dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), cutout.getFileId()),
                        false));
            }
            for (MeasurementSet measurementSet : dataAccessJob.getMeasurementSets())
            {
                results.add(
                        new Result(
                                DataAccessProductType.visibility + "-"
                                        + measurementSet.getId(),
                                XLINK_SIMPLE_TYPE,
                                fileDownloadBaseUrl + "/"
                                        + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                dataAccessJob.getRequestId(), measurementSet.getFileId()),
                                false));
                results.add(new Result(DataAccessProductType.visibility + "-" + measurementSet.getId() + ".checksum",
                        XLINK_SIMPLE_TYPE,
                        fileDownloadBaseUrl + "/"
                                + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                        dataAccessJob.getRequestId(), measurementSet.getFileId()),
                        false));
            }
        }

        return createUWSJob(dataAccessJob.getRequestId(), phase, startTime, endTime,
                dataAccessJob.getExpiredTimestamp(), dataAccessJob.getParamMap(), results, errorSummary);
    }

    /**
     * Creates a UWSJob with the given parameters. This method is public static primarily because UWSJob has several
     * 'final' elements that prevent it from being mocked for testing.
     * 
     * @param jobId
     *            the job id
     * @param phase
     *            the ExecutionPhase
     * @param startTime
     *            the startTime of the job
     * @param endTime
     *            the endTime of the job (optional)
     * @param destruction
     *            the destruction time of the job (optional)
     * @param paramMap
     *            the user supplied parameters for this job.
     * @param results
     *            the results (required but may be empty)
     * @param errorSummary
     *            the error summary (optional)
     * @return the UWSJob
     */
    public static UWSJob createUWSJob(String jobId, ExecutionPhase phase, long startTime, long endTime,
            DateTime destruction, ParamMap paramMap, List<Result> results, ErrorSummary errorSummary)
    {
        try
        {
            UWSParameters uwsParams = new UWSParameters();
            for (String key : paramMap.keySet())
            {
                String[] values = paramMap.get(key);
                String valueString = "";
                if (values.length == 1)
                {
                    valueString = values[0];
                }
                else if (values.length > 1)
                {
                    valueString = ArrayUtils.toString(values);
                }
                uwsParams.set(key, valueString);
            }
            UWSJob job = new UWSJob(// This is the only constructor available to us to create a UWSJob with a jobId
                    jobId, //
                    null, // We don't use the concept of a job owner
                    uwsParams, //
                    UWSJob.QUOTE_NOT_KNOWN, // we don't use quotes
                    startTime, //
                    endTime, //
                    results, //
                    errorSummary); // error summary will be set later if required
            Date destructionTime;
            if (EnumSet.of(ExecutionPhase.COMPLETED, ExecutionPhase.ERROR, ExecutionPhase.ABORTED).contains(phase))
            {
                destructionTime = destruction.toDate();
            }
            else
            {
                /*
                 * It may actually not be null as the destruction time may be asynchronously updated when the job stops
                 * but we need to report it as null in keeping with the phase being reported.
                 */
                destructionTime = null;
            }
            /*
             * Can't set the destruction time in anything other than PENDING unfortunately.
             */
            job.setPhase(ExecutionPhase.PENDING, true /* force */);
            job.setDestructionTime(destructionTime);
            job.setPhase(phase, true /* force */);

            /*
             * We're currently not supporting users being able to specify a (max) executionDuration so this will always
             * be zero (ie: unlimited)
             */
            job.setExecutionDuration(0);
            return job;
        }
        catch (UWSException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private ExecutionPhase getUWSJobPhaseForDataAccessJob(DataAccessJob dataAccessJob)
    {
        /*
         * Note: we do not report UNKNOWN because we always know our state. Also we don't currently support SUSPENDED,
         * though technically pausing the queue could result in Jobs being listed as SUSPENDED.
         */
        switch (dataAccessJob.getStatus())
        {
        case PREPARING:
            UWSJob runningJob = this.getJob(dataAccessJob.getRequestId());
            if (runningJob == null)
            {
                /*
                 * DataAccessJob's won't have a backing UWSJob until they are scheduled
                 */
                return ExecutionPhase.PENDING;
            }
            else
            {
                if (runningJob.getPhase() == ExecutionPhase.QUEUED)
                {
                    return ExecutionPhase.QUEUED;
                }
                else
                {
                    /*
                     * To avoid potential race conditions between UWS updating and persisting the Job state and updating
                     * and persisting the DataAccessJob state, any other UWSJob state at this point will be treated as
                     * ExecutionPhase.EXECUTING.
                     */
                    return ExecutionPhase.EXECUTING;
                }
            }
        case CANCELLED:
            return ExecutionPhase.ABORTED;
        case PAUSED:
            return ExecutionPhase.HELD;
        case ERROR:
            return ExecutionPhase.ERROR;
        case READY:
        case EXPIRED:
            /*
             * From a UWS perspective, expired jobs are indicated by their destructiontime, so they are otherwise
             * COMPLETED.
             */
            return ExecutionPhase.COMPLETED;
        default:
            throw new RuntimeException("Unhandled DataAccessJob status '" + dataAccessJob.getStatus().toString() + "'");
        }
    }
}
