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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.GeneratedFileDescriptor;
import au.csiro.casda.access.ImageFormat;
import au.csiro.casda.access.JobDto;
import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.SizeLimitReachedException;
import au.csiro.casda.access.cache.CacheException;
import au.csiro.casda.access.cache.CacheManagerInterface;
import au.csiro.casda.access.jpa.CatalogueRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.EncapsulationFileRepository;
import au.csiro.casda.access.jpa.EvaluationFileRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.services.CasdaMailService;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.soda.GeneratedFileBounds;
import au.csiro.casda.access.soda.GenerateFileService;
import au.csiro.casda.access.soda.ImageCubeAxis;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.GeneratedSpectrum;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.Cubelet;
import au.csiro.casda.entity.observation.EncapsulationFile;
import au.csiro.casda.entity.observation.EvaluationFile;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.MomentMap;
import au.csiro.casda.entity.observation.Spectrum;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.LogEvent;
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
 * <li>PENDING - using {@link #scheduleJob(String)}</li>
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
 * {@link #pauseJob(String)}.</li>
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
     * Name of the list of category A (size &lt;= category.a.job.max.size.kb, files not in cache when job was created) data
     * access jobs in UWS.
     */
    public static final String CATEGORY_A_JOB_LIST_NAME = "Category A";

    /**
     * Name of the list of category A (size &gt; category.a.job.max.size.kb, files not in cache when job was created) data
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
    
    private final EncapsulationFileRepository encapsulationFileRepository;
    
    private final EvaluationFileRepository evaluationFileRepository;
    
    private final SpectrumRepository spectrumRepository;
    
    private final MomentMapRepository momentMapRepository;
    
    private final CubeletRepository cubeletRepository;
    
    private final CacheManagerInterface cacheManager;

    private final int displayDaysOfAvailableJobs;

    private final int displayDaysOfFailedJobs;

    private String fileDownloadBaseUrl;

    private UWSFileManager uwsFileManager;

    private UWSFactory uwsFactory;

    private UWSService uws;

    private GenerateFileService generateFileService;

    private final EntityManagerFactory emf;
    
    private JobManager slurmJobManager;
     
    private String dataLinkAccessSecretKey;
    
    private DataAccessService dataAccessService;
    
    private CasdaMailService casdaMailService;
    
    private int expiryNotificationPeriod;

    private int hoursToExpiryDefault;

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
     * @param spectrumRepository
     *            The JPA repository for the spectrumRepository table.
     * @param momentMapRepository
     *            The JPA repository for the momentMapRepository table.
     * @param cubeletRepository
     *            The JPA repository for the cubeletRepository table.
     * @param encapsulationFileRepository
     *            The JPA repository for the encapsulationFile table.
     * @param evaluationFileRepository
     *            The JPA repository for the evaluationFile table.
     * @param cacheManager
     *            the cache manager
     * @param uwsFactory
     *            a UWSFactory that will be used to create an appropriate JobThread to process a DataAccessJob
     * @param uwsFileManager
     *            a UWSFileManager that will be used to manage the persistence of of the job queues
     * @param generateFileService
     *            The service for calculating the generated file's bounds.
     * @param slurmJobManager
     *            The Slurm job manager.
     * @param dataAccessService
     *            The dataAccessService
     * @param casdaMailService
     * 			  the email service for sending user notifications
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
     * @param dataLinkAccessSecretKey
     * 			  the key for unencrypting the id token
     * @param expiryNotificationPeriod
     * 			  the amount of notice users get before their job expires
     * @param hoursToExpiryDefault
     *            the default number of hours until a job will expire
     */
    @Autowired
    public AccessJobManager(EntityManagerFactory emf, DataAccessJobRepository dataAccessJobRepository,
            ImageCubeRepository imageCubeRepository, CatalogueRepository catalogueRepository,
            MeasurementSetRepository measurementSetRepository, SpectrumRepository spectrumRepository, 
            MomentMapRepository momentMapRepository, CubeletRepository cubeletRepository, 
            EncapsulationFileRepository encapsulationFileRepository, 
            EvaluationFileRepository evaluationFileRepository, CacheManagerInterface cacheManager, 
            UWSFactory uwsFactory, UWSFileManager uwsFileManager, GenerateFileService generateFileService, 
            JobManager slurmJobManager, DataAccessService dataAccessService,
            CasdaMailService casdaMailService,
            @Value("${uws.baseurl}") String baseUrl,
            @Value("${uws.category.a.maxrunningjobs}") int categoryAMaxRunningJobs,
            @Value("${uws.category.b.maxrunningjobs}") int categoryBMaxRunningJobs,
            @Value("${admin.ui.availablejobs.days}") int displayDaysOfAvailableJobs,
            @Value("${admin.ui.failedjobs.days}") int displayDaysOfFailedJobs,
            @Value("${category.a.job.max.size.kb}") long categoryAJobMaxSize,
            @Value("${download.base.url}") String fileDownloadBaseUrl,
            @Value("${siap.shared.secret.key}") String dataLinkAccessSecretKey,
            @Value("${email.expiry.notification.period}") int expiryNotificationPeriod,
            @Value("${hours.to.expiry.default}") int hoursToExpiryDefault)
    {
        this.emf = emf;
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.imageCubeRepository = imageCubeRepository;
        this.catalogueRepository = catalogueRepository;
        this.measurementSetRepository = measurementSetRepository;
        this.spectrumRepository = spectrumRepository;
        this.momentMapRepository = momentMapRepository;
        this.cubeletRepository = cubeletRepository;
        this.cacheManager = cacheManager;
        this.uwsFactory = uwsFactory;
        this.uwsFileManager = uwsFileManager;
        this.generateFileService = generateFileService;
        this.baseUrl = baseUrl;
        this.categoryAMaxRunningJobs = categoryAMaxRunningJobs;
        this.categoryBMaxRunningJobs = categoryBMaxRunningJobs;
        this.displayDaysOfAvailableJobs = displayDaysOfAvailableJobs;
        this.displayDaysOfFailedJobs = displayDaysOfFailedJobs;
        this.categoryAJobMaxSize = categoryAJobMaxSize;
        this.fileDownloadBaseUrl = fileDownloadBaseUrl;
        this.slurmJobManager = slurmJobManager;
        this.dataLinkAccessSecretKey = dataLinkAccessSecretKey;
        this.dataAccessService = dataAccessService;
        this.encapsulationFileRepository = encapsulationFileRepository;
        this.evaluationFileRepository = evaluationFileRepository;
        this.casdaMailService = casdaMailService;
        this.expiryNotificationPeriod = expiryNotificationPeriod;
        this.hoursToExpiryDefault = hoursToExpiryDefault;
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
        
        boolean runImmediately = true;
        
        List<Map<FileType, Integer[]>> paging = dataAccessService.getPaging(dataAccessJob.getRequestId(), false);
        
        for(int pageNum = 0; pageNum < paging.size(); pageNum++)
        {
        	List<DownloadFile> files = dataAccessService.getPageOfFiles(paging.get(pageNum), dataAccessJob);
        	runImmediately &= cacheManager.allFilesAvailableInCache(files);
        }
        
        // if all the files are already available in the cache, run the job immediately
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
     * scheduled job which checks for expiring data access jobs and sends out notifications for those expiring within
     * the given window, period in properties &amp; period ,minus one day, this will stop multiple notifications 
     * from being sent out
     */
    @Scheduled(cron = "${email.expiring.period}")
    public void handleStatusAndNotificationsForExpiringJobs()
    {
        logger.info("Started actioning expiring jobs");
        int numEmails = 0;
    	for(DataAccessJob job : 
    		dataAccessJobRepository.findAllJobsForExpiryNotification(new DateTime().plusDays(expiryNotificationPeriod), 
    				new DateTime().plusDays(expiryNotificationPeriod-1)))
    	{
    		casdaMailService.sendEmail(job, CasdaMailService.EXPIRING_EMAIL, CasdaMailService.EXPIRING_EMAIL_SUBJECT);
    		numEmails++;
    	}
    	for(DataAccessJob job : dataAccessJobRepository.findExpiredJobs())
    	{
    		casdaMailService.sendEmail(job, CasdaMailService.EXPIRED_EMAIL, CasdaMailService.EXPIRED_EMAIL_SUBJECT);
            numEmails++;
    		job.setStatus(DataAccessJobStatus.EXPIRED);
    		dataAccessJobRepository.save(job);
    	}
    	// Expire any old preparing jobs
    	DateTime newestCreationTime = new DateTime().minusHours(hoursToExpiryDefault);
        for(DataAccessJob job : dataAccessJobRepository.findPreparingJobsOlderThanTime(newestCreationTime))
        {
            logger.info("Expiring old unstarted job " + job.getRequestId());
            job.setStatus(DataAccessJobStatus.EXPIRED);
            job.setExpiredTimestamp(new DateTime());
            dataAccessJobRepository.save(job);
        }
        logger.info("Finished actioning expiring jobs. " + numEmails + " emails sent.");
    }
    
    /**
     * @param dataAccessJob the job to find position in queue for
     * @return the queue position of this job
     */
    public int getPositionInJobList(DataAccessJob dataAccessJob)
    {
    	UWSJob uwsJob = getJob(dataAccessJob.getRequestId());
    	if(uwsJob == null || uwsJob.getPhase() == ExecutionPhase.COMPLETED 
    			|| uwsJob.getPhase() == ExecutionPhase.EXECUTING)
    	{
    		//jobs which are completing or executing return 0, also jobs which have been removed.
    		return 0;
    	}
    	Iterator<UWSJob> jobList = 
    			((PriorityQueueExecutionManager) uwsJob.getJobList().getExecutionManager()).getQueuedJobs();

    	int count = 1;
    	
    	while(jobList.hasNext())
    	{
    		UWSJob job = jobList.next();
    		if(job == uwsJob)
    		{
    			break;
    		}
    		else
    		{
    			if(!job.isFinished())
    			{
        			count++;
    			}
    		}
    	}
    	
		return count;
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
        casdaMailService.sendEmail(dataAccessJob, CasdaMailService.CREATED_EMAIL, CasdaMailService.CREATED_EMAIL_SUBJECT);   
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
        if (dataAccessJob.getParamMap().get("format") != null)
        {
            dataAccessJob.setDownloadFormat(
                    ImageFormat.findMatchingFormat(dataAccessJob.getParamMap().get("format")[0]).getFileExtension());
        }
        List<String> paramsWithoutCutouts = new ArrayList<>();
        long fileSizeKb = 0l;
        if (ids != null)
        {
            for (String id : ids)
            {
                DataAccessDataProduct dataAccessProduct = new DataAccessDataProduct(id);
                switch (dataAccessProduct.getDataAccessProductType())
                {
                case cube:
                    if (DataAccessUtil.imageCutoutsShouldBeCreated(dataAccessJob, dataLinkAccessSecretKey))
                    {
                        fileSizeKb += addImageCutouts(dataAccessJob, dataAccessProduct, dataAccessJob.getParamMap(),
                                paramsWithoutCutouts);
                    }
                    else if (DataAccessUtil.spectrumShouldBeCreated(dataAccessJob, dataLinkAccessSecretKey))
                    {
                        fileSizeKb += addGeneratedSpectra(dataAccessJob, dataAccessProduct, dataAccessJob.getParamMap(),
                                paramsWithoutCutouts);
                    }
                    else
                    {
                        fileSizeKb += addImageCube(dataAccessJob, dataAccessProduct);
                    }
                    break;
                case spectrum:
                    fileSizeKb += addSpectrum(dataAccessJob, dataAccessProduct);
                    break;
                case moment_map:
                    fileSizeKb += addMomentMap(dataAccessJob, dataAccessProduct);
                    break;
                case cubelet:
                    fileSizeKb += addCubelet(dataAccessJob, dataAccessProduct);
                    break;
                case visibility:
                    fileSizeKb += addMeasurementSet(dataAccessJob, dataAccessProduct);
                    break;
                case catalogue:
                    addCatalogue(dataAccessJob, dataAccessProduct);
                    break;
                case encap:
                	addEncapsulationFile(dataAccessJob, dataAccessProduct);
                	break;
                case evaluation:
                    fileSizeKb += addEvaluationFile(dataAccessJob, dataAccessProduct);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Record type not supported: " + dataAccessProduct.getDataAccessProductType());
                }
            }
        }
        dataAccessJob.setSizeKb(fileSizeKb);
        for (String unmatchedParamCombo : paramsWithoutCutouts)
        {
            addErrorFile(dataAccessJob, unmatchedParamCombo);
        }

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

    private void addErrorFile(DataAccessJob dataAccessJob, String unmatchedParamCombo)
    {
        String message = "UsageError: No data is available for the parameter combination: " + unmatchedParamCombo;
        
        dataAccessJob.addError(message);
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
                try
                {
                    cacheManager.clearCacheIfPossible(cachedFile);
                }
                catch (CacheException e)
                {
                    logger.error("Failed to delete cache file " + file.getFileId(),
                            CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT).toString(), e);
                }
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
     * @param dataAccessJob
     *            the DataAccessJob
     * @return true if the job is pausable
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    public boolean isPausable(DataAccessJob dataAccessJob) throws ResourceNotFoundException
    {
        return !IMMEDIATE_JOB_LIST_NAME.equals(getJobList(dataAccessJob).getName()) && EnumSet.of(
        		ExecutionPhase.PENDING, ExecutionPhase.QUEUED)
        		.contains(getJobStatus(dataAccessJob).getPhase());
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
        cancelJob(dataAccessJob, expiryTime);
    }
    
    private void cancelJob(DataAccessJob dataAccessJob, DateTime expiryTime)
            throws ResourceIllegalStateException, ResourceNotFoundException
    {
        /*
         * We try to abort the job regardless of the state of the DataAccessJob. This is because the job is running on
         * its own thread and potentially updating the state of the DataAccessJob. The state of the returned UWSJob will
         * let us determine if this succeeded or not.
         */
        UWSJob uwsJob = tryAbortJob(dataAccessJob.getRequestId());
        if (uwsJob != null && !UWS_JOB_FINISHED_EXECUTION_PHASES.contains(uwsJob.getPhase()))
        {
            throw new ResourceIllegalStateException("Job with requestId '" + dataAccessJob.getRequestId()
                    + "' cannot be cancelled at this time as it is still in phase '" + uwsJob.getPhase() + "'");
        }
        dataAccessJob.setExpiredTimestamp(expiryTime);
        dataAccessJob.setStatus(DataAccessJobStatus.CANCELLED);
        dataAccessJobRepository.save(dataAccessJob);
        
        List<Map<FileType, Integer[]>> paging = dataAccessService.getPaging(dataAccessJob.getRequestId(), false);
        
        for(int pageNum = 0; pageNum < paging.size(); pageNum++)
        {
        	List<DownloadFile> files = dataAccessService.getPageOfFiles(paging.get(pageNum), dataAccessJob);
            cacheManager.updateUnlockForFiles(files, expiryTime);
            killSlurmJobsWherePossible(files, dataAccessJob);
        }
    }

    /**
     * Delete all cache. This includes killing all the running slurm jobs and marking all paused and preparing jobs to
     * expired and delete cache file and data files from the disk
     * 
     * @throws CacheException if cahced item cannot be found or is in an illegal state
     */
    @Transactional
    public void deleteAllCache() throws CacheException
    {
        List<DataAccessJob> runningJobs = dataAccessJobRepository.findPreparingJobs();        
        runningJobs.addAll(dataAccessJobRepository.findPausedJobs());
        DateTime now = new DateTime();
        for (DataAccessJob dataAccessJob : runningJobs)
        {
            try
            {
                cancelJob(dataAccessJob, now);
            }
            catch (ResourceIllegalStateException | ResourceNotFoundException e)
            {
                throw new CacheException(e);
            }
        }
        dataAccessJobRepository.expireAllJobs();
        cacheManager.deleteAllCache();
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

        UWSJob job = getJobStatus(dataAccessJob);
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
        if (Utils.SYNC_DOWNLOADS.contains(dataAccessJob.getDownloadMode())
                && !(IMMEDIATE_JOB_LIST_NAME.equals(jobList.getName())
                        || CATEGORY_A_JOB_LIST_NAME.equals(jobList.getName())))
        {
            throw new ScheduleJobException("Some of the requested files are large and not currently available.");
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
            ParamMap dataAccessJobParams, List<String> paramsWithoutCutouts)
    {
        long fileSizeKb = 0;
        ImageCube ic = imageCubeRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (ic != null)
        {
            List<GeneratedFileBounds> cutoutBounds = generateFileService.calcGeneratedFileBounds(dataAccessJobParams, 
            		ic, dataAccessProduct.getDataProductId(), paramsWithoutCutouts);
            for (GeneratedFileBounds cutoutBound : cutoutBounds)
            {
                ImageCutout cutout = new ImageCutout();
                cutout.setImageCube(ic);
                cutout.setBounds(cutoutBound.toString());

                List<ImageCubeAxis> imageCubeAxes = generateFileService.getAxisList(ic);
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
    
    private long addGeneratedSpectra(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct,
            ParamMap dataAccessJobParams, List<String> paramsWithoutSpectra)
    {
        long fileSizeKb = 0;
        ImageCube ic = imageCubeRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (ic != null)
        {
        	 List<GeneratedFileBounds> spectraBounds = generateFileService.calcGeneratedFileBounds(dataAccessJobParams, 
             		ic, dataAccessProduct.getDataProductId(), paramsWithoutSpectra);
             for (GeneratedFileBounds spectraBound : spectraBounds)
             {
                 GeneratedSpectrum spectrum = new GeneratedSpectrum();
                 spectrum.setImageCube(ic);
                 spectrum.setBounds(spectraBound.toString());

                 List<ImageCubeAxis> imageCubeAxes = generateFileService.getAxisList(ic);
                 ImageCubeAxis imageCubeAxis = imageCubeAxes.isEmpty() ? null : imageCubeAxes.get(0);
                 // estimate file size
                 double sizeOneLayer = imageCubeAxis == null
                         ? (double) ic.getFilesize() : ((double) ic.getFilesize()) / ((double) 
                                 imageCubeAxis.getSize() * imageCubeAxis.getPlaneSpan());
                 final double percentageOfOriginal = spectraBound.calculateFovEstimate() / ((double) ic.getSFov());
                 int numPlanes = spectraBound.getNumPlanes();
                 spectrum.setFilesize(Math.round(Math.min(percentageOfOriginal, 1) * sizeOneLayer * numPlanes));

                 dataAccessJob.addGeneratedSpectrum(spectrum);
                 fileSizeKb = spectrum.getFilesize();
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
    
    private long addEncapsulationFile(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        EncapsulationFile encapFile = encapsulationFileRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (encapFile != null)
        {
            dataAccessJob.addEncapsulationFile(encapFile);
            fileSizeKb = encapFile.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("encapsulation_file")
                    .add(dataAccessProduct.getId()).toString());
        }
        return fileSizeKb;
    }
    
    private long addEvaluationFile(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        EvaluationFile evaluationFile = evaluationFileRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (evaluationFile != null)
        {
            dataAccessJob.addEvaluationFile(evaluationFile);
            fileSizeKb = evaluationFile.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("evaluation_file")
                    .add(dataAccessProduct.getId()).toString());
        }
        return fileSizeKb;
    }
    
    private long addSpectrum(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        Spectrum spectrum = spectrumRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (spectrum != null)
        {
            dataAccessJob.addSpectrum(spectrum);
            fileSizeKb = spectrum.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("spectrum")
                    .add(dataAccessProduct.getId()).toString());
        }
        return fileSizeKb;
    }
    
    private long addMomentMap(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        MomentMap momentmap = momentMapRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (momentmap != null)
        {
            dataAccessJob.addMomentMap(momentmap);
            fileSizeKb = momentmap.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("moment_map")
                    .add(dataAccessProduct.getId()).toString());
        }
        return fileSizeKb;
    }
    
    private long addCubelet(DataAccessJob dataAccessJob, DataAccessDataProduct dataAccessProduct)
    {
        long fileSizeKb = 0;

        Cubelet cubelet = cubeletRepository.findOne(dataAccessProduct.getId());
        // ignore missing for now - selected from screen usually
        if (cubelet != null)
        {
            dataAccessJob.addCubelet(cubelet);
            fileSizeKb = cubelet.getFilesize();
        }
        else
        {
            logger.error(CasdaDataAccessEvents.E100.messageBuilder().add("cubelet")
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
     * @param dataAccessJob
     *            the DataAccessJob
     * @return a UWSJob representation of the given job
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    public UWSJob getJobStatus(DataAccessJob dataAccessJob) throws ResourceNotFoundException
    {
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
        	List<Map<FileType, Integer[]>> paging = dataAccessService.getPaging(dataAccessJob.getRequestId(), false);
        	for(int pageNum = 0; pageNum < paging.size(); pageNum++)
        	{

            	Collection<DownloadFile> downloadFiles = 
            			dataAccessService.getPageOfFiles(paging.get(pageNum), dataAccessJob);
            	
            	for(DownloadFile file : downloadFiles)
            	{
            		switch(file.getFileType())
            		{
            			case IMAGE_CUBE:
                            results.add(new Result(DataAccessProductType.cube + "-" + file.getId(), XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                    		dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), 
                                    		file.getFileId()),false));
                            results.add(new Result(DataAccessProductType.cube + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE, fileDownloadBaseUrl + "/" + 
                                    		DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob
                                    				.getDownloadMode(), dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case MEASUREMENT_SET:
                            results.add(new Result(
                                            DataAccessProductType.visibility + "-"
                                                    + file.getId(),
                                            XLINK_SIMPLE_TYPE,
                                            fileDownloadBaseUrl + "/"
                                                    + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                            dataAccessJob.getRequestId(), file.getFileId()),
                                            false));
                            results.add(new Result(DataAccessProductType.visibility + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/"
                                            + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                                    dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case ENCAPSULATION_FILE:
                            results.add(new Result(
                                            DataAccessProductType.encap + "-"
                                                    + file.getId(),
                                            XLINK_SIMPLE_TYPE,
                                            fileDownloadBaseUrl + "/"
                                                    + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                            dataAccessJob.getRequestId(), file.getFileId()),
                                            false));
                            results.add(new Result(DataAccessProductType.encap + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/"
                                            + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                                    dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case EVALUATION_FILE:
                            results.add(new Result(
                                            DataAccessProductType.encap + "-"
                                                    + file.getId(),
                                            XLINK_SIMPLE_TYPE,
                                            fileDownloadBaseUrl + "/"
                                                    + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                            dataAccessJob.getRequestId(), file.getFileId()),
                                            false));
                            results.add(new Result(DataAccessProductType.encap + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/"
                                            + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                                    dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
                            break;
            			case SPECTRUM:
                            results.add(
                                    new Result(
                                            DataAccessProductType.spectrum + "-"
                                                    + file.getId(),
                                            XLINK_SIMPLE_TYPE,
                                            fileDownloadBaseUrl + "/"
                                                    + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                            dataAccessJob.getRequestId(), file.getFileId()),
                                            false));
                            results.add(new Result(DataAccessProductType.spectrum + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/"
                                            + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                                    dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case MOMENT_MAP:
                            results.add(
                                    new Result(
                                            DataAccessProductType.moment_map + "-"
                                                    + file.getId(),
                                            XLINK_SIMPLE_TYPE,
                                            fileDownloadBaseUrl + "/"
                                                    + DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(),
                                                            dataAccessJob.getRequestId(), file.getFileId()),
                                            false));
                            results.add(new Result(DataAccessProductType.moment_map + "-" + file.getId() + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/"
                                            + DataAccessUtil.getRelativeLinkForFileChecksum(dataAccessJob.getDownloadMode(),
                                                    dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case CUBELET:
                                results.add(
                                        new Result(
                                                DataAccessProductType.cubelet + "-"
                                                        + file.getId(),
                                                XLINK_SIMPLE_TYPE,
                                                fileDownloadBaseUrl + "/"
                                                        + DataAccessUtil.getRelativeLinkForFile(
                                                        		dataAccessJob.getDownloadMode(),
                                                                dataAccessJob.getRequestId(), file.getFileId()),
                                                false));
                                results.add(new Result(DataAccessProductType.cubelet + "-" + file.getId() + ".checksum",
                                        XLINK_SIMPLE_TYPE,
                                        fileDownloadBaseUrl + "/"
                                                + DataAccessUtil.getRelativeLinkForFileChecksum(
                                                		dataAccessJob.getDownloadMode(),
                                                        dataAccessJob.getRequestId(), file.getFileId()),
                                        false));
                				break;
            			case GENERATED_SPECTRUM:
                            String specId = "spectrum-" + file.getId() + "-image_cube" + 
                            			((GeneratedFileDescriptor)file).getImageCubeId();
                            results.add(new Result(specId, XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                            dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
                            results.add(new Result(specId + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFileChecksum(
                                            dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case IMAGE_CUTOUT:
                            String cutoutId = "cutout-" + file.getId();
                            results.add(new Result(cutoutId, XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                            dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
                            results.add(new Result(cutoutId + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFileChecksum(
                                            dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getFileId()),
                                    false));
            				break;
            			case ERROR:
                            String error_id = String.format("error-%02d", file.getId());
                            results.add(new Result(error_id, XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFile(
                                    dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getId()+".txt"),
                                    false));
                            results.add(new Result(error_id + ".checksum",
                                    XLINK_SIMPLE_TYPE,
                                    fileDownloadBaseUrl + "/" + DataAccessUtil.getRelativeLinkForFileChecksum(
                                    dataAccessJob.getDownloadMode(), dataAccessJob.getRequestId(), file.getId()+".txt"),
                                    false));
            				break;
            			default:
            				break;
            		}
            	}
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
