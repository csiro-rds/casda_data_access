package au.csiro.casda.access.soda;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.wordnik.swagger.annotations.Api;

import au.csiro.casda.access.BadRequestException;
import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.JobDto;
import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNoLongerAvailableException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.SystemStatus;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import uws.UWSException;
import uws.job.ExecutionPhase;
import uws.job.JobList;
import uws.job.UWSJob;
import uws.job.serializer.XMLSerializer;
import uws.job.user.JobOwner;

/**
 * Controller for the SODA 'async' and 'sync' requests.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Api(value = "Server Operations for Data Access", description = "ASYNC end-points")
@RestController
/*
 * WARNING: Do not use a @RequestMapping annotation to set the base path as it is specified in the
 * ACCESS_DATA_ASYNC_BASE_PATH constant (used in various places in the code).
 */
public class AccessDataController
{
    private static final int SODA_SYNC_REQUEST_GRACE_PERIOD_MINUTES = 10;

    /**
     * Convert a ZonedDateTime to an XMLGregorianCalendar
     * 
     * @param zonedDateTime
     *            a ZonedDateTime
     * @return an XMLGregorianCalendar
     */
    public static XMLGregorianCalendar convertZonedDateTimeToXMLGregorianCalendar(ZonedDateTime zonedDateTime)
    {
        try
        {
            DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
            GregorianCalendar upSinceDate = GregorianCalendar.from(zonedDateTime);
            return datatypeFactory.newXMLGregorianCalendar(upSinceDate);
        }
        catch (DatatypeConfigurationException e)
        {
            logger.error("Exception creating XMLGregorianCalendar for availability", e);
            return null;
        }
    }

    private static enum UwsAction
    {
        RUN, ABORT
    };

    /**
     * The base path for this controller.
     */
    public static final String ACCESS_DATA_BASE_PATH = "/data";

    /**
     * The base path for the availability end-point.
     */
    public static final String ACCESS_DATA_AVAILABILITY_BASE_PATH = ACCESS_DATA_BASE_PATH + "/availability";

    /**
     * The base path for the capability end-point.
     */
    public static final String ACCESS_DATA_CAPABILITIES_BASE_PATH = ACCESS_DATA_BASE_PATH + "/capabilities";

    /**
     * The base path for the service descriptor returned in response to an empty sync query.
     */
    public static final String SODA_SERVICE_DESC_BASE_PATH = ACCESS_DATA_BASE_PATH + "/servicedesc";

    /**
     * The base path for the async end-point.
     */
    public static final String ACCESS_DATA_ASYNC_BASE_PATH = ACCESS_DATA_BASE_PATH + "/async";

    /**
     * The base path for the sync end-point.
     */
    public static final String ACCESS_DATA_SYNC_BASE_PATH = ACCESS_DATA_BASE_PATH + "/sync";
    
    /**
     * The base path for the sync end-point.
     */
    public static final String ACCESS_DATA_SYNC_BASE_PATH_PAWSEY = ACCESS_DATA_SYNC_BASE_PATH + "/pawsey";

    private static Logger logger = LoggerFactory.getLogger(AccessDataController.class);

    private HealthEndpoint healthEndpoint;

    private SystemStatus systemStatus;

    private final DataAccessService dataAccessService;

    private final AccessJobManager accessJobManager;

    private final DataAccessJobRepository dataAccessJobRepository;

    private String applicationBaseUrl;

    private final String dataLinkAccessSecretKey;

    private int cancelledJobHoursToExpiry;

    private final long pollTimeForSodaSyncJobsMillis;

    private final long timeoutForSodaSyncJobsMillis;

    private final long sizeLimitForSodaSyncJobsKb;

    private static final String ID_PARAM = "id";

    /**
     * Create a new AccessDataAsyncController instance.
     * 
     * @param healthEndpoint
     *            the HealEndpoint used to obtain information about system health for the availabilities end-point
     * @param systemStatus
     *            the SystemStatus object used to obtain information about system health for the availabilities
     *            end-point
     * @param dataAccessService
     *            The service instance for accessing the data.
     * @param accessJobManager
     *            the access job manager for interacting with the UWS job queue
     * @param dataAccessJobRepository
     *            the DataAccessJobRepository used for interacting with persistent DataAccessJobs
     * @param applicationBaseUrl
     *            the application base URL
     * @param dataLinkAccessSecretKey
     *            the AES secret key used to decrypt an authorised ID token
     * @param cancelledJobHoursToExpiry
     *            the number of hours before a cancelled job becomes expired
     * @param pollTimeForSodaSyncJobsMillis
     *            how often to poll for SODA sync jobs to see if the job has completed
     * @param timeoutForSodaSyncJobsMillis
     *            maximum timeout for SODA sync jobs to complete
     * @param sizeLimitForSodaSyncJobsKb
     *            size limit for SODA sync download requests
     */
    @Autowired
    public AccessDataController(HealthEndpoint healthEndpoint, SystemStatus systemStatus,
            DataAccessService dataAccessService, AccessJobManager accessJobManager,
            DataAccessJobRepository dataAccessJobRepository,
            @Value("${application.base.url}") String applicationBaseUrl,
            @Value("${siap.shared.secret.key}") String dataLinkAccessSecretKey,
            @Value("${hours.to.expiry.default}") int cancelledJobHoursToExpiry,
            @Value("${soda.sync.job.poll.time.millis}") long pollTimeForSodaSyncJobsMillis,
            @Value("${soda.sync.job.timeout.millis}") long timeoutForSodaSyncJobsMillis,
            @Value("${soda.sync.job.size.limit.kb}") long sizeLimitForSodaSyncJobsKb)
    {
        this.healthEndpoint = healthEndpoint;
        this.systemStatus = systemStatus;
        this.dataAccessService = dataAccessService;
        this.accessJobManager = accessJobManager;
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.applicationBaseUrl = applicationBaseUrl;
        this.dataLinkAccessSecretKey = dataLinkAccessSecretKey;
        this.cancelledJobHoursToExpiry = cancelledJobHoursToExpiry;
        this.pollTimeForSodaSyncJobsMillis = pollTimeForSodaSyncJobsMillis;
        this.timeoutForSodaSyncJobsMillis = timeoutForSodaSyncJobsMillis;
        this.sizeLimitForSodaSyncJobsKb = sizeLimitForSodaSyncJobsKb;
    }

    /**
     * Returns an XML response for the Access Data services' availability in accordance with the IVOA VOSI
     * specification.
     * 
     * @return a ModelAndView
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_AVAILABILITY_BASE_PATH, //
            produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ModelAndView getAvailability()
    {
        ModelAndView mav = new ModelAndView();
        mav.setViewName(ACCESS_DATA_AVAILABILITY_BASE_PATH + ".xml");
        Status status = healthEndpoint.invoke().getStatus();
        mav.getModel().put("available", Status.UP.equals(status));
        mav.getModel().put("note", Status.UP.equals(status) ? "" : "Health check FAILED");
        mav.getModel().put("upSince", convertZonedDateTimeToXMLGregorianCalendar(systemStatus.getUpSince()));
        return mav;
    }

    /**
     * Returns an XML response for the Access Data services' capabilities in accordance with the IVOA VOSI
     * specification.
     * 
     * @return a ModelAndView
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_CAPABILITIES_BASE_PATH, //
            produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ModelAndView getCapabilities()
    {
        ModelAndView mav = new ModelAndView();
        mav.setViewName(ACCESS_DATA_CAPABILITIES_BASE_PATH + ".xml");
        mav.getModel().put("capabilitiesURL", this.applicationBaseUrl + ACCESS_DATA_CAPABILITIES_BASE_PATH);
        mav.getModel().put("availabilityURL", this.applicationBaseUrl + ACCESS_DATA_AVAILABILITY_BASE_PATH);
        mav.getModel().put("asyncURL", this.applicationBaseUrl + ACCESS_DATA_ASYNC_BASE_PATH);
        mav.getModel().put("syncURL", this.applicationBaseUrl + ACCESS_DATA_SYNC_BASE_PATH);
        return mav;
    }

    /**
     * Returns an XML response that shows the SODA async job list. The list will always be empty because we do not
     * support job owners.
     * 
     * @return an (XML) String ResponseEntity representing the job list
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH, //
            produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ResponseEntity<String> getJobList()
    {
        XMLSerializer serializer = new XMLSerializer();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_XML);
        try
        {
            return new ResponseEntity<String>(serializer.getJobList(new JobList("CASDA SODA ASYNC"), true), httpHeaders,
                    HttpStatus.OK);
        }
        catch (UWSException e)
        {
            // unexpected
            throw new RuntimeException(e);
        }
    }

    /**
     * Web end point for performing a sync access data request according to the SODA specification.
     * 
     * The request requires a single id parameter that is used to specify the data product being requested. The id is in
     * our authenticated id token format (see {@link RequestToken}). This method will return the data in the data
     * product through the HttpServletResponse. This method requires that the data product be 'online' - if it is not
     * then a ResourceNotFound exception will be thrown.
     * 
     * Filtering parameters as defined in the SODA specification are currently not supported for SODA sync
     * requests.
     * 
     * @param request
     *            the request
     * @param response
     *            the response
     * @return a ModelAndView, or null if a download was processed.
     * 
     * @throws ResourceNotFoundException
     *             if the requested resource could not be found
     * @throws BadRequestException
     *             if the request was malformed
     * @throws InterruptedException
     *             if the method was interrupted
     */
    @RequestMapping(method = { RequestMethod.POST, RequestMethod.GET, RequestMethod.HEAD },
            value = ACCESS_DATA_SYNC_BASE_PATH, produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ModelAndView syncDownloadDataProduct(HttpServletRequest request, HttpServletResponse response)
            throws ResourceNotFoundException, BadRequestException, InterruptedException
    {
        return syncDownload(request, response);
    }
    
    /**
     * Web end point for performing a sync access data request according to the SODA specification.
     * 
     * The request requires a single id parameter that is used to specify the data product being requested. The id is in
     * our authenticated id token format (see {@link RequestToken}). This method will return the data in the data
     * product through the HttpServletResponse. This method requires that the data product be 'online' - if it is not
     * then a ResourceNotFound exception will be thrown.
     * 
     * Filtering parameters as defined in the SODA specification are currently not supported for SODA sync
     * requests.
     * 
     * @param request
     *            the request
     * @param response
     *            the response
     * @return a ModelAndView, or null if a download was processed.
     * 
     * @throws ResourceNotFoundException
     *             if the requested resource could not be found
     * @throws BadRequestException
     *             if the request was malformed
     * @throws InterruptedException
     *             if the method was interrupted
     */
    @RequestMapping(method = { RequestMethod.POST, RequestMethod.GET, RequestMethod.HEAD },
            value = ACCESS_DATA_SYNC_BASE_PATH_PAWSEY, produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ModelAndView syncDownloadDataProductFromPawsey(HttpServletRequest request, 
            HttpServletResponse response) throws ResourceNotFoundException, BadRequestException, InterruptedException
    {
        return syncDownload(request, response);
    }
    
    /**
     * This method will return the data in the data
     * product through the HttpServletResponse. This method requires that the data product be 'online' - if it is not
     * then a ResourceNotFound exception will be thrown.
     * 
     * Filtering parameters as defined in the SODA specification are currently not supported for SODA sync
     * requests.
     * 
     * @param request
     *            the request
     * @param response
     *            the response
     * @return a ModelAndView, or null if a download was processed.
     * 
     * @throws ResourceNotFoundException
     *             if the requested resource could not be found
     * @throws BadRequestException
     *             if the request was malformed
     * @throws InterruptedException
     *             if the method was interrupted
     */
    private ModelAndView syncDownload(HttpServletRequest request, HttpServletResponse response) 
            throws ResourceNotFoundException, BadRequestException, InterruptedException
    {

        logger.info("Hit the controller for '{} {}'", request.getMethod().toUpperCase(), ACCESS_DATA_SYNC_BASE_PATH);

        Map<String, String[]> params = DataAccessUtil.buildParamsMap(request.getParameterMap());

        if (params.isEmpty())
        {
            ModelAndView mav = new ModelAndView();
            mav.setViewName(SODA_SERVICE_DESC_BASE_PATH + ".xml");
            mav.getModel().put("syncURL", this.applicationBaseUrl + ACCESS_DATA_SYNC_BASE_PATH);
            return mav;
        }
        if (!params.containsKey(ID_PARAM))
        {
            throw new BadRequestException("Request requires a single 'id' parameter");
        }
        if (params.get(ID_PARAM).length > 1)
        {
            throw new BadRequestException("Request requires a single 'id' parameter");
        }
        RequestToken token;
        try
        {
            token = new RequestToken(params.get(ID_PARAM)[0], dataLinkAccessSecretKey);
        }
        catch (IllegalArgumentException ex)
        {
            throw new BadRequestException("Request 'id' parameter is invalid");
        }

        JobDto jobDto = new JobDto();
        if(RequestToken.INTERNAL_DOWNLOAD.equals(token.getDownloadMode()))
        {
            jobDto.setDownloadMode(CasdaDownloadMode.SODA_SYNC_PAWSEY);
        }
        else
        {
            jobDto.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
        }
        jobDto.setIds(new String[] { token.getId() });
        jobDto.setUserIdent(token.getUserId());
        jobDto.setUserLoginSystem(token.getLoginSystem());
        
        List<String> errors =
                validateParams(params, false, jobDto.getUserIdent(), jobDto.getUserLoginSystem());

        if (!errors.isEmpty())
        {
            throw new BadRequestException(StringUtils.join(errors, ";"));
        }

        jobDto.setParams(params);


        DataAccessJob dataAccessJob = accessJobManager.createDataAccessJob(jobDto, sizeLimitForSodaSyncJobsKb, true);

        Path path = null;
        boolean skipCacheCheck = false;
        /*
         * if it's in the cache, we want to schedule a data access job so that we can reclaim the cached files and more
         * accurately keep track of what files in the cache have been used most recently
         */
        try
        {
            /*
             * This will only queue a sync job if the files are in the cache. If not, it will throw a
             * ScheduleJobException
             */
            try
            {
                accessJobManager.scheduleJob(dataAccessJob.getRequestId());
            }
            catch (ResourceNotFoundException | ResourceIllegalStateException e)
            {
                /*
                 * Since we just created it and are just scheduling it this would be very unexpected.
                 */
                throw new RuntimeException(e);
            }

            long start = System.currentTimeMillis();
            while (true)
            {
                if (AccessJobManager.UWS_JOB_FINISHED_EXECUTION_PHASES
                        .contains(accessJobManager.getJobStatus(dataAccessJob).getPhase()))
                {
                    if (ExecutionPhase.COMPLETED == accessJobManager.getJobStatus(dataAccessJob)
                            .getPhase())
                    {
                        List<Map<FileType, Integer[]>> paging = 
                        		dataAccessService.getPaging(dataAccessJob.getRequestId(), false);
                        Collection<DownloadFile> files = dataAccessService.getPageOfFiles(paging.get(0), dataAccessJob);
                        if (files.iterator().hasNext())
                        {
                            path = dataAccessService.findFile(dataAccessJob, files.iterator().next().getFilename())
                                    .toPath();
                        }
                    }
                    // If wasn't COMPLETED then we will treat as a timeout and try and find it some other way
                    break;
                }
                else if (start + timeoutForSodaSyncJobsMillis < System.currentTimeMillis())
                {
                    try
                    {
                        accessJobManager.cancelJob(dataAccessJob.getRequestId(), DateTime.now(DateTimeZone.UTC));
                    }
                    catch (ResourceIllegalStateException | ResourceNotFoundException e)
                    {
                        ExecutionPhase phase = accessJobManager.getJobStatus(dataAccessJob).getPhase();
                        if (!(AccessJobManager.UWS_JOB_FINISHED_EXECUTION_PHASES.contains(phase)))
                        {
                            throw new RuntimeException(
                                    "Could not cancel job created to refresh SODA SYNC files in request after timeout. "
                                            + "Job with requestId '" + dataAccessJob.getRequestId()
                                            + "' is still in phase '" + phase + "'");
                        }
                    }
                    throw new TimeoutException("Job took too long to complete for a sync request");
                }
                Thread.sleep(pollTimeForSodaSyncJobsMillis);
            }
        }
        catch (IOException e)
        {
            /*
             * Indicates that the file was in the cache but could otherwise not be read. There's nothing we can do about
             * this.
             */
            throw new RuntimeException(e);
        }
        catch(ScheduleJobException e)
        {
            logger.info(
                    "ServiceUnavailable: " + e.getMessage());
            dataAccessService.returnErrorFile("ServiceUnavailable: " + e.getMessage(), response, dataAccessJob, false);
            return null;
        }
        catch (TimeoutException e)
        {
            logger.info(
                    "Unable to access the files via the cache, will try to access the files by other means. Reason: "
                            + e.getMessage());
            // don't return here to let it look for an alternative file location
        }

        if (path == null)
        {
            skipCacheCheck = true;
            try
            {
                path = dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(token.getId()));
            }
            catch (ResourceNotFoundException | ServiceCallException e)
            {
                logger.info("Unable to access the file via NGAS. Reason: " + e.getMessage());
            }
        }
        if (path == null)
        {
            /*
             * Update the DataAccessJob to mark it as failed as a way of recording that this request couldn't be
             * serviced.
             */
            dataAccessService.markRequestError(dataAccessJob.getRequestId(), DateTime.now(DateTimeZone.UTC));
            throw new ResourceNotFoundException("File is not available");
        }
        else
        {
            /*
             * Update the DataAccessJob regardless of its state (it could be COMPLETED, ERROR, or ABORTED) and mark it
             * as completed as a way of recording that this request was successfully serviced (most likely by the
             * presence of files in NGAS rather than the cache). We use an expiryTime of 'now' with a small grace period
             * because these files will be used immediately. Note: changing the DataAccessJob's expiry time will not
             * overwrite the underlying file's expiry time however.
             */
            dataAccessService.markRequestCompleted(dataAccessJob.getRequestId(),
                    DateTime.now(DateTimeZone.UTC).plusMinutes(SODA_SYNC_REQUEST_GRACE_PERIOD_MINUTES));
        }

        logger.info("{}", CasdaDataAccessEvents.E040.messageBuilder().add(dataAccessJob.getRequestId())
                .add(path.toFile().getName()));

        // Suppress the content if this is a HEAD request 
        boolean headersOnly = RequestMethod.HEAD.toString().equals(request.getMethod());
        dataAccessService.downloadFile(dataAccessJob, path.toFile().getName(), response, skipCacheCheck, headersOnly);
        
        return null;
    
    }

    /**
     * Web end point for creating a new async access data request. This will register the request, issue a unique id (a
     * UUID) and then create (but not schedule) a job to retrieve and package the data ready for use.
     * 
     * @param request
     *            the original request. This must contain an 'id' param (case insensitive) with an base64-encoded
     *            encrypted token containing details about the requested data product and the user requesting the data.
     *            This may also include fitering params, currently only supports "pos"
     * @return A response with code 303 and the UUID issued for the request. The Location header also contains a URL to
     *         query the created job.
     */
    @RequestMapping(//
            method = { RequestMethod.POST }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH, //
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public @ResponseBody ResponseEntity<String> createJob(HttpServletRequest request)
    {
        Map<String, String[]> params = DataAccessUtil.buildParamsMap(request.getParameterMap());

        logger.info("Hit the controller for 'POST {}'", ACCESS_DATA_ASYNC_BASE_PATH);

        List<String> errorList = validateParams(params, true, null, null);

        if (!errorList.isEmpty())
        {
            throw new BadRequestException(StringUtils.join(errorList, ";"));
        }
        
        RequestToken token = new RequestToken(params.get(ID_PARAM)[0], dataLinkAccessSecretKey);
        JobDto jobDetails = new JobDto();
        jobDetails.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_WEB);
        //loops through each id to check if any are pawsey, if so the job will go through as a pawsey job
        for(String id : params.get(ID_PARAM))
        {
        	token = new RequestToken(id, dataLinkAccessSecretKey);
        	//one of the id tokens is for a pawsey download
        	if(RequestToken.INTERNAL_DOWNLOAD.equals(token.getDownloadMode()))
        	{
        		jobDetails.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_PAWSEY);
        		break;
        	}
        }

        String userId = token.getUserId();
        String loginSystem = token.getLoginSystem();

        jobDetails.setDownloadFormat(null); 
        jobDetails.setIds(null); // ids are stored in the params until the async job is started
        jobDetails.setParams(params);
        jobDetails.setUserEmail(null); // not available
        jobDetails.setUserIdent(userId);
        jobDetails.setUserLoginSystem(loginSystem);
        jobDetails.setUserName(null); // not available
        jobDetails.setJobType(token.getDownloadMode());
        
        DataAccessJob newJob = accessJobManager.createDataAccessJob(jobDetails, null, false);

        return redirectToJobPage(newJob.getRequestId(), newJob.getRequestId());
    }

    /**
     * Returns an XML response that details the specified job.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an (XML) String ResponseEntity
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}", //
            produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ResponseEntity<String> getJob(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        /*
         * Check job exists and is not expired
         */
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        XMLSerializer serializer = new XMLSerializer();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_XML);
        return new ResponseEntity<String>(serializer.getJob(job, true), httpHeaders, HttpStatus.OK);
    }

    /**
     * Returns the phase (text/plain) of the Job
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of job's phase
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * 
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/phase", method = RequestMethod.GET,
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public String getAsyncJobPhase(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/phase'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        return job.getPhase().name();
    }

    /**
     * Returns the owner (text/plain) of the Job
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of job owner
     * @throws ResourceNotFoundException
     *             UWSJob job = getUWSJobForJobId(jobId);
     * 
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/owner", method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_JSON_VALUE })
    public @ResponseBody ResponseEntity<JobOwner> getAsyncJobOwner(@PathVariable() String jobId)
            throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/owner'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);
        // once we fully implement authentication model
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        return new ResponseEntity<JobOwner>(job.getOwner(), HttpStatus.OK);
    }

    /**
     * Returns the executionduration (text/plain).
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of executionduration
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * 
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/executionduration", method = RequestMethod.GET,
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public String getAsyncJobExecutionduration(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/executionduration'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        if (job.getExecutionDuration() < 0)
        {
            return "";
        }
        return Long.toString(job.getExecutionDuration());
    }

    /**
     * Returns quote (text/plain) of the UWS job
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of quote
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * 
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/quote", method = RequestMethod.GET,
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public String getAsyncJobQuote(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/quote'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        return Long.toString(job.getQuote());
    }

    /**
     * Returns the destruction time (text/plain)
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of destruction time
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/destruction", method = RequestMethod.GET,
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public String getAsyncJobDestruction(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/destruction'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        if (job.getDestructionTime() == null)
        {
            return "";
        }

        // This can be replaced with
        // return ISO8601Format.format(job.getDestructionTime())
        // When UWS 4.1 is available
        return Utils.formatDateToISO8601(job.getDestructionTime());
    }

    /**
     * Returns any error message associated with (job-id) (text/plain)
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a string of error summary
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/error", method = RequestMethod.GET,
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public String getAsyncJobError(@PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/error'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        return job.getErrorSummary() != null ? job.getErrorSummary().toString() : "";
    }

    /**
     * Returns the &lt;results&gt; element from the full XML job status (application/xml)
     * @param response 
     *            the https servlet response
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an (XML) String ResponseEntity
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/results", method = RequestMethod.GET,
            produces = { MediaType.APPLICATION_XML_VALUE })
    public @ResponseBody ResponseEntity<String> getAsyncJobResults(
            HttpServletResponse response, @PathVariable() String jobId) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/results'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);
        UWSJob job = getNonExpiredUWSJobForJobId(jobId);
        XMLSerializer serializer = new XMLSerializer();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_XML);
        if(job.getNbResults() == 0)
        {
            DataAccessJob daj = dataAccessService.getExistingJob(jobId);
            String message = job.getErrorSummary() != null ? job.getErrorSummary().toString()
                    : "UsageError: the given combination of parameters returned no results.";
            dataAccessService.returnErrorFile(message, response, daj, true);
            throw new ResourceNotFoundException(message);
        }
        return new ResponseEntity<String>(serializer.getResults(job, true), httpHeaders, HttpStatus.OK);
    }

    /**
     * Get UWSJob for job id
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return The UWSJob
     * @throws ResourceNotFoundException
     *             Thrown when resource is not found
     */
    private UWSJob getNonExpiredUWSJobForJobId(String jobId) throws ResourceNotFoundException
    {
        DataAccessJob dataAccessJob = retrieveNonExpiredJob(jobId);
        return accessJobManager.getJobStatus(dataAccessJob);
    }

    /**
     * Sets the phase of the given job. Currently the only legal 'phase' value to set is 'RUN'. If the job is PENDING
     * then it will become QUEUED, otherwise a ResourceIllegalStateException will be thrown.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param request
     *            the HttpServletRequest
     * @return an empty String ResponseEntity redirecting to the job page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     * @throws ScheduleJobException
     *             if the scheduling of the job failed
     */
    @RequestMapping(method = { RequestMethod.POST }, value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/phase")
    public @ResponseBody ResponseEntity<String> setJobPhase(@PathVariable() String jobId, HttpServletRequest request)
            throws ResourceNotFoundException, ResourceIllegalStateException, ScheduleJobException
    {
        logger.info("Hit the controller for 'POST {}/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        /*
         * Check job exists and is not expired
         */
        DataAccessJob dataAccessJob = retrieveNonExpiredJob(jobId);

        Map<String, String[]> params = DataAccessUtil.buildParamsMap(request.getParameterMap());
        String[] phaseValues = params.get("phase");
        if (ArrayUtils.isEmpty(phaseValues))
        {
            throw new BadRequestException("phase is a required parameter");
        }
        if (phaseValues.length > 1)
        {
            throw new BadRequestException("only a single phase parameter is allowed");
        }
        UwsAction action;
        try
        {
            action = UwsAction.valueOf(phaseValues[0]);
        }
        catch (IllegalArgumentException ex)
        {
            throw new BadRequestException(
                    "phase parameter must have value " + StringUtils.join(UwsAction.values(), " or "));
        }
        
        ExecutionPhase phase = accessJobManager.getJobStatus(dataAccessJob).getPhase();
        
        switch (action)
        {
        case RUN:
            if(phase == ExecutionPhase.PENDING)
            {
                String[] authenticatedIdTokens = dataAccessJob.getParamMap().get(ParamKeyWhitelist.ID.name());
                String[] ids = new String[authenticatedIdTokens.length];
                int index = 0;
                if (ArrayUtils.isNotEmpty(authenticatedIdTokens))
                {
                    for (String authenticatedIdToken : authenticatedIdTokens)
                    {
                        RequestToken token = new RequestToken(authenticatedIdToken, dataLinkAccessSecretKey);
                        DataAccessDataProduct dataAccessDataProduct = new DataAccessDataProduct(token.getId());
                        ids[index] = dataAccessDataProduct.getDataProductId();
                        index++;
                    }
                }
                accessJobManager.prepareAsyncJobToStart(jobId, ids, null);
                accessJobManager.scheduleJob(jobId);
            }
            else
            {
                throw new BadRequestException("Only Jobs which are PENDING can be run");
            }

            break;
        case ABORT:
            if(phase == ExecutionPhase.PENDING 
                                || phase == ExecutionPhase.EXECUTING || phase == ExecutionPhase.QUEUED)
            {
                accessJobManager.cancelJob(jobId, 
                        new DateTime(DateTimeZone.UTC).plusHours(this.cancelledJobHoursToExpiry));
            }
            else
            {
                throw new BadRequestException("Only Jobs which are PENDING, QUEUED or EXECUTING can be aborted");
            }
            
            break;
        default:
            throw new IllegalStateException("UwsAction value of '" + action + "' not handled.");
        }

        return redirectToJobPage(jobId, "");
    }

    /**
     * Deletes a job.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an empty String ResponseEntity redirecting to the job page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    @RequestMapping(method = { RequestMethod.DELETE }, value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}")
    public @ResponseBody ResponseEntity<String> deleteJob(@PathVariable() String jobId)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        logger.info("Hit the controller for 'DELETE {}/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        return doDeleteJob(jobId);
    }

    /**
     * Deletes a job.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param request
     *            the HttpServletRequest
     * @return an empty String ResponseEntity redirecting to the job page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    @RequestMapping(method = { RequestMethod.POST }, value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}")
    public @ResponseBody ResponseEntity<String> deleteJobViaAction(@PathVariable() String jobId,
            HttpServletRequest request) throws ResourceNotFoundException, ResourceIllegalStateException
    {
        logger.info("Hit the controller for 'POST {}/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        Map<String, String[]> params = DataAccessUtil.buildParamsMap(request.getParameterMap());
        String[] actionValues = params.get("action");
        if (ArrayUtils.isEmpty(actionValues))
        {
            throw new BadRequestException("action is a required parameter");
        }
        if (actionValues.length > 1)
        {
            throw new BadRequestException("only a single action parameter is allowed");
        }
        if (!"DELETE".equals(actionValues[0]))
        {
            throw new BadRequestException("action parameter must have value DELETE");
        }
        return doDeleteJob(jobId);
    }

    private ResponseEntity<String> doDeleteJob(String jobId)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        /*
         * Check job exists and is not expired
         */
        retrieveNonExpiredJob(jobId);

        accessJobManager.cancelJob(jobId, new DateTime(DateTimeZone.UTC)); // No grace period for a deleted job

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(
                ServletUriComponentsBuilder.fromCurrentContextPath().path(ACCESS_DATA_ASYNC_BASE_PATH).build().toUri());
        return new ResponseEntity<String>(httpHeaders, HttpStatus.SEE_OTHER);
    }

    /**
     * A do nothing implementation of the destruction UWS endpoint.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an empty String ResponseEntity redirecting to the job page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(method = { RequestMethod.POST }, value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/destruction")
    public @ResponseBody ResponseEntity<String> updateJobDestructionTime(@PathVariable() String jobId)
            throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'POST {}/{}/destruction'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        retrieveNonExpiredJob(jobId);

        return redirectToJobPage(jobId);
    }

    /**
     * A do nothing implementation of the executionduration UWS endpoint.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an empty String ResponseEntity redirecting to the job page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(method = { RequestMethod.POST }, value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/executionduration")
    public @ResponseBody ResponseEntity<String> updateJobExecutionDuration(@PathVariable() String jobId)
            throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'POST {}/{}/executionduration'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        retrieveNonExpiredJob(jobId);

        return redirectToJobPage(jobId);
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for retrieving parameters.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return a String ResponseEntity representing the job parameters.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters", //
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public @ResponseBody ResponseEntity<String> getParameters(@PathVariable() String jobId)
            throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/parameters'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);

        UWSJob uwsJob = accessJobManager.getJobStatus(dataAccessJob);
        XMLSerializer serializer = new XMLSerializer();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_XML);

        return new ResponseEntity<String>(serializer.getAdditionalParameters(uwsJob, true), httpHeaders, HttpStatus.OK);
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for setting parameters.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param request
     *            the web request being processed.
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.POST }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters")
    public @ResponseBody ResponseEntity<String> updateParameters(@PathVariable() String jobId,
            HttpServletRequest request) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'POST {}/{}/parameters'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);

        Map<String, String[]> params = DataAccessUtil.buildParamsMap(request.getParameterMap());
        List<String> errors =
                validateParams(params, false, dataAccessJob.getUserIdent(), dataAccessJob.getUserLoginSystem());

        if (!errors.isEmpty())
        {
            throw new BadRequestException(StringUtils.join(errors, ";"));
        }

        dataAccessJob.getParamMap().addAll(params);

        dataAccessJobRepository.save(dataAccessJob);

        return redirectToJobPage(jobId, "");
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for deleting parameters.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.DELETE }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters")
    public @ResponseBody ResponseEntity<String> deleteParameters(@PathVariable() String jobId)
            throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'DELETE {}/{}/parameters'", ACCESS_DATA_ASYNC_BASE_PATH, jobId);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);

        ParamMap paramMap = dataAccessJob.getParamMap();
        Set<String> keys = new HashSet<>(paramMap.keySet());
        for (String key : keys)
        {
            paramMap.remove(key);
        }
        dataAccessJobRepository.save(dataAccessJob);

        return redirectToJobPage(jobId, "");
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for retrieving parameters.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param name
     *            The name or key of the parameter to be retrieved.
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.GET }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters/{name}", //
            produces = { MediaType.TEXT_PLAIN_VALUE })
    public @ResponseBody ResponseEntity<String> getNamedParameter(@PathVariable() String jobId,
            @PathVariable() String name) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'GET {}/{}/parameters/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId, name);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);

        UWSJob uwsJob = accessJobManager.getJobStatus(dataAccessJob);
        XMLSerializer serializer = new XMLSerializer();
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_XML);
        Object value = uwsJob.getParameter(name.toLowerCase());
        String valueString = String.valueOf(value);
        return new ResponseEntity<String>(value == null ? "" : 
            serializer.getAdditionalParameter(name.toUpperCase(), valueString, false), httpHeaders, HttpStatus.OK);
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for setting a named parameter.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param name
     *            The name or key of the parameter to be updated.
     * @param value
     *            The new value to be added to the parameter.
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.POST }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters/{name}")
    public @ResponseBody ResponseEntity<String> addValueToNamedParameter(@PathVariable() String jobId,
            @PathVariable() String name, @RequestParam(value = "value") String[] value) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'POST {}/{}/parameters/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId, name);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);

        Map<String, String[]> params = new HashMap<>();
        params.put(name.toLowerCase(), value);

        List<String> errors =
                validateParams(params, false, dataAccessJob.getUserIdent(), dataAccessJob.getUserLoginSystem());

        if (!errors.isEmpty())
        {
            throw new BadRequestException(StringUtils.join(errors, ";"));
        }

        dataAccessJob.getParamMap().add(name, value);
        dataAccessJobRepository.save(dataAccessJob);

        return redirectToJobPage(jobId, "");
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for deleting a named parameters.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param name
     *            The name of the parameter to be deleted.
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.DELETE }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters/{name}")
    public @ResponseBody ResponseEntity<String> deleteNamedParameter(@PathVariable() String jobId,
            @PathVariable() String name) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'DELETE {}/{}/parameters/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId, name);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);
        dataAccessJob.getParamMap().remove(name);
        dataAccessJobRepository.save(dataAccessJob);

        return redirectToJobPage(jobId, "");
    }

    /**
     * Implementation of the (optional) parameters UWS endpoint for deleting a specific value for a named parameter.
     * 
     * @param jobId
     *            a jobId (obtained when creating the job)
     * @param name
     *            The name of the parameter to be deleted.
     * @param value
     *            The value to be deleted.
     * @return an empty String ResponseEntity redirecting to the job page.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(//
            method = { RequestMethod.DELETE }, //
            value = ACCESS_DATA_ASYNC_BASE_PATH + "/{jobId}/parameters/{name}/{value}")
    public @ResponseBody ResponseEntity<String> deleteNamedParameterValue(@PathVariable() String jobId,
            @PathVariable() String name, @PathVariable() String value) throws ResourceNotFoundException
    {
        logger.info("Hit the controller for 'DELETE {}/{}/parameters/{}/{}'", ACCESS_DATA_ASYNC_BASE_PATH, jobId, name,
                value);

        DataAccessJob dataAccessJob = retrievePendingJob(jobId);
        dataAccessJob.getParamMap().remove(name, value);
        dataAccessJobRepository.save(dataAccessJob);

        return redirectToJobPage(jobId, "");
    }

    private DataAccessJob retrievePendingJob(String jobId) throws ResourceNotFoundException
    {
        DataAccessJob dataAccessJob = retrieveNonExpiredJob(jobId);
        UWSJob uwsJob = accessJobManager.getJobStatus(dataAccessJob);
        if (uwsJob.getPhase() != ExecutionPhase.PENDING)
        {
            throw new BadRequestException("Job with id '" + jobId + "' is not pending.");
        }
        return dataAccessJob;
    }

    private ResponseEntity<String> redirectToJobPage(String jobId)
    {
        return redirectToJobPage(jobId, null);
    }

    private ResponseEntity<String> redirectToJobPage(String jobId, String content)
    {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(ServletUriComponentsBuilder.fromCurrentContextPath().path(ACCESS_DATA_ASYNC_BASE_PATH)
                .path("/{id}").buildAndExpand(jobId).toUri());
        if (StringUtils.isBlank(content))
        {
            return new ResponseEntity<String>(httpHeaders, HttpStatus.SEE_OTHER);
        }
        else
        {
            return new ResponseEntity<String>(content, httpHeaders, HttpStatus.SEE_OTHER);
        }
    }

    private DataAccessJob retrieveNonExpiredJob(String jobId) throws ResourceNotFoundException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(jobId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("No job with id '" + jobId + "'");
        }

        if (dataAccessJob.isExpired())
        {
            throw new ResourceNoLongerAvailableException("Job with id '" + jobId + "' has expired");
        }

        return dataAccessJob;
    }

    private List<String> validateParams(Map<String, String[]> params, boolean idRequired, String userId,
            String loginSystem)
    {
        List<String> errorList = new ArrayList<>();
        if (idRequired && !params.containsKey(ID_PARAM))
        {
            errorList.add("id is a required parameter");
        }
        for (Entry<String, String[]> entry : params.entrySet())
        {
            if (ID_PARAM.equalsIgnoreCase(entry.getKey()))
            {
                errorList.addAll(validateIdParams(entry.getValue(), userId, loginSystem));
            }
            else if (!ParamMap.ParamKeyWhitelist.isAllowed(entry.getKey()))
            {
                errorList.add(String.format("UsageFault: %s is an invalid parameter", entry.getKey()));
            }
            else
            {
                switch (ParamMap.ParamKeyWhitelist.valueOf(entry.getKey().toUpperCase()))
                {
                case POS:
                    errorList.addAll(new PositionParamProcessor().validate(entry.getValue()));
                    break;
                case POL:
                    errorList.addAll(new PolParamProcessor().validate(entry.getValue()));
                    break;
                case COORD:
                    errorList.addAll(new CoordParamProcessor().validate(entry.getValue()));
                    break;
                case BAND:
                    errorList.addAll(new BandParamProcessor().validate(entry.getValue()));
                    break;
                case CIRCLE:
                    errorList.addAll(new CircleParamProcessor().validate(entry.getValue()));
                    break;
                case POLYGON:
                    errorList.addAll(new PolygonParamProcessor().validate(entry.getValue()));
                    break;
                case TIME:
                    errorList.addAll(new TimeParamProcessor().validate(entry.getValue()));
                    break;
                case FORMAT:
                    errorList.addAll(new FormatParamProcessor().validate(entry.getValue()));
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported parameter " + entry.getKey());
                }
            }
        }
        return errorList;
    }

    private List<String> validateIdParams(String[] authenticatedIdTokens, String userId, String loginSystem)
    {
        List<String> errorList = new ArrayList<>();
        int index = 0;
        for (String authenticatedIdToken : authenticatedIdTokens)
        {
            RequestToken token;
            DataAccessDataProduct dataAccessDataProduct;
            try
            {
                token = new RequestToken(authenticatedIdToken, dataLinkAccessSecretKey);
                dataAccessDataProduct = new DataAccessDataProduct(token.getId());

                if (index == 0 && userId == null && loginSystem == null)
                {
                    userId = token.getUserId();
                    loginSystem = token.getLoginSystem();
                }

                if (!token.getUserId().equals(userId) || !token.getLoginSystem().equals(loginSystem))
                {
                    errorList.add("id '" + authenticatedIdToken + "' is not valid");
                }
                else if (!EnumSet.of(DataAccessProductType.cube, DataAccessProductType.encap, DataAccessProductType.cubelet,
                		DataAccessProductType.visibility, DataAccessProductType.moment_map, DataAccessProductType.spectrum, 
                		DataAccessProductType.evaluation)
                        .contains(dataAccessDataProduct.getDataAccessProductType()))
                {
                    errorList.add(
                            "Unsupported data access product type " + dataAccessDataProduct.getDataAccessProductType());
                }
                else if (dataAccessService.findDataProduct(dataAccessDataProduct) == null)
                /*
                 * Ensure the data product exists (almost impossible, because it means a user has to correctly guess an
                 * authenticated token for a data product that doesn't exist)
                 */
                {
                    errorList.add("No resource found matching id '" + authenticatedIdToken + "'");
                }
            }
            catch (IllegalArgumentException ex)
            {
                errorList.add("id '" + authenticatedIdToken + "' is not valid");
            }
            index++;
        }
        return errorList;
    }

}
