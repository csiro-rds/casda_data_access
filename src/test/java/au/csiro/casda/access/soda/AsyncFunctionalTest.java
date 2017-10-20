package au.csiro.casda.access.soda;

import static org.hamcrest.junit.internal.ThrowableMessageMatcher.hasMessage;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.XMLUnit;
import org.hamcrest.junit.ExpectedException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.boot.actuate.endpoint.HealthEndpoint;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver;
import org.springframework.web.util.NestedServletException;
import org.xml.sax.SAXException;

import au.csiro.casda.AESTRule;
import au.csiro.casda.access.ResourceNoLongerAvailableException;
import au.csiro.casda.access.SystemStatus;
import au.csiro.casda.access.cache.CacheManager;
import au.csiro.casda.access.cache.CacheManagerInterface;
import au.csiro.casda.access.cache.DownloadManager;
import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.cache.Packager.Result;
import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CatalogueRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.EncapsulationFileRepository;
import au.csiro.casda.access.jpa.EvaluationFileRepository;
import au.csiro.casda.access.jpa.GeneratedSpectrumRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.jpa.ThumbnailRepository;
import au.csiro.casda.access.services.CasdaMailService;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.NgasService;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.access.uws.TestAccessUwsFactory;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import uws.service.file.LocalUWSFileManager;

/**
 * Test cases for AccessDataAsyncController.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class AsyncFunctionalTest
{
    @Mock
    private GenerateFileService generateFileService;

    @Rule
    public TemporaryFolder uwsDir = new TemporaryFolder();

    @Rule
    public TemporaryFolder cacheDir = new TemporaryFolder();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public AESTRule utcRule = new AESTRule();

    @Mock
    private EntityManagerFactory emf;

    @Mock
    private DataAccessJobRepository dataAccessJobRepository;

    @Mock
    private ImageCubeRepository imageCubeRepository;

    @Mock
    private CatalogueRepository catalogueRepository;

    @Mock
    private MeasurementSetRepository measurementSetRepository;

    @Mock
    private SpectrumRepository spectrumRepository;

    @Mock
    private MomentMapRepository momentMapRepository;

    @Mock
    private CubeletRepository cubeletRepository;

    @Mock
    private ThumbnailRepository thumbnailRepository;

    @Mock
    private DataAccessJdbcRepository dataAccessJdbcRepository;

    @Mock
    private ImageCutoutRepository imageCutoutRepository;

    @Mock
    private GeneratedSpectrumRepository generatedSpectrumRepository;

    @Mock
    private EncapsulationFileRepository encapsulationFileRepository;

    @Mock
    private EvaluationFileRepository evaluationFileRepository;

    @Mock
    private Packager packager;
    
    private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

    private AccessDataController controller;

    private MockMvc mockMvc;

    private AccessJobManager manager;

    private DataAccessService dataAccessService;

    @Mock
    private CacheManagerInterface cacheManager;

    @Mock
    private CasdaMailService casdaMailService;

    @Mock
    private DownloadManager downloadManager;

    private TestAccessUwsFactory accessUwsFactory;

    private String fileDownloadBaseUrl;

    private String secretKey;

    private Map<String, DataAccessJob> dataAccessJobs;

    private int hoursToExpiryDefault;

    private int hoursToExpirySiapSync;

    private int cancelledJobHoursToExpiry;

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

        hoursToExpiryDefault = RandomUtils.nextInt(20, 30);

        hoursToExpirySiapSync = RandomUtils.nextInt(1, 10);

        cancelledJobHoursToExpiry = RandomUtils.nextInt(10, 20);

        secretKey = "dhjken71^)m`.d*$";
        dataAccessJobs = new HashMap<>();
        when(dataAccessJobRepository.save(any(DataAccessJob.class))).thenAnswer(new Answer<DataAccessJob>()
        {
            public DataAccessJob answer(InvocationOnMock invocation)
            {
                Object[] args = invocation.getArguments();
                DataAccessJob dataAccessJob = (DataAccessJob) args[0];
                dataAccessJob.setId((long) dataAccessJobs.size());
                dataAccessJobs.put(dataAccessJob.getRequestId(), dataAccessJob);
                return dataAccessJob;
            }
        });
        when(dataAccessJobRepository.findByRequestId(any(String.class))).thenAnswer(new Answer<DataAccessJob>()
        {
            public DataAccessJob answer(InvocationOnMock invocation)
            {
                Object[] args = invocation.getArguments();
                String requestId = (String) args[0];
                return dataAccessJobs.get(requestId);
            }
        });
        when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));
        when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                .thenAnswer(new Answer<DataAccessJob>()
                {
                    @Override
                    public DataAccessJob answer(InvocationOnMock invocation) throws Throwable
                    {
                        Object[] args = invocation.getArguments();
                        Long id = (Long) args[1];
                        for (DataAccessJob dataAccessJob : dataAccessJobs.values())
                        {
                            if (dataAccessJob.getId().equals(id))
                            {
                                return dataAccessJob;
                            }
                        }
                        return null;
                    }
                });
        fileDownloadBaseUrl = "cPeqkDzgAmWgghwpjBhegtrVGVBMOE"; // to match the XML test files

        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, thumbnailRepository,
                mock(CachedFileRepository.class), mock(NgasService.class), cacheDir.getRoot().getAbsolutePath(), 25,
                1000, "", "", "", mock(ProcessJobFactory.class), mock(CacheManager.class), dataAccessJdbcRepository,
                imageCutoutRepository, generatedSpectrumRepository, casdaMailService, downloadManager);
        accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, hoursToExpiryDefault,
                hoursToExpirySiapSync);

        manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, cacheManager, 
                accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                generateFileService, null, dataAccessService, casdaMailService, "uwsBaseUrl", 1, 1, 2, 3,
                MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl, "mySecretKey", 3, 72);

        manager.init();

        controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class), dataAccessService,
                manager, dataAccessJobRepository, "http://localhost:8088/foo", secretKey, cancelledJobHoursToExpiry,
                500, 5000, 100000);

        when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(createCount());
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
    }

    private Map<String, Object> createCount()
    {
        Map<String, Object> paging = new HashMap<String, Object>();
        paging.put("IMAGE_CUBE", 27L);
        paging.put("MEASUREMENT_SET", 0L);
        paging.put("SPECTRUM", 0L);
        paging.put("MOMENT_MAP", 0L);
        paging.put("CUBELET", 0L);
        paging.put("CATALOGUE", 0L);
        paging.put("IMAGE_CUTOUT", 0L);
        paging.put("GENERATED_SPECTRUM", 0L);
        paging.put("ENCAPSULATION_FILE", 0L);
        paging.put("EVALUATION_FILE", 0L);
        paging.put("ERROR", 0L);
        return paging;
    }

    @Test
    public void testCreateJob() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc
                .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(equalTo(this.dataAccessJobs.values().iterator().next().getRequestId())))) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/"
                                + this.dataAccessJobs.values().iterator().next().getRequestId())))) //
                .andExpect(content().contentType(MediaType.TEXT_PLAIN)) //
                .andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("functional.pending", result.getResponse().getContentAsString());
    }

    @Test
    public void testAbortCreatedJob() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Abort the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "ABORT"))
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(
                new Period(cancellationTime.plusHours(this.cancelledJobHoursToExpiry),
                        dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        /*
         * Override certain fields so they match the test file
         */
        dataAccessJob.setLastModified(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setExpiredTimestamp(new DateTime("2999-12-13T17:33:12.431+1100"));

        location = result.getResponse().getHeader("Location");

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk()).andReturn();

        checkXmlAgainstTestCaseFile("functional.aborted", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelCreatedJobViaHttpDelete() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(delete(location)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelCreatedJobViaHttpPost() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(post(location).param("ACTION", "DELETE")) //
                .andExpect(status().isSeeOther()) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))))
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testScheduleJobWithSuspendedQueue() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Pause the queue so the job does not transition from the QUEUED state when we schedul it
         */
        manager.pauseQueue(AccessJobManager.CATEGORY_A_JOB_LIST_NAME);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("functional.queued", result.getResponse().getContentAsString());
    }

    @Test
    public void testAbortQueuedJob() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Pause the queue so the job does not transition from the QUEUED state when we schedul it
         */
        manager.pauseQueue(AccessJobManager.CATEGORY_A_JOB_LIST_NAME);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther())
                .andReturn();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Abort the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "ABORT"))
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(
                new Period(cancellationTime.plusHours(this.cancelledJobHoursToExpiry),
                        dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        /*
         * Override certain fields so they match the test file
         */
        dataAccessJob.setLastModified(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setExpiredTimestamp(new DateTime("2999-12-13T17:33:12.431+1100"));

        location = result.getResponse().getHeader("Location");

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("functional.aborted", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelQueuedJobViaHttpDelete() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Pause the queue so the job does not transition from the QUEUED state when we schedul it
         */
        manager.pauseQueue(AccessJobManager.CATEGORY_A_JOB_LIST_NAME);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther())
                .andReturn();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Delete the job
         */
        result = this.mockMvc.perform(delete(location)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelQueuedJobViaHttpPost() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Pause the queue so the job does not transition from the QUEUED state when we schedul it
         */
        manager.pauseQueue(AccessJobManager.CATEGORY_A_JOB_LIST_NAME);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther())
                .andReturn();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Delete the job
         */
        result = this.mockMvc.perform(post(location).param("ACTION", "DELETE")) //
                .andExpect(status().isSeeOther()) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))))
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testRun() throws Exception
    {
        String dataProductId = Utils.encryptAesUrlSafe("cube-2|myUserId|NEXUS|WEB|123", secretKey);
        String dataProductId2 = Utils.encryptAesUrlSafe("visibility-10|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube1 = new ImageCube();
        imageCube1.setId(2L);
        imageCube1.setFilesize(1800L);
        imageCube1.setParent(new Observation(123));
        imageCube1.setFilename("imagecubefile.fits");
        when(imageCubeRepository.findOne(Mockito.eq(2L))).thenReturn(imageCube1);

        MeasurementSet measurementSet10 = new MeasurementSet();
        measurementSet10.setId(10L);
        measurementSet10.setFilesize(1805L);
        measurementSet10.setParent(new Observation(1233));
        measurementSet10.setFilename("measurementsetfile.tar");
        when(measurementSetRepository.findOne(Mockito.eq(10L))).thenReturn(measurementSet10);

        MvcResult result;
        /*
         * Create the job
         */
        result = this.mockMvc
                .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", dataProductId, dataProductId2))
                .andReturn();

        String location = result.getResponse().getHeader("Location");
        String requestId = result.getResponse().getContentAsString();

        /* data products shouldn't have been linked to the job yet, this happens when it is run */
        DataAccessJob job = dataAccessJobRepository.findByRequestId(requestId);
        assertThat(job.getImageCubes().size(), is(0));
        assertThat(job.getMeasurementSets().size(), is(0));

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther())
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(
                        "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + requestId))))
                .andReturn();

        job = dataAccessJobRepository.findByRequestId(requestId);
        assertThat(job.getImageCubes().size(), is(1));
        assertThat(job.getImageCubes().get(0), is(imageCube1));
        assertThat(job.getMeasurementSets().size(), is(1));
        assertThat(job.getMeasurementSets().get(0), is(measurementSet10));
    }

    @Test
    public void testExecutingJob() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startAndPauseJobThread();

        /*
         * Check the result
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("functional.executing", result.getResponse().getContentAsString());
    }

    @Test
    public void testAbortExecutingJob() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startAndPauseJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Abort the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "ABORT"))
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(
                new Period(cancellationTime.plusHours(this.cancelledJobHoursToExpiry),
                        dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        /*
         * Override certain fields so they match the test file
         */
        dataAccessJob.setLastModified(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setExpiredTimestamp(new DateTime("2999-12-13T17:33:12.431+1100"));

        location = result.getResponse().getHeader("Location");

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk()).andReturn();

        checkXmlAgainstTestCaseFile("functional.aborted", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelExecutingJobViaHttpDelete() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startAndPauseJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(delete(location)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelExecutingJobViaHttpPost() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startAndPauseJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(post(location).param("ACTION", "DELETE")) //
                .andExpect(status().isSeeOther()) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))))
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testJobFailure() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        DateTime failureTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        jobThread.waitForJobThread();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(failureTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(
                new Period(failureTime.plusHours(this.hoursToExpiryDefault), dataAccessJob.getExpiredTimestamp())
                        .toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        /*
         * Override certain fields so they match the test file
         */
        dataAccessJob.setLastModified(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setExpiredTimestamp(new DateTime("2999-12-13T17:33:12.431+1100"));

        location = result.getResponse().getHeader("Location");

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk()).andReturn();

        checkXmlAgainstTestCaseFile("functional.error", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelFailedJobViaHttpDelete() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        jobThread.waitForJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(delete(location)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelFailedJobViaHttpPost() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        jobThread.waitForJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(post(location).param("ACTION", "DELETE")) //
                .andExpect(status().isSeeOther()) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))))
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testJobCompletion() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(getPaging());

        when(dataAccessJdbcRepository.getPageOfDownloadFiles(any(String.class), any(String.class), any(Integer[].class),
                any(Boolean.class))).thenReturn(getMeasurementSets());

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        DateTime completion = DateTime.now(DateTimeZone.UTC);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        Result packagerResult = new Result(completion.plusHours(hoursToExpiryDefault), RandomUtils.nextLong(0, 10),
                RandomUtils.nextLong(1, 20));
        when(packager.pack(Mockito.eq(dataAccessJob), Mockito.eq(hoursToExpiryDefault))).thenReturn(packagerResult);
        jobThread.waitForJobThread();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(completion, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(completion, dataAccessJob.getAvailableTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(
                new Period(completion.plusHours(this.hoursToExpiryDefault), dataAccessJob.getExpiredTimestamp())
                        .toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        /*
         * Override certain fields so they match the test file
         */
        dataAccessJob.setLastModified(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setAvailableTimestamp(new DateTime("2015-12-06T17:33:12.431+1100"));
        dataAccessJob.setExpiredTimestamp(new DateTime("2999-12-13T17:33:12.431+1100"));

        location = result.getResponse().getHeader("Location");

        /*
         * Check the status
         */
        result = this.mockMvc.perform(get(location)).andExpect(status().isOk()).andReturn();
        checkXmlAgainstTestCaseFile("functional.completed", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelCompletedJobViaHttpDelete() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        Result packagerResult = new Result(new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryDefault),
                RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
        when(packager.pack(Mockito.eq(dataAccessJob), Mockito.eq(hoursToExpiryDefault))).thenReturn(packagerResult);
        jobThread.waitForJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(delete(location)) //
                .andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)))) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testCancelCompletedJobViaHttpPost() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(mock(MeasurementSet.class));

        MvcResult result;

        /*
         * Create the job
         */
        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)).andReturn();

        /*
         * Override certain fields so they match the test file
         */
        String requestId = "ad1c7b3c-f478-426e-81d0-cb6702d009e2";
        DataAccessJob dataAccessJob = this.dataAccessJobs.remove(result.getResponse().getContentAsString());
        dataAccessJob.setRequestId(requestId);
        assertThat(dataAccessJob.getCreatedTimestamp(), is(not(nullValue())));
        dataAccessJob.setCreatedTimestamp(new DateTime("2015-12-06T18:25:13.823+1100"));
        this.dataAccessJobs.put(requestId, dataAccessJob);
        String location = result.getResponse().getHeader("Location").replace(result.getResponse().getContentAsString(),
                requestId);

        /*
         * Schedule the job
         */
        result = this.mockMvc.perform(post(location + "/phase").param("PHASE", "RUN")).andExpect(status().isSeeOther()) //
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andExpect(header().string("Location", is(equalTo(location)))) //
                .andReturn();

        /*
         * Make sure the job is executing and wait for it to finish
         */
        TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
        jobThread = accessUwsFactory.getJobThreads().get(0);
        jobThread.startJobThread();
        Result packagerResult = new Result(new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryDefault),
                RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
        when(packager.pack(Mockito.eq(dataAccessJob), Mockito.eq(hoursToExpiryDefault))).thenReturn(packagerResult);
        jobThread.waitForJobThread();

        DateTime cancellationTime = DateTime.now(DateTimeZone.UTC);

        /*
         * Cancel the job
         */
        result = this.mockMvc.perform(post(location).param("ACTION", "DELETE")) //
                .andExpect(status().isSeeOther()) //
                .andExpect(header().string("Location",
                        is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))))
                .andExpect(content().string(is(emptyOrNullString()))) //
                .andExpect(header().doesNotExist("Content-Type")) //
                .andReturn();

        assertThat(dataAccessJob.getExpiredTimestamp(), is(not(nullValue())));
        assertThat(new Period(cancellationTime, dataAccessJob.getLastModified()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        assertThat(new Period(cancellationTime, dataAccessJob.getExpiredTimestamp()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        String jobListLocation = result.getResponse().getHeader("Location");

        /*
         * Check the job is expired
         */
        try
        {
            this.mockMvc.perform(get(location));
            Assert.fail("Expected GET " + location + " to fail");
        }
        catch (NestedServletException ex)
        {
            assertThat(ex.getCause(), is(instanceOf(ResourceNoLongerAvailableException.class)));
        }

        /*
         * Check the job list is empty
         */
        result = this.mockMvc.perform(get(jobListLocation)).andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_XML)).andReturn();

        checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
    }

    @Test
    public void testExpiredJob() throws Exception
    {
        String jobId = RandomStringUtils.randomAlphanumeric(20);
        DataAccessJob dataAccessJob = mock(DataAccessJob.class);
        when(dataAccessJob.isExpired()).thenReturn(true);
        when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

        exception.expect(NestedServletException.class);
        exception.expectCause(is(instanceOf(ResourceNoLongerAvailableException.class)));
        exception.expectCause(hasMessage(equalTo("Job with id '" + jobId + "' has expired")));

        this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId));
    }

    /**
     * Tests adding, retrieving and removing a single parameter.
     * 
     * @throws Exception
     *             Not expected
     */
    @Test
    public void testSingleParameterChanges() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("cube-1|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube = new ImageCube();
        imageCube.setId(1L);
        imageCube.setFilesize(1800L);
        when(imageCubeRepository.findOne(Mockito.eq(1L))).thenReturn(imageCube);

        MvcResult result;

        result = this.mockMvc
                .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id))
                .andExpect(status().isSeeOther()).andReturn();
        String location = result.getResponse().getHeader("Location");

        // Check the initial parameter output
        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        checkXmlAgainstTestCaseFile("functional.parameters.id", result.getResponse().getContentAsString());

        // Add the parameter and check its value is retained
        result = this.mockMvc.perform(post(location + "/parameters/pos").param("value", "CIRCLE 1 2 3"))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + id + "]]></uws:parameter>"));
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"pos\"><![CDATA[CIRCLE 1 2 3]]></uws:parameter>"));

        // Add an extra value for the parameter and check its value is retained
        result = this.mockMvc
                .perform(post(location + "/parameters/pos").param("value", "RANGE 135 136.1 -45.56 -46.23"))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + id + "]]></uws:parameter>"));
        assertThat(result.getResponse().getContentAsString(), anyOf(
                containsString("<uws:parameter id=\"pos\"><![CDATA[{CIRCLE 1 2 3,RANGE 135 136.1 -45.56 -46.23}]]>"
                        + "</uws:parameter>"),
                containsString("<uws:parameter id=\"pos\"><![CDATA[{RANGE 135 136.1 -45.56 -46.23,CIRCLE 1 2 3}]]>"
                        + "</uws:parameter>")));

        // Check the retrieval of a specific parameter
        result = this.mockMvc.perform(get(location + "/parameters/pos")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                not(containsString("<uws:parameter id=\"id\"><![CDATA[" + id + "]]></uws:parameter>")));
        assertThat(result.getResponse().getContentAsString(), anyOf(
                containsString("<uws:parameter id=\"POS\"><![CDATA[{CIRCLE 1 2 3,RANGE 135 136.1 -45.56 -46.23}]]>"
                        + "</uws:parameter>"),
                containsString("<uws:parameter id=\"POS\"><![CDATA[{RANGE 135 136.1 -45.56 -46.23,CIRCLE 1 2 3}]]>"
                        + "</uws:parameter>")));

        // Remove a value from the parameter and check the other value is retained
        result = this.mockMvc.perform(delete(location + "/parameters/pos/CIRCLE 1 2 3"))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + id + "]]></uws:parameter>"));
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"pos\"><![CDATA[RANGE 135 136.1 -45.56 -46.23]]></uws:parameter>"));

        // Remove the parameter and check it is gone
        result = this.mockMvc.perform(delete(location + "/parameters/pos")).andExpect(status().isSeeOther())
                .andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + id + "]]></uws:parameter>"));
        assertThat(result.getResponse().getContentAsString(), not(
                containsString("<uws:parameter id=\"pos\"><![CDATA[RANGE 135 136.1 -45.56 -46.23]]></uws:parameter>")));
        assertThat(result.getResponse().getContentAsString(), not(containsString("<uws:parameter id=\"pos\">")));
        result = this.mockMvc.perform(get(location + "/parameters/pos")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(), is(""));
    }

    @Test
    public void testAddIdParameter() throws Exception
    {
        String imageCubeId1 = Utils.encryptAesUrlSafe("cube-1|myUserId|NEXUS|WEB|123", secretKey);
        String imageCubeId2 = Utils.encryptAesUrlSafe("cube-2|myUserId|NEXUS|WEB|123", secretKey);
        String measurementSetId1 = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);
        String measurementSetId2 = Utils.encryptAesUrlSafe("visibility-2|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube1 = new ImageCube();
        imageCube1.setId(1L);
        imageCube1.setFilesize(1800L);
        when(imageCubeRepository.findOne(Mockito.eq(1L))).thenReturn(imageCube1);

        ImageCube imageCube2 = new ImageCube();
        imageCube2.setId(2L);
        imageCube2.setFilesize(1900L);
        when(imageCubeRepository.findOne(Mockito.eq(2L))).thenReturn(imageCube2);

        MeasurementSet measurementSet1 = new MeasurementSet();
        measurementSet1.setId(1L);
        measurementSet1.setFilesize(1800L);
        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(measurementSet1);

        MeasurementSet measurementSet2 = new MeasurementSet();
        measurementSet2.setId(2L);
        measurementSet2.setFilesize(1900L);
        when(measurementSetRepository.findOne(Mockito.eq(2L))).thenReturn(measurementSet2);

        MvcResult result;

        result = this.mockMvc
                .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", imageCubeId1))
                .andExpect(status().isSeeOther()).andReturn();
        String location = result.getResponse().getHeader("Location");

        // Check the initial parameter output
        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        checkXmlAgainstTestCaseFile("functional.parameters.id", result.getResponse().getContentAsString());

        // add a second image cube
        result = this.mockMvc.perform(post(location + "/parameters/id").param("value", imageCubeId2))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                anyOf(containsString("<uws:parameter id=\"id\"><![CDATA[{" + imageCubeId1 + "," + imageCubeId2 + "}]]>"
                        + "</uws:parameter>"),
                        containsString("<uws:parameter id=\"id\"><![CDATA[{" + imageCubeId2 + "," + imageCubeId1
                                + "}]]>" + "</uws:parameter>")));

        // add measurement sets
        result = this.mockMvc
                .perform(post(location + "/parameters").param("id", measurementSetId1).param("id", measurementSetId2))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                allOf(containsString("<uws:parameter id=\"id\"><![CDATA[{"), containsString(imageCubeId1),
                        containsString(imageCubeId2), containsString(measurementSetId1),
                        containsString(measurementSetId2), containsString("}]]></uws:parameter>")));

    }

    @Test
    public void testDeleteIdParameterWithValues() throws Exception
    {
        String imageCubeId1 = Utils.encryptAesUrlSafe("cube-1|myUserId|NEXUS|WEB|123", secretKey);
        String measurementSetId1 = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube1 = new ImageCube();
        imageCube1.setId(1L);
        imageCube1.setFilesize(1800L);
        when(imageCubeRepository.findOne(Mockito.eq(1L))).thenReturn(imageCube1);

        MeasurementSet measurementSet1 = new MeasurementSet();
        measurementSet1.setId(1L);
        measurementSet1.setFilesize(1800L);
        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(measurementSet1);

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", imageCubeId1, measurementSetId1))
                .andExpect(status().isSeeOther()).andReturn();
        String location = result.getResponse().getHeader("Location");

        // Check the initial parameter output
        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                allOf(containsString("<uws:parameter id=\"id\"><![CDATA[{"), containsString(imageCubeId1),
                        containsString(measurementSetId1)));

        // remove a data product with encoded id
        result = this.mockMvc.perform(delete(location + "/parameters/id/" + imageCubeId1))
                .andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + measurementSetId1 + "]]>"));

        // remove a data product with plain id does nothing
        result = this.mockMvc.perform(delete(location + "/parameters/id/cube-2")).andExpect(status().isSeeOther())
                .andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[" + measurementSetId1 + "]]>"));
    }

    @Test
    public void testDeleteIdParameter() throws Exception
    {
        String imageCubeId1 = Utils.encryptAesUrlSafe("cube-1|myUserId|NEXUS|WEB|123", secretKey);
        String imageCubeId2 = Utils.encryptAesUrlSafe("cube-2|myUserId|NEXUS|WEB|123", secretKey);
        String measurementSetId1 = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);
        String measurementSetId2 = Utils.encryptAesUrlSafe("visibility-2|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube1 = new ImageCube();
        imageCube1.setId(1L);
        imageCube1.setFilesize(1800L);
        when(imageCubeRepository.findOne(Mockito.eq(1L))).thenReturn(imageCube1);

        ImageCube imageCube2 = new ImageCube();
        imageCube2.setId(2L);
        imageCube2.setFilesize(1900L);
        when(imageCubeRepository.findOne(Mockito.eq(2L))).thenReturn(imageCube2);

        MeasurementSet measurementSet1 = new MeasurementSet();
        measurementSet1.setId(1L);
        measurementSet1.setFilesize(1800L);
        when(measurementSetRepository.findOne(Mockito.eq(1L))).thenReturn(measurementSet1);

        MeasurementSet measurementSet2 = new MeasurementSet();
        measurementSet2.setId(2L);
        measurementSet2.setFilesize(1900L);
        when(measurementSetRepository.findOne(Mockito.eq(2L))).thenReturn(measurementSet2);

        MvcResult result;

        result = this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("id", imageCubeId1, imageCubeId2, measurementSetId1, measurementSetId2))
                .andExpect(status().isSeeOther()).andReturn();
        String location = result.getResponse().getHeader("Location");

        // Check the initial parameter output
        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                allOf(containsString("<uws:parameter id=\"id\"><![CDATA[{"), containsString(imageCubeId1),
                        containsString(imageCubeId2), containsString(measurementSetId1),
                        containsString(measurementSetId2)));

        // remove all ids
        result = this.mockMvc.perform(delete(location + "/parameters/id")).andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        checkXmlAgainstTestCaseFile("functional.parameters.empty", result.getResponse().getContentAsString());
    }

    /**
     * Tests adding, retrieving and removing parameters in bulk.
     * 
     * @throws Exception
     *             Not expected
     */
    @Test
    public void testBulkParameterChanges() throws Exception
    {
        String id = Utils.encryptAesUrlSafe("cube-1|myUserId|NEXUS|WEB|123", secretKey);

        ImageCube imageCube = new ImageCube();
        imageCube.setId(1L);
        imageCube.setFilesize(1800L);
        when(imageCubeRepository.findOne(Mockito.eq(1L))).thenReturn(imageCube);

        MvcResult result;

        result = this.mockMvc
                .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id))
                .andExpect(status().isSeeOther()).andReturn();
        String location = result.getResponse().getHeader("Location");

        // Check the initial parameter output
        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();

        checkXmlAgainstTestCaseFile("functional.parameters.id", result.getResponse().getContentAsString());

        // Add two params and test values
        result = this.mockMvc.perform(post(location + "/parameters").param("pos", "CIRCLE 1 2 3").param("pos",
                "RANGE 135 136.1 -45.56 -46.23")).andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();
        assertThat(result.getResponse().getContentAsString(),
                containsString("<uws:parameter id=\"id\"><![CDATA[dJz-LNHpR8XP3DMW9-Yo5i8J0odXzcwEnAs9fg74NxQ]]>"
                        + "</uws:parameter>"));
        assertThat(result.getResponse().getContentAsString(), anyOf(
                containsString("<uws:parameter id=\"pos\"><![CDATA[{CIRCLE 1 2 3,RANGE 135 136.1 -45.56 -46.23}]]>"
                        + "</uws:parameter>"),
                containsString("<uws:parameter id=\"pos\"><![CDATA[{RANGE 135 136.1 -45.56 -46.23,CIRCLE 1 2 3}]]>"
                        + "</uws:parameter>")));

        // Delete all params and check all are gone
        result = this.mockMvc.perform(delete(location + "/parameters")).andExpect(status().isSeeOther()).andReturn();
        assertThat(result.getResponse().getHeader("Location"), is(equalTo(location)));
        assertThat(result.getResponse().getContentAsString(), is(equalTo("")));

        result = this.mockMvc.perform(get(location + "/parameters")).andExpect(status().isOk()).andReturn();

        checkXmlAgainstTestCaseFile("functional.parameters.empty", result.getResponse().getContentAsString());
    }

    private static void checkXmlAgainstTestCaseFile(String testCase, String xml) throws SAXException, IOException
    {
        XMLUnit.setIgnoreWhitespace(true);
        XMLUnit.setIgnoreAttributeOrder(true);

        DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(
                FileUtils.readFileToString(new File("src/test/resources/siap2/" + testCase + ".xml")), xml));

        List<?> allDifferences = diff.getAllDifferences();
        Assert.assertEquals("Differences found: " + diff.toString(), 0, allDifferences.size());
    }

    private static Map<String, Object> getPaging()
    {
        Map<String, Object> paging = new LinkedHashMap<String, Object>();
        paging.put("IMAGE_CUBE", 0L);
        paging.put("MEASUREMENT_SET", 1L);
        paging.put("SPECTRUM", 0L);
        paging.put("MOMENT_MAP", 0L);
        paging.put("CUBELET", 0L);
        paging.put("IMAGE_CUTOUT", 0L);
        paging.put("GENERATED_SPECTRUM", 0L);
        paging.put("ENCAPSULATION_FILE", 0L);
        paging.put("EVALUATION_FILE", 0L);
        paging.put("CATALOGUE", 0L);
        paging.put("ERROR", 0L);
        return paging;
    }

    private static List<Map<String, Object>> getMeasurementSets()
    {
        List<Map<String, Object>> measurementSets = new ArrayList<Map<String, Object>>();
        Map<String, Object> measurementSet = new HashMap<String, Object>();
        measurementSet.put("id", 1L);
        measurementSet.put("filesize", 27L);
        measurementSet.put("filename", "visibility-2.fits");
        measurementSet.put("obsid", 12345);
        measurementSet.put("l7id", null);
        measurementSets.add(measurementSet);
        return measurementSets;
    }
}
