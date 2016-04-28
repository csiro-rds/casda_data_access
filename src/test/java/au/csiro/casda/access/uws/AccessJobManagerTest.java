package au.csiro.casda.access.uws;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.Level;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.jdbc.core.JdbcTemplate;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.JobDto;
import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.SizeLimitReachedException;
import au.csiro.casda.access.cache.CacheManagerInterface;
import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.cache.Packager.Result;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CatalogueRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.NgasService;
import au.csiro.casda.access.siap2.CutoutService;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.Level7Collection;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import au.csiro.casda.jobmanager.SlurmJobManager;
import uws.job.ErrorType;
import uws.job.ExecutionPhase;
import uws.job.UWSJob;
import uws.service.UWSFactory;
import uws.service.file.LocalUWSFileManager;

/**
 * AccessJobManager test cases.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class AccessJobManagerTest
{

    /**
     * createJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class JobCreationTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private JdbcTemplate jdbcTemplate;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private CutoutService cutoutService;

        private AccessJobManager manager;

        private Log4JTestAppender testAppender;

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
            testAppender = Log4JTestAppender.createAppender();
            cutoutService = new CutoutService("", jdbcTemplate);

            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, mock(CacheManagerInterface.class), mock(UWSFactory.class),
                    new LocalUWSFileManager(uwsDir.getRoot()), cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3,
                    100000, "http://localhost:8088/foo");
            manager.init();
        }

        @Test
        public void testCreateDataAccessJobImageCube() throws Exception
        {
            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(imageCubeRepository.findOne(2l))
                    .thenReturn(createImageCube(2l, "image_cube-2.fits", 100l, "ABC123", 123123));
            when(imageCubeRepository.findOne(5l))
                    .thenReturn(createImageCube(5l, "image_cube-5.fits", 300l, "ABC123", 123123));

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            jobDto.setIds(new String[] { "cube-2", "cube-5", "cube-7" });
            jobDto.setDownloadMode(CasdaDownloadMode.WEB);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            assertThat(job.getUserName(), is("bob"));
            assertThat(job.getUserIdent(), is("12345"));
            assertThat(job.getUserLoginSystem(), is("sysdy"));
            assertThat(job.getUserEmail(), is("bob@bob.com"));
            assertThat(job.getRequestId(), notNullValue());
            assertThat(job.getImageCubes().size(), is(2));
            assertThat(job.getSizeKb(), is(400l));
            assertNull(job.getDownloadFormat());
            assertThat(job.getDownloadMode(), is(CasdaDownloadMode.WEB));

            assertThat(manager.getJobList(job).getName(), is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.PENDING)));
            assertThat(status.getDestructionTime(), is(nullValue()));

            // 7 doesn't exist, so make sure the error is logged.
            String logMessage = CasdaDataAccessEvents.E100.messageBuilder().add("image_cube").add("7").toString();
            testAppender.verifyLogMessage(Level.ERROR, logMessage);

            assertThat(logMessage, containsString("[E100] "));
            assertThat(logMessage, containsString("image_cube"));
            assertThat(logMessage, containsString("7"));

            // make sure the job creation is logged
            logMessage = CasdaDataAccessEvents.E037.messageBuilder().add("WEB").add(job.getRequestId()).toString();
            testAppender.verifyLogMessage(Level.INFO, logMessage);
            assertThat(logMessage, containsString(CasdaDownloadMode.WEB.name()));
            assertThat(logMessage, containsString(job.getRequestId()));
            assertThat(logMessage, containsString("[E037] "));
        }

        @Test
        public void testCreateDataAccessJobSizeLimitExceeded() throws Exception
        {
            exception.expect(SizeLimitReachedException.class);
            exception.expectMessage("Data products accessed through this method are limited to 2GB. Your requested"
                    + " data exceeded this limit at 4GB");

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(imageCubeRepository.findOne(2l))
                    .thenReturn(createImageCube(2l, "image_cube-2.fits", 1024000l, "ABC123", 123123));
            when(imageCubeRepository.findOne(5l))
                    .thenReturn(createImageCube(5l, "image_cube-5.fits", 3068000l, "ABC123", 123123));

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            jobDto.setIds(new String[] { "cube-2", "cube-5" });
            jobDto.setDownloadMode(CasdaDownloadMode.WEB);

            manager.createDataAccessJob(jobDto, 2048000L, true);
        }

        @Test
        public void testCreateDataAccessJobCatalogue() throws Exception
        {
            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(catalogueRepository.findOne(2l)).thenReturn(createCatalogue(2l, 10L));
            when(catalogueRepository.findOne(5l)).thenReturn(createCatalogue(5l, 10L));

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            jobDto.setIds(new String[] { "catalogue-2", "catalogue-5", "catalogue-7" });
            jobDto.setDownloadFormat("CSV_INDIVIDUAL");
            jobDto.setDownloadMode(CasdaDownloadMode.PAWSEY_HTTP);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            assertThat(job.getUserName(), is("bob"));
            assertThat(job.getUserIdent(), is("12345"));
            assertThat(job.getUserLoginSystem(), is("sysdy"));
            assertThat(job.getUserEmail(), is("bob@bob.com"));
            assertThat(job.getRequestId(), notNullValue());
            assertThat(job.getCatalogues().size(), is(2));
            assertThat(job.getSizeKb(), is(0l));
            assertThat(job.getDownloadFormat(), is("CSV_INDIVIDUAL"));
            assertThat(job.getDownloadMode(), is(CasdaDownloadMode.PAWSEY_HTTP));

            assertThat(manager.getJobList(job).getName(), is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.PENDING)));
            assertThat(status.getDestructionTime(), is(nullValue()));

            // 7 doesn't exist, so make sure the error is logged.
            testAppender.verifyLogMessage(Level.ERROR,
                    Matchers.allOf(containsString("[E100]"), containsString("catalogue"), containsString("7")),
                    sameInstance((Throwable) null));

            // make sure the job creation is logged
            testAppender.verifyLogMessage(
                    Level.INFO, Matchers.allOf(containsString("[E037]"),
                            containsString(CasdaDownloadMode.PAWSEY_HTTP.name()), containsString(job.getRequestId())),
                    sameInstance((Throwable) null));
        }

        @Test
        public void testCreateDataAccessJobMeasurementSet() throws Exception
        {
            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(measurementSetRepository.findOne(2l)).thenReturn(createMeasurementSet(2l, 100l, "ABC111", 111111));
            when(measurementSetRepository.findOne(5l)).thenReturn(createMeasurementSet(5l, 300l, "ABC111", 111111));

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            jobDto.setIds(new String[] { "visibility-2", "visibility-5", "visibility-7" });
            jobDto.setDownloadMode(CasdaDownloadMode.WEB);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            assertThat(job.getUserName(), is("bob"));
            assertThat(job.getUserIdent(), is("12345"));
            assertThat(job.getUserLoginSystem(), is("sysdy"));
            assertThat(job.getUserEmail(), is("bob@bob.com"));
            assertThat(job.getRequestId(), notNullValue());
            assertThat(job.getMeasurementSets().size(), is(2));
            assertThat(job.getSizeKb(), is(400l));
            assertNull(job.getDownloadFormat());
            assertThat(job.getDownloadMode(), is(CasdaDownloadMode.WEB));

            assertThat(manager.getJobList(job).getName(), is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.PENDING)));
            assertThat(status.getDestructionTime(), is(nullValue()));

            // 7 doesn't exist, so make sure the error is logged.
            String logMessage = CasdaDataAccessEvents.E100.messageBuilder().add("measurement_set").add("7").toString();
            testAppender.verifyLogMessage(Level.ERROR, logMessage);

            assertThat(logMessage, containsString("[E100] "));
            assertThat(logMessage, containsString("measurement_set"));
            assertThat(logMessage, containsString("7"));

            // make sure the job creation is logged
            logMessage = CasdaDataAccessEvents.E037.messageBuilder().add("WEB").add(job.getRequestId()).toString();
            testAppender.verifyLogMessage(Level.INFO, logMessage);
            assertThat(logMessage, containsString(CasdaDownloadMode.WEB.name()));
            assertThat(logMessage, containsString(job.getRequestId()));
            assertThat(logMessage, containsString("[E037] "));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCreateDataAccessJobCutout() throws Exception
        {
            ImageCube imageCube1 = createImageCube(2l, "image_cube-2.fits", 100l, "ABC123", 123123,
                    "src/test/resources/soda/image_geometry.freq_stokes.json", 122.5);
            ImageCube imageCube2 = createImageCube(5l, "image_cube-5.fits", 300l, "ABC123", 123123, null, 182.5);

            final String requestId = "somerequestid";
            when(dataAccessJobRepository.save((DataAccessJob) any())).thenAnswer(new Answer<DataAccessJob>()
            {
                @Override
                public DataAccessJob answer(InvocationOnMock invocation) throws Throwable
                {
                    DataAccessJob job = invocation.getArgumentAt(0, DataAccessJob.class);
                    job.setRequestId(requestId);
                    return job;
                }

            });
            when(imageCubeRepository.findOne(2l)).thenReturn(imageCube1);
            when(imageCubeRepository.findOne(5l)).thenReturn(imageCube2);
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            Map<String, String[]> params = new HashMap<>();
            params.put("id", new String[] { "cube-2", "cube-5", "cube-7" });
            params.put("pos", new String[] { "CIRCLE 32.0 14.0 4.7" });
            jobDto.setParams(params);
            jobDto.setDownloadMode(CasdaDownloadMode.SIAP_ASYNC);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);
            job = manager.prepareAsyncJobToStart(job.getRequestId(), new String[] { "cube-2", "cube-5", "cube-7" },
                    null);

            assertThat(job.getUserName(), is("bob"));
            assertThat(job.getUserIdent(), is("12345"));
            assertThat(job.getUserLoginSystem(), is("sysdy"));
            assertThat(job.getUserEmail(), is("bob@bob.com"));
            assertThat(job.getRequestId(), notNullValue());
            assertThat(job.getFileCount(), is(2L));
            assertThat(job.getImageCutouts().size(), is(2));
            assertThat(job.getSizeKb(), is(217L));
            assertNull(job.getDownloadFormat());
            assertThat(job.getDownloadMode(), is(CasdaDownloadMode.SIAP_ASYNC));
            assertThat(job.getImageCutouts().get(0).getBounds(), is("32.0 14.0 9.4 9.4 D null null N 12"));
            assertThat(job.getImageCutouts().get(0).getFilesize(), is(72L));
            assertThat(job.getImageCutouts().get(0).getImageCube(), is(imageCube1));
            assertThat(job.getImageCutouts().get(1).getBounds(), is("32.0 14.0 9.4 9.4 D null null N 1"));
            assertThat(job.getImageCutouts().get(1).getFilesize(), is(145L));
            assertThat(job.getImageCutouts().get(1).getImageCube(), is(imageCube2));

            assertThat(manager.getJobList(job).getName(), is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.PENDING)));
            assertThat(status.getDestructionTime(), is(nullValue()));

            // make sure the job creation is logged
            testAppender
                    .verifyLogMessage(Level.INFO,
                            Matchers.allOf(containsString("[E037] "), containsString(job.getRequestId()),
                                    containsString(CasdaDownloadMode.SIAP_ASYNC.name())),
                    sameInstance((Throwable) null));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCreateDataAccessJobCutoutOversize() throws Exception
        {
            ImageCube imageCube1 = createImageCube(2l, "image_cube-2.fits", 100l, "ABC123", 123123,
                    "src/test/resources/soda/image_geometry.freq_stokes.json", 110.5);
            final String requestId = "somerequestid";
            when(dataAccessJobRepository.save((DataAccessJob) any())).thenAnswer(new Answer<DataAccessJob>()
            {
                @Override
                public DataAccessJob answer(InvocationOnMock invocation) throws Throwable
                {
                    DataAccessJob job = invocation.getArgumentAt(0, DataAccessJob.class);
                    job.setRequestId(requestId);
                    return job;
                }

            });
            when(imageCubeRepository.findOne(2l)).thenReturn(imageCube1);
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");

            JobDto jobDto = new JobDto();
            jobDto.setUserName("bob");
            jobDto.setUserIdent("12345");
            jobDto.setUserLoginSystem("sysdy");
            jobDto.setUserEmail("bob@bob.com");
            Map<String, String[]> params = new HashMap<>();
            params.put("id", new String[] { "cube-2" });
            params.put("pos", new String[] { "CIRCLE 32.0 14.0 4.7" });
            jobDto.setParams(params);
            jobDto.setDownloadMode(CasdaDownloadMode.SIAP_ASYNC);

            DataAccessJob job = manager.createDataAccessJob(jobDto, null, false);
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);
            job = manager.prepareAsyncJobToStart(job.getRequestId(), new String[] { "cube-2" }, null);

            assertThat(job.getFileCount(), is(1L));
            assertThat(job.getImageCutouts().size(), is(1));
            assertThat(job.getSizeKb(), is(80L));
            assertThat(job.getImageCutouts().get(0).getBounds(), is("32.0 14.0 9.4 9.4 D null null N 12"));
            assertThat(job.getImageCutouts().get(0).getFilesize(), is(80L));
            assertThat(job.getImageCutouts().get(0).getImageCube(), is(imageCube1));

            assertThat(manager.getJobList(job).getName(), is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.PENDING)));
            assertThat(status.getDestructionTime(), is(nullValue()));
        }

    }

    /**
     * getJobList method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class GetJobListTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Mock
        private CacheManagerInterface cacheManager;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private AccessJobManager manager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

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
            manager = new AccessJobManager(mock(EntityManagerFactory.class), mock(DataAccessJobRepository.class),
                    mock(ImageCubeRepository.class), mock(CatalogueRepository.class),
                    mock(MeasurementSetRepository.class), cacheManager, mock(UWSFactory.class),
                    new LocalUWSFileManager(uwsDir.getRoot()), mock(CutoutService.class), slurmJobManager, "uwsBaseUrl",
                    1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, "http://localhost:8088/foo");
            manager.init();
        }

        @Test
        public void testGetJobListForImmediateRequest()
        {
            when(cacheManager.allFilesAvailableInCache(any())).thenReturn(true);
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.getRequestId()).thenReturn(RandomStringUtils.randomAlphanumeric(20));

            assertThat(manager.getJobList(dataAccessJob).getName(),
                    is(equalTo(AccessJobManager.IMMEDIATE_JOB_LIST_NAME)));
        }

        @Test
        public void testGetJobListForCategoryARequest()
        {
            when(cacheManager.allFilesAvailableInCache(any())).thenReturn(false);
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.getRequestId()).thenReturn(RandomStringUtils.randomAlphanumeric(20));
            when(dataAccessJob.getSizeKb()).thenReturn(MAX_SMALL_JOB_SIZE_KB);

            assertThat(manager.getJobList(dataAccessJob).getName(),
                    is(equalTo(AccessJobManager.CATEGORY_A_JOB_LIST_NAME)));
        }

        @Test
        public void testGetJobListForCategoryBRequest()
        {
            when(cacheManager.allFilesAvailableInCache(any())).thenReturn(false);
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.getRequestId()).thenReturn(RandomStringUtils.randomAlphanumeric(20));
            when(dataAccessJob.getSizeKb()).thenReturn(MAX_SMALL_JOB_SIZE_KB + 1);

            assertThat(manager.getJobList(dataAccessJob).getName(),
                    is(equalTo(AccessJobManager.CATEGORY_B_JOB_LIST_NAME)));
        }
    }

    /**
     * getJobStatus method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class GetJobStatusTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        @Mock
        private Packager packager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private CacheManagerInterface cacheManager;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            cacheManager = mock(CacheManagerInterface.class);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, cacheManager, accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                    cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testIdsGetJobStatusIds() throws Exception
        {
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(RandomStringUtils.randomAscii(12));
            job.addImageCube(createImageCube(12L, "image_cube-12.fits", 199L, "ABC123", 123));
            job.addImageCube(createImageCube(16L, "image_cube-16.fits", 19L, "ABC122", 1223));
            job.addMeasurementSet(createMeasurementSet(15L, 155L, "ABB111", 111));
            ImageCutout cutoutA = createImageCutout(12L, 156L);
            cutoutA.setImageCube(createImageCube(14L, "image_cube-14.fits", 162L, "AA111", 112));
            job.addImageCutout(cutoutA);
            job.setStatus(DataAccessJobStatus.PREPARING);
            job.setCreatedTimestamp(DateTime.now());
            job.getParamMap().add(ParamKeyWhitelist.ID.name(), "secretid1", "secretid2");
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            UWSJob uwsJob = manager.getJobStatus(requestId);
            assertThat(((String) uwsJob.getAdditionalParameterValue("id")).replace("{", "").replace("}", "").split(","),
                    arrayContainingInAnyOrder("secretid1", "secretid2"));
        }
    }

    /**
     * scheduleJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class ScheduleJobTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        @Mock
        private Packager packager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private CacheManagerInterface cacheManager;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

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

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            cacheManager = mock(CacheManagerInterface.class);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, cacheManager, accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                    cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testScheduleJobNotInDb() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();

            exception.expect(ResourceNotFoundException.class);
            exception.expectMessage("Job with requestId '" + requestId + "' could not be found");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobPending() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.scheduleJob(requestId);

            UWSJob status = manager.getJobStatus(requestId);
            /*
             * The job should be in any other state than PENDING
             */
            assertThat(status.getPhase(), is(not(equalTo(ExecutionPhase.PENDING))));
        }

        @Test
        public void testScheduleJobPendingNotOnlineSiapSyncRequestJob() throws Exception
        {
            JobDto jobDto = createJobDto();
            jobDto.setDownloadMode(CasdaDownloadMode.SIAP_SYNC);
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            when(cacheManager.allFilesAvailableInCache(any())).thenReturn(false);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            exception.expect(ScheduleJobException.class);
            exception.expectMessage("Not all files for the sync job are available in the cache.");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobQueued() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseQueue(manager.getJobList(job).getName());
            manager.scheduleJob(requestId);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'QUEUED'");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobExecuting() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startAndPauseJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'EXECUTING'");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobCompleted() throws Exception
        {
            JobDto jobDto = createJobDto();
            int sbid = 123123;
            long imageId = 2L;
            addImageCubeToJobDto(jobDto, imageId, 100L, "ABC123", sbid, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'COMPLETED'");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobErrored() throws Exception
        {
            JobDto jobDto = createJobDto();
            long imageId = 2L;
            int sbid = 123123;
            addImageCubeToJobDto(jobDto, imageId, 100L, "ABC123", sbid, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'ERROR'");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobAborted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);
            when(slurmJobManager.cancelJob(anyString())).thenReturn(false);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'ABORTED'");

            manager.scheduleJob(requestId);
            
            verify(slurmJobManager, times(1)).cancelJob(anyString());
        }

        @Test
        public void testScheduleJobPaused() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseJob(requestId);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be scheduled because it is in state 'HELD'");

            manager.scheduleJob(requestId);
        }

        @Test
        public void testScheduleJobThatThenFails() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ERROR)));
            assertThat(status.getDestructionTime(), is(not(nullValue())));
            assertThat(status.getErrorSummary().getType(), is(equalTo(ErrorType.FATAL)));
            assertThat(status.getErrorSummary().getMessage(),
                    is(equalTo("Error: A problem occured obtaining access to the requested item(s)")));
            assertThat(status.getErrorSummary().getDetails(), is(nullValue()));
        }

        @Test
        public void testSuccessfulJob() throws Exception
        {
            JobDto jobDto = createJobDto();
            int sbid = 123123;
            long imageId = 2L;
            addImageCubeToJobDto(jobDto, imageId, 100L, "ABC123", sbid, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.COMPLETED)));
            assertThat(status.getDestructionTime(), is(not(nullValue())));
            assertThat(status.getErrorSummary(), is(nullValue()));
            assertThat(status.getNbResults(), is(equalTo(2)));

            uws.job.Result imageResult = status.getResult("cube-" + imageId);
            assertThat(imageResult, is(not(nullValue())));
            assertThat(imageResult.getId(), is(equalTo("cube-" + imageId)));
            assertThat(imageResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, false))));
            assertThat(imageResult.getType(), is(equalTo("simple")));
            assertThat(imageResult.getMimeType(), is(nullValue()));
            assertThat(imageResult.getSize(), is(equalTo(-1L)));
            assertThat(imageResult.isRedirectionRequired(), is(false));

            uws.job.Result imageChecksumResult = status.getResult("cube-" + +imageId + ".checksum");
            assertThat(imageChecksumResult, is(not(nullValue())));
            assertThat(imageChecksumResult.getId(), is(equalTo("cube-" + imageId + ".checksum")));
            assertThat(imageChecksumResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, true))));
            assertThat(imageChecksumResult.getType(), is(equalTo("simple")));
            assertThat(imageChecksumResult.getMimeType(), is(nullValue()));
            assertThat(imageChecksumResult.getSize(), is(equalTo(-1L)));
            assertThat(imageChecksumResult.isRedirectionRequired(), is(false));
        }
    }

    /**
     * retryJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class RetryTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private Packager packager;

        @Mock
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

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

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, mock(CacheManagerInterface.class), accessUwsFactory,
                    new LocalUWSFileManager(uwsDir.getRoot()), cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3,
                    MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testRetryJobNotNotInDb() throws Exception
        {
            String requestId = RandomStringUtils.randomAlphanumeric(20);

            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(null);

            exception.expect(ResourceNotFoundException.class);
            exception.expectMessage("Job with requestId '" + requestId + "' could not be found");

            manager.retryJob(requestId);
        }

        @Test
        public void testRetryJobPending() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be retried because it is in state 'PENDING'");

            manager.retryJob(requestId);
        }

        @Test
        public void testRetryJobQueued() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseQueue(manager.getJobList(job).getName());
            manager.scheduleJob(requestId);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be retried because it is in state 'QUEUED'");

            manager.retryJob(requestId);
        }

        @Test
        public void testRetryJobExecuting() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startAndPauseJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be retried because it is in state 'EXECUTING'");

            manager.retryJob(requestId);
        }

        @Test
        public void testRetryJobCompleted() throws Exception
        {
            JobDto jobDto = createJobDto();
            int sbid = 123123;
            long imageId = 2L;
            addImageCubeToJobDto(jobDto, imageId, 100L, "ABC123", sbid, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            manager.retryJob(requestId);

            jobThread = accessUwsFactory.getJobThreads().get(1);
            jobThread.startAndPauseJobThread();
            assertThat(manager.getJobStatus(requestId).getDestructionTime(), is(nullValue()));
            jobThread.unpauseJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.COMPLETED)));
            assertThat(status.getDestructionTime(), is(not(nullValue())));
            assertThat(status.getErrorSummary(), is(nullValue()));
            assertThat(status.getNbResults(), is(equalTo(2)));

            uws.job.Result imageResult = status.getResult("cube-" + imageId);
            assertThat(imageResult, is(not(nullValue())));
            assertThat(imageResult.getId(), is(equalTo("cube-" + imageId)));
            assertThat(imageResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, false))));
            assertThat(imageResult.getType(), is(equalTo("simple")));
            assertThat(imageResult.getMimeType(), is(nullValue()));
            assertThat(imageResult.getSize(), is(equalTo(-1L)));
            assertThat(imageResult.isRedirectionRequired(), is(false));

            uws.job.Result imageChecksumResult = status.getResult("cube-" + +imageId + ".checksum");
            assertThat(imageChecksumResult, is(not(nullValue())));
            assertThat(imageChecksumResult.getId(), is(equalTo("cube-" + imageId + ".checksum")));
            assertThat(imageChecksumResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, true))));
            assertThat(imageChecksumResult.getType(), is(equalTo("simple")));
            assertThat(imageChecksumResult.getMimeType(), is(nullValue()));
            assertThat(imageChecksumResult.getSize(), is(equalTo(-1L)));
            assertThat(imageChecksumResult.isRedirectionRequired(), is(false));
        }

        @Test
        public void testRetryJobErrored() throws Exception
        {
            JobDto jobDto = createJobDto();
            long imageId = 2L;
            int sbid = 123123;
            addImageCubeToJobDto(jobDto, imageId, 100L, "ABC123", sbid, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            assertThat(manager.getJobStatus(requestId).getPhase(), is(ExecutionPhase.ERROR));

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.retryJob(requestId);

            jobThread = accessUwsFactory.getJobThreads().get(1);
            jobThread.startAndPauseJobThread();
            assertThat(manager.getJobStatus(requestId).getDestructionTime(), is(nullValue()));
            jobThread.unpauseJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.COMPLETED)));
            assertThat(status.getDestructionTime(), is(not(nullValue())));
            assertThat(status.getErrorSummary(), is(nullValue()));
            assertThat(status.getNbResults(), is(equalTo(2)));

            uws.job.Result imageResult = status.getResult("cube-" + imageId);
            assertThat(imageResult, is(not(nullValue())));
            assertThat(imageResult.getId(), is(equalTo("cube-" + imageId)));
            assertThat(imageResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, false))));
            assertThat(imageResult.getType(), is(equalTo("simple")));
            assertThat(imageResult.getMimeType(), is(nullValue()));
            assertThat(imageResult.getSize(), is(equalTo(-1L)));
            assertThat(imageResult.isRedirectionRequired(), is(false));

            uws.job.Result imageChecksumResult = status.getResult("cube-" + +imageId + ".checksum");
            assertThat(imageChecksumResult, is(not(nullValue())));
            assertThat(imageChecksumResult.getId(), is(equalTo("cube-" + imageId + ".checksum")));
            assertThat(imageChecksumResult.getHref(),
                    is(equalTo(getImageCubeDownloadLink(fileDownloadBaseUrl, requestId, sbid, imageId, true))));
            assertThat(imageChecksumResult.getType(), is(equalTo("simple")));
            assertThat(imageChecksumResult.getMimeType(), is(nullValue()));
            assertThat(imageChecksumResult.getSize(), is(equalTo(-1L)));
            assertThat(imageChecksumResult.isRedirectionRequired(), is(false));
        }

        @Test
        public void testRetryJobAborted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            assertThat(manager.getJobStatus(requestId).getPhase(), is(ExecutionPhase.ABORTED));

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.retryJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ERROR)));
        }

        @Test
        public void testRetryJobPaused() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseJob(requestId);

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.HELD)));

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.retryJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startAndPauseJobThread();
            assertThat(manager.getJobStatus(requestId).getDestructionTime(), is(nullValue()));
            jobThread.unpauseJobThread();
            jobThread.waitForJobThread();

            UWSJob status = manager.getJobStatus(requestId);

            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ERROR)));
            assertThat(status.getDestructionTime(), is(not(nullValue())));
        }
    }

    /**
     * cancelJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class CancelTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private Packager packager;

        @Mock
        private CacheManagerInterface cacheManager;

        @Mock
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

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

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));
            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, cacheManager, accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                    cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testCancelJobNotInDb() throws Exception
        {
            String requestId = RandomStringUtils.randomAlphanumeric(20);

            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(null);

            exception.expect(ResourceNotFoundException.class);
            exception.expectMessage("Job with requestId '" + requestId + "' could not be found");

            manager.cancelJob(requestId, new DateTime());
        }

        @Test
        public void testCancelJobPending() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(ExecutionPhase.ABORTED));
            assertThat(status.getDestructionTime(), is(equalTo(cancellationTime.toDate())));
            verify(cacheManager, times(1)).updateUnlockForFiles(
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job)),
                    cancellationTime);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Test
        public void testCancelJobQueued() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseQueue(manager.getJobList(job).getName());
            manager.scheduleJob(requestId);

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.QUEUED)));

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            assertThat(job.getStatus(), is(DataAccessJobStatus.CANCELLED));
            assertThat(job.getExpiredTimestamp(), is(equalTo(cancellationTime)));
            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            assertThat(fileCaptor.getAllValues().size(), is(equalTo(1)));
            Collection<DownloadFile> unlockedFiles =
                    (Collection<DownloadFile>) fileCaptor.getAllValues().iterator().next();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Test
        public void testCancelJobExecuting() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startAndPauseJobThread();

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.EXECUTING)));

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            assertThat(job.getStatus(), is(DataAccessJobStatus.CANCELLED));
            assertThat(job.getExpiredTimestamp(), is(equalTo(cancellationTime)));
            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            assertThat(fileCaptor.getAllValues().size(), is(equalTo(1)));
            Collection<DownloadFile> unlockedFiles =
                    (Collection<DownloadFile>) fileCaptor.getAllValues().iterator().next();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Test
        public void testCancelJobCompleted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            UWSJob status = manager.getJobStatus(job.getRequestId());
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ABORTED)));
            assertThat(status.getDestructionTime(), is(equalTo(cancellationTime.toDate())));

            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            Collection<DownloadFile> unlockedFiles = (Collection<DownloadFile>) fileCaptor.getValue();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Test
        public void testCancelJobErrored() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            UWSJob status = manager.getJobStatus(job.getRequestId());
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ABORTED)));
            assertThat(status.getDestructionTime(), is(equalTo(cancellationTime.toDate())));

            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            assertThat(fileCaptor.getAllValues().size(), is(equalTo(1)));
            Collection<DownloadFile> unlockedFiles =
                    (Collection<DownloadFile>) fileCaptor.getAllValues().iterator().next();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

        @Test
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public void testCancelJobAborted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            UWSJob status = manager.getJobStatus(job.getRequestId());
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ABORTED)));
            assertThat(status.getDestructionTime(), is(equalTo(cancellationTime.toDate())));

            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            assertThat(fileCaptor.getAllValues().size(), is(equalTo(1)));
            Collection<DownloadFile> unlockedFiles =
                    (Collection<DownloadFile>) fileCaptor.getAllValues().iterator().next();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Test
        public void testCancelJobPaused() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseJob(requestId);

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.HELD)));

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(job.getRequestId(), cancellationTime);

            UWSJob status = manager.getJobStatus(job.getRequestId());
            assertThat(status.getPhase(), is(equalTo(ExecutionPhase.ABORTED)));
            assertThat(status.getDestructionTime(), is(equalTo(cancellationTime.toDate())));

            ArgumentCaptor<Collection> fileCaptor = ArgumentCaptor.forClass(Collection.class);
            Collection<DownloadFile> files =
                    DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));
            verify(cacheManager, times(1)).updateUnlockForFiles(fileCaptor.capture(), Mockito.eq(cancellationTime));
            assertThat(fileCaptor.getAllValues().size(), is(equalTo(1)));
            Collection<DownloadFile> unlockedFiles =
                    (Collection<DownloadFile>) fileCaptor.getAllValues().iterator().next();
            assertThat(unlockedFiles, is(equalTo(files)));
        }

    }

    /**
     * prioritiseJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class PrioritiseTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private Packager packager;

        @Mock
        private CacheManagerInterface cacheManager;

        @Mock
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

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

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, cacheManager, accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                    cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testPrioritise() throws Exception
        {
            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));

            JobDto[] jobDtos = new JobDto[] { createJobDto(), createJobDto(), createJobDto() };
            DataAccessJob[] dataAccessJobs = new DataAccessJob[jobDtos.length];

            for (int i = 0; i < jobDtos.length; i++)
            {
                addImageCubeToJobDto(jobDtos[i], 2L, 100L, "ABC123", 123123, imageCubeRepository);
                dataAccessJobs[i] = manager.createDataAccessJob(jobDtos[i]);
                when(dataAccessJobRepository.findByRequestId(dataAccessJobs[i].getRequestId()))
                        .thenReturn(dataAccessJobs[i]);
                when(emf.createEntityManager().find(DataAccessJob.class, dataAccessJobs[i].getId()))
                        .thenReturn(dataAccessJobs[i]);
                when(packager.pack(Mockito.eq(dataAccessJobs[i]), any(Integer.class))).thenReturn(packagerResult);
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.pauseQueue(manager.getJobList(dataAccessJobs[i]).getName());
                assertThat(manager.isQueuePaused(manager.getJobList(dataAccessJobs[i]).getName()), is(true));
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.scheduleJob(dataAccessJobs[i].getRequestId());
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                assertThat(manager.getJobStatus(dataAccessJobs[i].getRequestId()).getPhase(),
                        is(ExecutionPhase.QUEUED));
            }

            /*
             * Prioritise in reverse order
             */
            for (int i = dataAccessJobs.length - 1; i >= 0; i--)
            {
                manager.prioritise(dataAccessJobs[i].getRequestId(), dataAccessJobs.length - 1 - i);
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.unpauseQueue(manager.getJobList(dataAccessJobs[i]).getName());
                assertThat(manager.isQueuePaused(manager.getJobList(dataAccessJobs[i]).getName()), is(false));
            }

            int runningJobsCount = 0;
            while (runningJobsCount < dataAccessJobs.length)
            {
                List<TestAccessUwsFactory.NotifyingDataAccessThread> runningJobThreads =
                        new ArrayList<>(accessUwsFactory.getJobThreads());
                if (runningJobThreads.size() > runningJobsCount)
                {
                    for (int i = runningJobsCount; i < runningJobThreads.size(); i++)
                    {
                        runningJobThreads.get(i).startJobThread();
                        runningJobThreads.get(i).waitForJobThread();
                        /*
                         * The jobs should finish in reverse order because of the prioritisation:
                         */
                        assertThat(manager.getJobStatus(dataAccessJobs[dataAccessJobs.length - 1 - i].getRequestId())
                                .getPhase(), is(ExecutionPhase.COMPLETED));
                    }
                    runningJobsCount = runningJobThreads.size();
                }
                else
                {
                    Thread.sleep(10);
                }
            }
        }

    }

    /**
     * pauseJob method test cases.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class PauseTest
    {
        @Rule
        public TemporaryFolder uwsDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public ExpectedException exception = ExpectedException.none();

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
        private Packager packager;

        @Mock
        private CacheManagerInterface cacheManager;

        @Mock
        private CutoutService cutoutService;
        
        @Mock
        private SlurmJobManager slurmJobManager;

        private static final long MAX_SMALL_JOB_SIZE_KB = 100000;

        private AccessJobManager manager;

        private DataAccessService dataAccessService;

        private TestAccessUwsFactory accessUwsFactory;

        private String fileDownloadBaseUrl;

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

            when(dataAccessJobRepository.save((DataAccessJob) any())).then((returnsFirstArg()));

            fileDownloadBaseUrl = RandomStringUtils.randomAlphabetic(30);

            when(emf.createEntityManager()).thenReturn(mock(EntityManager.class));

            dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    measurementSetRepository, mock(CachedFileRepository.class), mock(NgasService.class),
                    cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
            accessUwsFactory = new TestAccessUwsFactory(dataAccessService, packager, 1, 1);
            manager = new AccessJobManager(emf, dataAccessJobRepository, imageCubeRepository, catalogueRepository,
                    measurementSetRepository, cacheManager, accessUwsFactory, new LocalUWSFileManager(uwsDir.getRoot()),
                    cutoutService, slurmJobManager, "uwsBaseUrl", 1, 1, 2, 3, MAX_SMALL_JOB_SIZE_KB, fileDownloadBaseUrl);
            manager.init();
        }

        @Test
        public void testPauseJobNotInDb() throws Exception
        {
            String requestId = RandomStringUtils.randomAlphanumeric(20);

            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(null);

            exception.expect(ResourceNotFoundException.class);
            exception.expectMessage("Job with requestId '" + requestId + "' could not be found");

            manager.pauseJob(requestId);
        }

        @Test
        public void testPauseJobPending() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseJob(requestId);

            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(ExecutionPhase.HELD));
            assertThat(status.getDestructionTime(), is(nullValue()));
            verify(cacheManager, never()).updateUnlockForFiles(Mockito.any(), Mockito.any(DateTime.class));
        }

        @Test
        public void testPauseJobQueued() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.pauseQueue(manager.getJobList(job).getName());
            manager.scheduleJob(requestId);

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.QUEUED)));

            manager.pauseJob(requestId);

            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(ExecutionPhase.HELD));
            assertThat(status.getDestructionTime(), is(nullValue()));
            verify(cacheManager, never()).updateUnlockForFiles(Mockito.any(), Mockito.any(DateTime.class));
        }

        @Test
        public void testPauseJobExecuting() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startAndPauseJobThread();

            assertThat(manager.getJobStatus(requestId).getPhase(), is(equalTo(ExecutionPhase.EXECUTING)));

            manager.pauseJob(requestId);

            UWSJob status = manager.getJobStatus(requestId);
            assertThat(status.getPhase(), is(ExecutionPhase.HELD));
            assertThat(status.getDestructionTime(), is(nullValue()));
            verify(cacheManager, never()).updateUnlockForFiles(Mockito.any(), Mockito.any(DateTime.class));
        }

        @Test
        public void testPauseJobCompleted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));
            when(packager.pack(Mockito.eq(job), any(Integer.class))).thenReturn(packagerResult);

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be paused as it is in phase 'COMPLETED'");

            manager.pauseJob(requestId);
        }

        @Test
        public void testPauseJobErrored() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(emf.createEntityManager().find(Mockito.eq(DataAccessJob.class), Mockito.any(Long.class)))
                    .thenReturn(job);

            /*
             * Deliberately fail the DataAccessThread
             */
            doThrow(new RuntimeException("oops!")).when(packager).pack(Mockito.eq(job), any(Integer.class));

            manager.scheduleJob(requestId);

            TestAccessUwsFactory.NotifyingDataAccessThread jobThread;
            jobThread = accessUwsFactory.getJobThreads().get(0);
            jobThread.startJobThread();
            jobThread.waitForJobThread();

            exception.expect(ResourceIllegalStateException.class);
            exception
                    .expectMessage("Job with requestId '" + requestId + "' cannot be paused as it is in phase 'ERROR'");

            manager.pauseJob(requestId);
        }

        @Test
        public void testPauseJobAborted() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

            DateTime cancellationTime = new DateTime().minusMillis(RandomUtils.nextInt(0, 3600000))
                    .plusMillis(RandomUtils.nextInt(0, 3600000));
            manager.cancelJob(requestId, cancellationTime);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage(
                    "Job with requestId '" + requestId + "' cannot be paused as it is in phase 'ABORTED'");

            manager.pauseJob(requestId);
        }

        @Test
        public void testPauseJobPaused() throws Exception
        {
            JobDto jobDto = createJobDto();
            addImageCubeToJobDto(jobDto, 2L, 100L, "ABC123", 123123, imageCubeRepository);

            DataAccessJob job = manager.createDataAccessJob(jobDto);
            String requestId = job.getRequestId();
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            manager.pauseJob(requestId);

            exception.expect(ResourceIllegalStateException.class);
            exception.expectMessage("Job with requestId '" + requestId + "' cannot be paused as it is in phase 'HELD'");

            manager.pauseJob(requestId);
        }

        @Test
        public void testPauseAndUnpauseQueue() throws Exception
        {
            Result packagerResult =
                    new Result(new DateTime().plusDays(1), RandomUtils.nextLong(0, 10), RandomUtils.nextLong(1, 20));

            JobDto[] jobDtos = new JobDto[] { createJobDto(), createJobDto() };
            DataAccessJob[] dataAccessJobs = new DataAccessJob[jobDtos.length];

            for (int i = 0; i < jobDtos.length; i++)
            {
                addImageCubeToJobDto(jobDtos[i], 2L, 100L, "ABC123", 123123, imageCubeRepository);
                dataAccessJobs[i] = manager.createDataAccessJob(jobDtos[i]);
                dataAccessJobs[i].setId((long) i);
                when(dataAccessJobRepository.findByRequestId(dataAccessJobs[i].getRequestId()))
                        .thenReturn(dataAccessJobs[i]);
                when(emf.createEntityManager().find(DataAccessJob.class, dataAccessJobs[i].getId()))
                        .thenReturn(dataAccessJobs[i]);
                when(packager.pack(Mockito.eq(dataAccessJobs[i]), any(Integer.class))).thenReturn(packagerResult);
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.pauseQueue(manager.getJobList(dataAccessJobs[i]).getName());
                assertThat(manager.isQueuePaused(manager.getJobList(dataAccessJobs[i]).getName()), is(true));
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.scheduleJob(dataAccessJobs[i].getRequestId());
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                assertThat(manager.getJobStatus(dataAccessJobs[i].getRequestId()).getPhase(),
                        is(ExecutionPhase.QUEUED));
            }

            for (int i = 0; i < dataAccessJobs.length; i++)
            {
                manager.unpauseQueue(manager.getJobList(dataAccessJobs[i]).getName());
                assertThat(manager.isQueuePaused(manager.getJobList(dataAccessJobs[i]).getName()), is(false));
            }

            int runningJobsCount = 0;
            while (runningJobsCount < dataAccessJobs.length)
            {
                List<TestAccessUwsFactory.NotifyingDataAccessThread> runningJobThreads =
                        new ArrayList<>(accessUwsFactory.getJobThreads());
                if (runningJobThreads.size() > runningJobsCount)
                {
                    for (int i = runningJobsCount; i < runningJobThreads.size(); i++)
                    {
                        runningJobThreads.get(i).startJobThread();
                        runningJobThreads.get(i).waitForJobThread();
                        /*
                         * The jobs should finish in order:
                         */
                        assertThat(manager.getJobStatus(dataAccessJobs[i].getRequestId()).getPhase(),
                                is(ExecutionPhase.COMPLETED));
                    }
                    runningJobsCount = runningJobThreads.size();
                }
                else
                {
                    Thread.sleep(10);
                }
            }
        }

    }

    /**
     * Creates a suitable JobDto for testing.
     * 
     * @return
     */
    public static JobDto createJobDto()
    {
        JobDto jobDto = new JobDto();
        jobDto.setUserIdent("unittest");
        jobDto.setUserLoginSystem("java");
        jobDto.setUserName("DataAccessServiceTest");
        jobDto.setUserEmail("bob@bob.com");
        jobDto.setDownloadMode(CasdaDownloadMode.WEB);
        return jobDto;
    }

    /**
     * Adds image cube information to a JobDto and ensures that the mockImageCubeRepository will return the ImageCube.
     * 
     * @param jobDto
     *            the JobDto
     * @param id
     *            the ImageCube's id
     * @param sizeKb
     *            the ImageCube's sizeKb
     * @param projectCode
     *            the ImageCube's projectCode
     * @param sbid
     *            the ImageCube's sbid
     * @param mockImageCubeRepository
     *            a mock ImageCubeRepository
     */
    public static void addImageCubeToJobDto(JobDto jobDto, Long id, Long sizeKb, String projectCode, Integer sbid,
            ImageCubeRepository mockImageCubeRepository)
    {
        if (mockImageCubeRepository != null)
        {
            when(mockImageCubeRepository.findOne(id)).thenReturn(
                    AccessJobManagerTest.createImageCube(id, "image_cube-" + id + ".fits", sizeKb, projectCode, sbid));
        }
        ArrayList<String> ids = new ArrayList<>();
        if (jobDto.getIds() != null)
        {
            ids.addAll(Arrays.asList(jobDto.getIds()));
        }
        ids.add("cube-" + Long.toString(id));
        jobDto.setIds(ids.toArray(new String[0]));
    }

    private static String getImageCubeDownloadLink(String fileDownloadBaseUrl, String requestId, int sbid, long imageId,
            boolean isChecksumLink)
    {
        return fileDownloadBaseUrl + "/requests/" + requestId + "/observations-" + sbid + "-image_cubes-"
                + "image_cube-" + imageId + ".fits" + (isChecksumLink ? ".checksum" : "");
    }

    public static ImageCutout createImageCutout(Long id, long size)
    {
        ImageCutout cutout = new ImageCutout();
        cutout.setId(id);
        cutout.setFilesize(size);
        return cutout;
    }

    /**
     * Creates an ImageCube with the given parameters
     * 
     * @param id
     *            the id
     * @param filename
     *            the name of the image file
     * @param fileSizeKb
     *            the size of the image in kb
     * @param projectCode
     *            the project code
     * @param sbid
     *            the observation SBID
     * @param jsonFileName
     *            the name of the json file containing the image bounds
     * @param sfov
     *            the field of view of the image
     * @return the ImageCube
     */
    public static ImageCube createImageCube(Long id, String filename, Long fileSizeKb, String projectCode, Integer sbid,
            String jsonFileName, double sfov) throws IOException
    {
        ImageCube ic = createImageCube(id, filename, fileSizeKb, projectCode, sbid);
        if (jsonFileName != null)
        {
            String sampleWcsLibOutputJson = FileUtils.readFileToString(new File(jsonFileName));
            ic.setDimensions(sampleWcsLibOutputJson);
        }
        ic.setSFov(sfov);
        return ic;
    }

    /**
     * Creates an ImageCube with the given parameters
     * 
     * @param id
     *            the id
     * @param filename
     *            TODO
     * @param fileSizeKb
     *            the size of the image in kb
     * @param projectCode
     *            the project code
     * @param sbid
     *            the observation SBID
     * @return the ImageCube
     */
    public static ImageCube createImageCube(Long id, String filename, Long fileSizeKb, String projectCode, Integer sbid)
    {
        ImageCube ic = new ImageCube();
        ic.setId(id);
        ic.setFilesize(fileSizeKb);
        ic.setProject(new Project(projectCode));
        ic.setParent(new Observation(sbid));
        ic.setFilename(filename);
        return ic;
    }

    /**
     * Creates a MeasurementSet with the given parameters
     * 
     * @param id
     *            the id
     * @param fileSizeKb
     *            the size of the measurement set in kb
     * @param projectCode
     *            the project code
     * @param sbid
     *            the observation sbid
     * @return the MeasurementSet
     */
    public static MeasurementSet createMeasurementSet(Long id, Long fileSizeKb, String projectCode, Integer sbid)
    {
        MeasurementSet measurementSet = new MeasurementSet();
        measurementSet.setId(id);
        measurementSet.setFilesize(fileSizeKb);
        measurementSet.setProject(new Project(projectCode));
        measurementSet.setParent(new Observation(sbid));
        measurementSet.setFilename("measurement_set.tar");
        return measurementSet;
    }

    /**
     * Creates a Catalogue with the given parameters
     * 
     * @param id
     *            the id
     * @param fileSize
     *            the size of the catalogue
     * @return the Catalogue
     */
    public static Catalogue createCatalogue(Long id, Long fileSize)
    {
        Catalogue catalogue = new Catalogue();
        catalogue.setId(id);
        catalogue.setCatalogueType(CatalogueType.values()[RandomUtils.nextInt(0, CatalogueType.values().length)]);
        catalogue.setFilesize(fileSize);
        catalogue.setProject(new Project("ABC124"));
        if (catalogue.getCatalogueType() == CatalogueType.LEVEL7)
        {
            catalogue.setParent(new Level7Collection(222));
            catalogue.setEntriesTableName(RandomStringUtils.randomAlphabetic(20));
        }
        else
        {
            catalogue.setParent(new Observation(111));
        }
        catalogue.setFilename("catalogue.votable");
        return catalogue;
    }

}
