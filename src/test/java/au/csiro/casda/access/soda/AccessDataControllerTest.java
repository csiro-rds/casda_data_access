package au.csiro.casda.access.soda;

import static org.hamcrest.junit.internal.ThrowableMessageMatcher.hasMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.XMLUnit;
import org.hamcrest.junit.ExpectedException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.ExceptionHandlerExceptionResolver;
import org.springframework.web.util.NestedServletException;
import org.xml.sax.SAXException;

import au.csiro.TestUtils;
import au.csiro.casda.access.BadRequestException;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.JobDto;
import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNoLongerAvailableException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.SystemStatus;
import au.csiro.casda.access.cache.CacheManager;
import au.csiro.casda.access.cache.DownloadManager;
import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.CachedFileRepository;
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
import au.csiro.casda.access.uws.AccessJobManagerTest;
import au.csiro.casda.entity.CasdaDataProductEntity;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;
import uws.job.ErrorSummary;
import uws.job.ErrorType;
import uws.job.ExecutionPhase;
import uws.job.Result;
import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;
import uws.job.user.DefaultJobOwner;

/**
 * Test cases for AccessDataAsyncController.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class AccessDataControllerTest
{
    
    private static Map<String, Object> createCount()
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
        paging.put("ERROR", 0L);
        return paging;
    }
    
    /**
     * Test cases for the getAvailability end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class GetAvailabilityTest
    {
        @Mock
        HealthEndpoint healthEndpoint;

        @Mock
        SystemStatus systemStatus;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            // testAppender = Log4JTestAppender.createAppender();
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(healthEndpoint, systemStatus, mock(DataAccessService.class),
                    mock(AccessJobManager.class), mock(DataAccessJobRepository.class), "http://localhost:8088/foo",
                    RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500, 5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testGetAvailabilityForStatusUp() throws Exception
        {
            Status status = Status.UP;
            Health health = new Health.Builder().status(status).build();
            when(healthEndpoint.invoke()).thenReturn(health);

            ZonedDateTime upSince = ZonedDateTime.now().minus(RandomUtils.nextLong(1, 1000), ChronoUnit.MILLIS);
            when(systemStatus.getUpSince()).thenReturn(upSince);

            MvcResult result = mockMvc.perform(get(AccessDataController.ACCESS_DATA_AVAILABILITY_BASE_PATH))
                    .andExpect(status().isOk()).andDo(print()).andReturn();

            /*
             * Can't test actual response because it's rendered with a JSP.
             */
            ModelAndView mav = result.getModelAndView();
            assertThat(mav.getViewName(),
                    is(equalTo(AccessDataController.ACCESS_DATA_AVAILABILITY_BASE_PATH + ".xml")));
            assertThat(mav.getModel().get("available"), is(equalTo(true)));
            assertThat(mav.getModel().get("upSince"),
                    is(equalTo(AccessDataController.convertZonedDateTimeToXMLGregorianCalendar(upSince))));
            assertThat(mav.getModel().get("note"), is(equalTo("")));
        }

        @Test
        public void testGetAvailabilityForStatusDown() throws Exception
        {
            Status status = Status.DOWN;
            Health health = new Health.Builder().status(status).build();
            when(healthEndpoint.invoke()).thenReturn(health);

            ZonedDateTime upSince = ZonedDateTime.now().plus(RandomUtils.nextLong(1, 1000), ChronoUnit.MILLIS);
            when(systemStatus.getUpSince()).thenReturn(upSince);

            MvcResult result = mockMvc.perform(get(AccessDataController.ACCESS_DATA_AVAILABILITY_BASE_PATH))
                    .andExpect(status().isOk()).andDo(print()).andReturn();

            /*
             * Can't test actual response because it's rendered with a JSP.
             */
            ModelAndView mav = result.getModelAndView();
            assertThat(mav.getViewName(),
                    is(equalTo(AccessDataController.ACCESS_DATA_AVAILABILITY_BASE_PATH + ".xml")));
            assertThat(mav.getModel().get("available"), is(equalTo(false)));
            assertThat(mav.getModel().get("upSince"),
                    is(equalTo(AccessDataController.convertZonedDateTimeToXMLGregorianCalendar(upSince))));
            assertThat(mav.getModel().get("note"), is(equalTo("Health check FAILED")));
        }
    }

    /**
     * Test cases for the getCapabilities end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    @RunWith(Parameterized.class)
    public static class GetCapabilitiesTest
    {
        @Parameters
        public static Collection<Object[]> data()
        {
            return Arrays.asList(new Object[][] { { RandomStringUtils.randomAlphanumeric(20) },
                    { RandomStringUtils.randomAlphanumeric(20) } });
        }

        private String baseUrl;

        public GetCapabilitiesTest(Object baseUrl) throws Exception
        {
            this.baseUrl = (String) baseUrl;
        }

        @Test
        public void testGetCapabilities() throws Exception
        {
            AccessDataController controller = new AccessDataController(mock(HealthEndpoint.class),
                    mock(SystemStatus.class), mock(DataAccessService.class), mock(AccessJobManager.class),
                    mock(DataAccessJobRepository.class), this.baseUrl, RandomStringUtils.randomAscii(16),
                    RandomUtils.nextInt(10, 20), 500, 5000, 100000);
            MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();

            MvcResult result = mockMvc.perform(get(AccessDataController.ACCESS_DATA_CAPABILITIES_BASE_PATH))
                    .andExpect(status().isOk()).andDo(print()).andReturn();

            /*
             * Can't test actual response because it's rendered with a JSP.
             */
            ModelAndView mav = result.getModelAndView();
            assertThat(mav.getViewName(),
                    is(equalTo(AccessDataController.ACCESS_DATA_CAPABILITIES_BASE_PATH + ".xml")));
            assertThat(mav.getModel().get("capabilitiesURL"),
                    is(equalTo(this.baseUrl + AccessDataController.ACCESS_DATA_CAPABILITIES_BASE_PATH)));
            assertThat(mav.getModel().get("availabilityURL"),
                    is(equalTo(this.baseUrl + AccessDataController.ACCESS_DATA_AVAILABILITY_BASE_PATH)));
            assertThat(mav.getModel().get("asyncURL"),
                    is(equalTo(this.baseUrl + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)));
        }
    }

    /**
     * Test cases for the getJobList end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class GetJobListTest
    {
        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            // testAppender = Log4JTestAppender.createAppender();
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), mock(AccessJobManager.class), mock(DataAccessJobRepository.class),
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testGetJobList() throws Exception
        {

            MvcResult result =
                    mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)).andExpect(status().isOk()) //
                            .andExpect(content().contentType(MediaType.APPLICATION_XML_VALUE)).andDo(print()) //
                            .andReturn();
            checkXmlAgainstTestCaseFile("job_list", result.getResponse().getContentAsString());
        }
    }

    /**
     * Test cases for syncDownloadDataProduct endpoint.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class SyncDownloadDataProductTest
    {
        private static final Long SYNC_SIZE_LIMIT = Long.valueOf(50000);

        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Rule
        public TemporaryFolder cacheDir = new TemporaryFolder();

        @Rule
        public TemporaryFolder ngasDir = new TemporaryFolder();

        private String secretKey;

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        @Mock
        private DataAccessJdbcRepository dataAccessJdbcRepository;
        
        @Mock
        private ImageCubeRepository imageCubeRepository;

        @Mock
        private CachedFileRepository cachedFileRepository;
        
        @Mock
        private ImageCutoutRepository imageCutoutRepository;

        @Mock
        private GeneratedSpectrumRepository generatedSpectrumRepository;

        @Mock
        private NgasService ngasService;
        
        @Mock
        private CasdaMailService casdaMailService;

        @Mock
        private DownloadManager downloadManager;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            secretKey = RandomStringUtils.randomAscii(16);
            String archiveStatusCommandAndArgs = TestUtils.getCommandAndArgsElStringForEchoOutput("DUL");
            DataAccessService dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                    mock(MeasurementSetRepository.class), mock(SpectrumRepository.class), 
                    mock(MomentMapRepository.class), mock(CubeletRepository.class), mock(EncapsulationFileRepository.class), 
                    mock(EvaluationFileRepository.class),
                    mock(ThumbnailRepository.class), cachedFileRepository, ngasService,
                    cacheDir.getRoot().getAbsolutePath(), 25, 1000, archiveStatusCommandAndArgs, "", "", 
                    new JavaProcessJobFactory(), mock(CacheManager.class), dataAccessJdbcRepository, 
                    imageCutoutRepository, generatedSpectrumRepository, casdaMailService, downloadManager);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    dataAccessService, accessJobManager, dataAccessJobRepository, "http://localhost:8088/foo",
                    secretKey, RandomUtils.nextInt(10, 20), 10, 20, SYNC_SIZE_LIMIT);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
            when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(createCount());
        }

        @Test
        public void testFileInCache() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            int sbid = 445;
            String filename = "image_cube-123.fits";
            String fileId = "observations-" + sbid + "-image_cubes-" + filename;

            String requestId = UUID.randomUUID().toString();
            long jobId = 123456;
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(requestId);
            job.setId(jobId);
            job.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(getPaging());
            when(dataAccessJdbcRepository.getPageOfDownloadFiles(any(String.class), any(String.class), 
            		any(Integer[].class), any(Boolean.class))).thenReturn(getImageCubes(123L, sbid));
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            createCachedDataFile(requestId, fileId, "ABC123 data", FileType.IMAGE_CUBE);
            when(accessJobManager.createDataAccessJob(any(), eq(SYNC_SIZE_LIMIT), eq(true))).thenReturn(job);
            when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(requestId,
                    ExecutionPhase.COMPLETED, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                    new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(),
                    new DateTime(DateTimeZone.UTC).plusDays(2), job.getParamMap(), Arrays.asList(), null));
            when(imageCubeRepository.findOne(1L)).thenReturn(new ImageCube());
            doNothing().when(accessJobManager).scheduleJob(any());

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_SYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id))
                    .andExpect(status().isOk())
                    .andExpect(header().string("Content-Disposition", "attachment; filename=" + fileId))
                    .andExpect(content().string(is(equalTo("ABC123 data")))).andReturn();
        }

        @Test
        public void testFileInCacheCheckTimesOutTriesNgas() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            int sbid = 445;
            String filename = "image_cube-123.fits";
            String fileId = "observations-" + sbid + "-image_cubes-" + filename;

            String requestId = UUID.randomUUID().toString();
            long jobId = 123456;
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(requestId);
            job.setId(jobId);
            job.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            ImageCube imageCube = AccessJobManagerTest.createImageCube(123L, filename, 123L, "ABC123", sbid);
            job.addImageCube(imageCube);
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(imageCubeRepository.findOne(Mockito.anyLong())).thenReturn(imageCube);

            /*
             * Create a cached file. This shouldn't be found because the cache check job will still be executing
             */
            createCachedDataFile(requestId, fileId, "ABC123 data", FileType.IMAGE_CUBE);

            /*
             * Create a file in 'ngas'
             */
            File dataFile = new File(ngasDir.getRoot(), fileId);
            dataFile.getParentFile().mkdirs();
            FileUtils.writeStringToFile(dataFile, "ABC123 data");

            when(accessJobManager.createDataAccessJob(any(), eq(SYNC_SIZE_LIMIT), eq(true))).thenReturn(job);
            when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(requestId,
                    ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                    new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(),
                    new DateTime(DateTimeZone.UTC).plusDays(2), job.getParamMap(), Arrays.asList(), null));
            NgasService.Status ngasStatus = mock(NgasService.Status.class);
            when(ngasService.getStatus(fileId)).thenReturn(ngasStatus);
            when(ngasStatus.wasSuccess()).thenReturn(true);
            when(ngasStatus.getMountPoint()).thenReturn(ngasDir.getRoot().getPath());
            when(ngasStatus.getFileName()).thenReturn(fileId);

            doNothing().when(accessJobManager).scheduleJob(any());

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_SYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id))
                    .andExpect(status().isOk())
                    .andExpect(header().string("Content-Disposition", "attachment; filename=" + fileId))
                    .andExpect(content().string(is(equalTo("ABC123 data")))).andReturn();
        }

        @Test
        public void testFileNotInCacheTriesNgas() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            int sbid = 445;
            String filename = "image_cube-123.fits";
            String fileId = "observations-" + sbid + "-image_cubes-" + filename;

            String requestId = UUID.randomUUID().toString();
            long jobId = 123456;
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(requestId);
            job.setId(jobId);
            job.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            ImageCube imageCube = AccessJobManagerTest.createImageCube(123L, filename, 123L, "ABC123", sbid);
            job.addImageCube(imageCube);
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(imageCubeRepository.findOne(Mockito.anyLong())).thenReturn(imageCube);

            /*
             * Create a file in 'ngas'
             */
            File dataFile = new File(ngasDir.getRoot(), fileId);
            dataFile.getParentFile().mkdirs();
            FileUtils.writeStringToFile(dataFile, "ABC123 data");

            when(accessJobManager.createDataAccessJob(any(), eq(SYNC_SIZE_LIMIT), eq(true))).thenReturn(job);
            when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(requestId,
                    ExecutionPhase.ERROR, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                    new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(),
                    new DateTime(DateTimeZone.UTC).plusDays(2), job.getParamMap(), Arrays.asList(), null));
            NgasService.Status ngasStatus = mock(NgasService.Status.class);
            when(ngasService.getStatus(fileId)).thenReturn(ngasStatus);
            when(ngasStatus.wasSuccess()).thenReturn(true);
            when(ngasStatus.getMountPoint()).thenReturn(ngasDir.getRoot().getPath());
            when(ngasStatus.getFileName()).thenReturn(fileId);

            doNothing().when(accessJobManager).scheduleJob(any());

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_SYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id))
                    .andExpect(status().isOk())
                    .andExpect(header().string("Content-Disposition", "attachment; filename=" + fileId))
                    .andExpect(content().string(is(equalTo("ABC123 data")))).andReturn();
        }

        @Test
        public void testFileInCacheCheckTimesOutAndFailsIfCancelFails() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            int sbid = 445;
            String filename = "image_cube-123.fits";

            String requestId = UUID.randomUUID().toString();
            long jobId = 123456;
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(requestId);
            job.setId(jobId);
            job.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            ImageCube imageCube = AccessJobManagerTest.createImageCube(123L, filename, 123L, "ABC123", sbid);
            job.addImageCube(imageCube);
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(imageCubeRepository.findOne(Mockito.anyLong())).thenReturn(imageCube);

            when(accessJobManager.createDataAccessJob(any(), eq(SYNC_SIZE_LIMIT), eq(true))).thenReturn(job);
            when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(requestId,
                    ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                    new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(),
                    new DateTime(DateTimeZone.UTC).plusDays(2), job.getParamMap(), Arrays.asList(), null));

            doNothing().when(accessJobManager).scheduleJob(any());
            doThrow(new ResourceIllegalStateException("some problem")).when(accessJobManager).cancelJob(any(),
                    any(DateTime.class));

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(RuntimeException.class)));
            exception.expectCause(hasMessage(
                    equalTo("Could not cancel job created to refresh SODA SYNC files in request after timeout. "
                            + "Job with requestId '" + requestId + "' is still in phase 'EXECUTING'")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_SYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id));
        }

        @Test
        public void testFileInCacheUnreadableThrowsException() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            int sbid = 445;
            String filename = "image_cube-123.fits";
            String fileId = "observations-" + sbid + "-image_cubes-" + filename;

            String requestId = UUID.randomUUID().toString();
            long jobId = 123456;
            DataAccessJob job = new DataAccessJob();
            job.setRequestId(requestId);
            job.setId(jobId);
            job.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            job.addImageCube(AccessJobManagerTest.createImageCube(123L, filename, 123L, "ABC123", 445));
            when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
            when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(getPaging());
            when(dataAccessJdbcRepository.getPageOfDownloadFiles(any(String.class), any(String.class), 
            		any(Integer[].class), any(Boolean.class))).thenReturn(getImageCubes(123L, sbid));
            File cachedFile = createCachedDataFile(requestId, fileId, "ABC123 data", FileType.IMAGE_CUBE);
            TestUtils.makeFileUnreadable(cachedFile);

            when(accessJobManager.createDataAccessJob(any(), eq(SYNC_SIZE_LIMIT), eq(true))).thenReturn(job);
            when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(requestId,
                    ExecutionPhase.COMPLETED, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                    new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(),
                    new DateTime(DateTimeZone.UTC).plusDays(2), job.getParamMap(), Arrays.asList(), null));
            when(imageCubeRepository.findOne(1L)).thenReturn(new ImageCube());

            NgasService.Status ngasStatus = mock(NgasService.Status.class);
            when(ngasService.getStatus(fileId)).thenReturn(ngasStatus);
            when(ngasStatus.wasSuccess()).thenReturn(true);
            when(ngasStatus.getMountPoint()).thenReturn(ngasDir.getRoot().getPath());
            when(ngasStatus.getFileName()).thenReturn(fileId);

            doNothing().when(accessJobManager).scheduleJob(any());

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(RuntimeException.class)));
            exception.expectCause(hasMessage(containsString(fileId)));

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_SYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id));
        }

        private File createCachedDataFile(String requestId, String filename, String contents, FileType fileType)
                throws IOException
        {
            File dataFile = createDataFile(requestId, filename, contents);
            CachedFile cachedFile = mock(CachedFile.class);
            when(cachedFileRepository.findByFileId(filename)).thenReturn(cachedFile);
            when(cachedFile.getFileType()).thenReturn(fileType);
            return dataFile;
        }

        private File createDataFile(String requestId, String filename, String contents) throws IOException
        {
            File dataFile = getFileForCachedJob(requestId, filename);
            dataFile.getParentFile().mkdirs();
            FileUtils.writeStringToFile(dataFile, contents);
            return dataFile;
        }

        private File getFileForCachedJob(String requestId, String filename)
        {
            return new File(new File(new File(cacheDir.getRoot(), "jobs"), requestId), filename);
        }

    }

    /**
     * Test cases for the createJob end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class CreateJobTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        private String secretKey;

        @Mock
        private DataAccessService dataAccessService;

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            secretKey = RandomStringUtils.randomAscii(16);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    dataAccessService, accessJobManager, dataAccessJobRepository, "http://localhost:8088/foo",
                    secretKey, RandomUtils.nextInt(10, 20), 500, 5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdRequired() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id is a required parameter")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED));
        }

        @Test
        public void testIdNotDecryptable() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id 'foo' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", "foo"));
        }

        @Test
        public void testBase64EncodedIdNotDecryptable() throws Exception
        {
            String id = Base64.encodeBase64String("foo".getBytes(Charset.forName("UTF-8")));

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedUnstructuredId() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("foo", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdMissingElement() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("a|b", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdTooManyElements() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("a|b|c|d|e", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdBadTimestamp() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|c", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdBadDataProductType() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cubey-1|myUserId|OPAL|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdUnsupportedDataProductType() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("catalogue-1|myUserId|OPAL|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("Unsupported data access product type catalogue")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdBadDataProductId() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-a|myUserId|OPAL|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id + "' is not valid")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdUnknownDataProduct() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("No resource found matching id '" + id + "'")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id));
        }

        @Test
        public void testEncryptedIdKnownCubeDataProduct() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 1L)))
                    .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            String requestId = "abcd-1234";
            when(dataAccessJob.getRequestId()).thenReturn(requestId);
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id))
                    .andExpect(status().isSeeOther())
                    .andExpect(header().string("Location", is(equalTo(
                            "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + requestId))))
                    .andExpect(content().string(is(equalTo(requestId))));

            ArgumentCaptor<JobDto> jobDetails = ArgumentCaptor.forClass(JobDto.class);

            verify(this.accessJobManager).createDataAccessJob(jobDetails.capture(), eq(null), eq(false));

            assertThat(jobDetails.getAllValues().size(), is(equalTo(1)));
            JobDto jobDto = jobDetails.getAllValues().get(0);
            assertThat(jobDto.getDownloadMode(), is(equalTo(CasdaDownloadMode.SODA_ASYNC_WEB)));
            assertThat(jobDto.getIds(), is(nullValue()));
            assertThat(jobDto.getParams().get("id"), is(arrayContaining(id)));
            assertThat(jobDto.getDownloadFormat(), is(nullValue()));
            assertThat(jobDto.getUserIdent(), is(equalTo("myUserId")));
            assertThat(jobDto.getUserLoginSystem(), is(equalTo("OPAL")));
            assertThat(jobDto.getUserEmail(), is(nullValue()));
            assertThat(jobDto.getUserName(), is(nullValue()));

            verify(this.accessJobManager, never()).scheduleJob(dataAccessJob.getRequestId());
        }

        @Test
        public void testEncryptedIdKnownVisibilityDataProduct() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService
                    .findDataProduct(new DataAccessDataProduct(DataAccessProductType.visibility, 1L)))
                            .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            String requestId = "abcd-1234";
            when(dataAccessJob.getRequestId()).thenReturn(requestId);
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id))
                    .andExpect(status().isSeeOther())
                    .andExpect(header().string("Location", is(equalTo(
                            "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + requestId))))
                    .andExpect(content().string(is(equalTo(requestId))));

            ArgumentCaptor<JobDto> jobDetails = ArgumentCaptor.forClass(JobDto.class);

            verify(this.accessJobManager).createDataAccessJob(jobDetails.capture(), eq(null), eq(false));

            assertThat(jobDetails.getAllValues().size(), is(equalTo(1)));
            JobDto jobDto = jobDetails.getAllValues().get(0);
            assertThat(jobDto.getDownloadMode(), is(equalTo(CasdaDownloadMode.SODA_ASYNC_WEB)));
            assertThat(jobDto.getIds(), is(nullValue()));
            assertThat(jobDto.getParams().get("id"), is(arrayContaining(id)));
            assertThat(jobDto.getDownloadFormat(), is(nullValue()));
            assertThat(jobDto.getUserIdent(), is(equalTo("myUserId")));
            assertThat(jobDto.getUserLoginSystem(), is(equalTo("NEXUS")));
            assertThat(jobDto.getUserEmail(), is(nullValue()));
            assertThat(jobDto.getUserName(), is(nullValue()));

            verify(this.accessJobManager, never()).prepareAsyncJobToStart(eq(requestId), any(), any());
            verify(this.accessJobManager, never()).scheduleJob(dataAccessJob.getRequestId());
        }

        @Test
        public void testEncryptedIdKnownCubeDataProductWithFiltersSavesAllParams() throws Exception
        {
            String id = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 1L)))
                    .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            String requestId = "abcd-1234";
            when(dataAccessJob.getRequestId()).thenReturn(requestId);
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("id", id)
                            .param("POS", new String[] { "CIRCLE 1 1 1" }).param("POL", new String[] { "U", "Q" })
                            .param("COORD", "axis 20 40"))
                    .andExpect(status().isSeeOther())
                    .andExpect(header().string("Location", is(equalTo(
                            "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + requestId))))
                    .andExpect(content().string(is(equalTo(requestId))));

            ArgumentCaptor<JobDto> jobDetails = ArgumentCaptor.forClass(JobDto.class);

            verify(this.accessJobManager).createDataAccessJob(jobDetails.capture(), eq(null), eq(false));

            assertThat(jobDetails.getAllValues().size(), is(equalTo(1)));
            JobDto jobDto = jobDetails.getAllValues().get(0);
            assertThat(jobDto.getDownloadMode(), is(equalTo(CasdaDownloadMode.SODA_ASYNC_WEB)));
            assertThat(jobDto.getIds(), is(nullValue()));
            assertThat(jobDto.getParams().get("id"), is(arrayContaining(id)));
            assertThat(jobDto.getDownloadFormat(), is(nullValue()));
            assertThat(jobDto.getUserIdent(), is(equalTo("myUserId")));
            assertThat(jobDto.getUserLoginSystem(), is(equalTo("OPAL")));
            assertThat(jobDto.getUserEmail(), is(nullValue()));
            assertThat(jobDto.getUserName(), is(nullValue()));
            assertThat(jobDto.getParams().get("pos"), arrayContaining("CIRCLE 1 1 1"));
            assertThat(jobDto.getParams().get("pol"), arrayContainingInAnyOrder("U", "Q"));
            assertThat(jobDto.getParams().get("coord"), arrayContainingInAnyOrder("axis 20 40"));

            verify(this.accessJobManager, never()).scheduleJob(dataAccessJob.getRequestId());
        }

        @Test
        public void testEncryptedIdMultipleDataProducts() throws Exception
        {
            String id1 = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            String id2 = Utils.encryptAesUrlSafe("visibility-1|myUserId|OPAL|WEB|123", secretKey);
            String id3 = Utils.encryptAesUrlSafe("cube-2|myUserId|OPAL|WEB|123", secretKey);

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 1L)))
                    .thenReturn(entity);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 2L)))
                    .thenReturn(entity);
            when(this.dataAccessService
                    .findDataProduct(new DataAccessDataProduct(DataAccessProductType.visibility, 1L)))
                            .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            String requestId = "abcd-1234";
            when(dataAccessJob.getRequestId()).thenReturn(requestId);
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                            .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id1).param("id", id2)
                            .param("Id", id3))
                    .andExpect(status().isSeeOther())
                    .andExpect(header().string("Location", is(equalTo(
                            "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + requestId))))
                    .andExpect(content().string(is(equalTo(requestId))));

            ArgumentCaptor<JobDto> jobDetails = ArgumentCaptor.forClass(JobDto.class);

            verify(this.accessJobManager).createDataAccessJob(jobDetails.capture(), eq(null), eq(false));

            assertThat(jobDetails.getAllValues().size(), is(equalTo(1)));
            JobDto jobDto = jobDetails.getAllValues().get(0);
            assertThat(jobDto.getDownloadMode(), is(equalTo(CasdaDownloadMode.SODA_ASYNC_WEB)));
            assertThat(jobDto.getIds(), is(nullValue()));
            assertThat(jobDto.getParams().get("id"), is(arrayContaining(id1, id2, id3)));
            assertThat(jobDto.getDownloadFormat(), is(nullValue()));
            assertThat(jobDto.getUserIdent(), is(equalTo("myUserId")));
            assertThat(jobDto.getUserLoginSystem(), is(equalTo("OPAL")));
            assertThat(jobDto.getUserEmail(), is(nullValue()));
            assertThat(jobDto.getUserName(), is(nullValue()));

            verify(this.accessJobManager, never()).scheduleJob(Mockito.anyString());
        }

        @Test
        public void testEncryptedIdMultipleDataProductsUserLoginSystemDontMatch() throws Exception
        {
            String id1 = Utils.encryptAesUrlSafe("cube-1|myUserId|OPAL|WEB|123", secretKey);
            String id2 = Utils.encryptAesUrlSafe("visibility-1|myUserId|NEXUS|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id2 + "' is not valid")));

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 1L)))
                    .thenReturn(entity);
            when(this.dataAccessService
                    .findDataProduct(new DataAccessDataProduct(DataAccessProductType.visibility, 1L)))
                            .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.getRequestId()).thenReturn("abcd-1234");
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id1).param("id", id2));
        }

        @Test
        public void testEncryptedIdMultipleDataProductsUserIdDontMatch() throws Exception
        {
            String id1 = Utils.encryptAesUrlSafe("cube-1|myUserId1|OPAL|WEB|123", secretKey);
            String id2 = Utils.encryptAesUrlSafe("visibility-1|myUserId2|OPAL|WEB|123", secretKey);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id '" + id2 + "' is not valid")));

            CasdaDataProductEntity entity = mock(CasdaDataProductEntity.class);
            when(this.dataAccessService.findDataProduct(new DataAccessDataProduct(DataAccessProductType.cube, 1L)))
                    .thenReturn(entity);
            when(this.dataAccessService
                    .findDataProduct(new DataAccessDataProduct(DataAccessProductType.visibility, 1L)))
                            .thenReturn(entity);

            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.getRequestId()).thenReturn("abcd-1234");
            when(this.accessJobManager.createDataAccessJob(Mockito.any(JobDto.class), eq(null), eq(false)))
                    .thenReturn(dataAccessJob);

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED).param("ID", id1).param("id", id2));
        }
    }

    /**
     * Test cases for the getJob end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class GetJobTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
            UWSJob.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo"));
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

        @Test
        public void testNonExpiredJob() throws Exception
        {
            String jobId = "iReSuKZ9D7LTNcMLcoYA";
            DateTime created = new DateTime("2015-11-21T01:34:55.245+00:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createImageCube(12111, 12L, "image.fits")),
                    Arrays.asList(createMeasurementSet(121112, 15L, "visibility.fits")), null, null, null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            dataAccessJob.getParamMap().add(ParamKeyWhitelist.ID.name(), "secretid1");
            when(accessJobManager.getJobStatus(dataAccessJob))
                    .thenReturn(AccessJobManager.createUWSJob(jobId, ExecutionPhase.QUEUED, created.toDate().getTime(),
                            0l, null, dataAccessJob.getParamMap(), Arrays.asList(), null));

            MvcResult result = this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId))//
                    .andExpect(status().isOk()) //
                    .andExpect(content().contentType(MediaType.APPLICATION_XML)) //
                    .andDo(print()) //
                    .andReturn();
            checkXmlAgainstTestCaseFile("controller.get_job_status", result.getResponse().getContentAsString());
        }

    }

    /**
     * Test cases for the setJobPhase end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class SetJobPhaseTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        private int cancelledJobHoursToExpiry;

        private String secretKey = RandomStringUtils.randomAscii(16);

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            cancelledJobHoursToExpiry = RandomUtils.nextInt(10, 20);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", secretKey, cancelledJobHoursToExpiry, 500, 5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo/phase"));
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

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase"));
        }

        @Test
        public void testMissingPhaseParam() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("phase is a required parameter")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase"));
        }

        @Test
        public void testPhaseParamNotRunOrAbort() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("phase parameter must have value RUN or ABORT")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase")
                    .param("pHAsE", "run")); // case-sensitive value
        }

        @Test
        public void testMultiplePhaseParams() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("only a single phase parameter is allowed")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase")
                    .param("pHAsE", "RUN").param("pHAsE", "RUN")); // case-sensitive value
        }

        @Test
        public void testAbort() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            UWSJob job = new UWSJob(new UWSParameters());
            job.setPhase(ExecutionPhase.PENDING);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(job);
            
            DateTime approximateCancelTime = new DateTime(DateTimeZone.UTC).plusHours(cancelledJobHoursToExpiry);
            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase")
                            .param("pHAsE", "ABORT")) //
                    .andExpect(status().isSeeOther()) //
                    .andExpect(content().string(is(emptyOrNullString()))) //
                    .andExpect(header().doesNotExist("Content-Type")) //
                    .andExpect(header().string("Location", is(equalTo(
                            "http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId))));

            ArgumentCaptor<DateTime> cancelTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
            verify(accessJobManager, Mockito.times(1)).cancelJob(Mockito.eq(jobId), cancelTimeCaptor.capture());
            assertThat(new Period(approximateCancelTime, cancelTimeCaptor.getValue()).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        }
        
        @Test
        public void testAbortFailure()
        {
            try
            {
                String jobId = RandomStringUtils.randomAlphanumeric(20);
                DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
                DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                        Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                        null);
                when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
                UWSJob job = new UWSJob(new UWSParameters());
                job.setPhase(ExecutionPhase.ERROR);
                when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(job);
                
                this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase")
                                .param("pHAsE", "ABORT"));
                
                fail();
            }
            catch(Exception e)
            {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof BadRequestException);
                assertEquals("Only Jobs which are PENDING, QUEUED or EXECUTING can be aborted", cause.getMessage());
            }

        }
        
        @Test
        public void testRunFailure()
        {
            try
            {
                String jobId = RandomStringUtils.randomAlphanumeric(20);
                DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
                DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                        Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                        null);
                when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
                UWSJob job = new UWSJob(new UWSParameters());
                job.setPhase(ExecutionPhase.QUEUED);
                when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(job);
                
                this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase")
                                .param("pHAsE", "RUN"));
                
                fail();
            }
            catch(Exception e)
            {
                Throwable cause = e.getCause();
                assertTrue(cause instanceof BadRequestException);
                assertEquals("Only Jobs which are PENDING can be run", cause.getMessage());
            }
        }
    }
    

    /**
     * Test cases for the deleteJobViaAcion end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class DeleteJobViaActionTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        private int cancelledJobHoursToExpiry;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            cancelledJobHoursToExpiry = RandomUtils.nextInt(10, 20);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), cancelledJobHoursToExpiry, 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo").param("ACTION", "DELETE"));
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

            this.mockMvc.perform(
                    post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId).param("ACTION", "DELETE"));
        }

        @Test
        public void testMissingActionParam() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("action is a required parameter")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId));
        }

        @Test
        public void testActionParamNotDelete() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("action parameter must have value DELETE")));

            this.mockMvc.perform(
                    post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId).param("AcTIon", "run"));
        }

        @Test
        public void testMultipleActionParams() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("only a single action parameter is allowed")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId)
                    .param("AcTIon", "DELETE").param("actION", "DELETE")); // case-sensitive value
        }

        @Test
        public void testDelete() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            DateTime approximateCancelTime = new DateTime(DateTimeZone.UTC);
            this.mockMvc
                    .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId).param("ACTion",
                            "DELETE")) //
                    .andExpect(status().isSeeOther()) //
                    .andExpect(content().string(is(emptyOrNullString()))) //
                    .andExpect(header().doesNotExist("Content-Type")) //
                    .andExpect(status().isSeeOther()).andExpect(header().string("Location",
                            is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))));

            ArgumentCaptor<DateTime> cancelTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
            verify(accessJobManager, Mockito.times(1)).cancelJob(Mockito.eq(jobId), cancelTimeCaptor.capture());
            assertThat(new Period(approximateCancelTime, cancelTimeCaptor.getValue()).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    /**
     * Test cases for the deleteJobViaAcion end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class DeleteJobViaHttpDeleteTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        private int cancelledJobHoursToExpiry;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            cancelledJobHoursToExpiry = RandomUtils.nextInt(10, 20);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), cancelledJobHoursToExpiry, 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc.perform(delete(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo"));
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

            this.mockMvc.perform(delete(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId));
        }

        @Test
        public void testDelete() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DateTime created = new DateTime("2015-11-21T12:34:55.245+11:00");
            DataAccessJob dataAccessJob = createDataAccessJob(jobId, DataAccessJobStatus.PREPARING, created,
                    Arrays.asList(createSampleImageCube()), Arrays.asList(createSampleMeasurementSet()), null, null,
                    null);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            DateTime approximateCancelTime = new DateTime(DateTimeZone.UTC);
            this.mockMvc.perform(delete(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId)) //
                    .andExpect(status().isSeeOther()) //
                    .andExpect(content().string(is(emptyOrNullString()))) //
                    .andExpect(header().doesNotExist("Content-Type")) //
                    .andExpect(header().string("Location",
                            is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH))));

            ArgumentCaptor<DateTime> cancelTimeCaptor = ArgumentCaptor.forClass(DateTime.class);
            verify(accessJobManager, Mockito.times(1)).cancelJob(Mockito.eq(jobId), cancelTimeCaptor.capture());
            assertThat(new Period(approximateCancelTime, cancelTimeCaptor.getValue()).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    /**
     * Test cases for the updateJobDestructionTime end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class UpdateJobDestructionTimeTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            // testAppender = Log4JTestAppender.createAppender();
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo/destruction"));
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

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/destruction"));
        }

        @Test
        public void testJobDestruction() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            MvcResult result =
                    this.mockMvc
                            .perform(post(
                                    AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/destruction"))
                            .andExpect(status().isSeeOther()) //
                            .andExpect(content().string(is(emptyOrNullString()))) //
                            .andExpect(header().doesNotExist("Content-Type")) //
                            .andExpect(header().string("Location",
                                    is(equalTo("http://localhost" + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH
                                            + "/" + jobId)))) //
                    .andReturn();
            assertThat(result.getResponse().getContentAsString(), is(""));
        }

    }

    /**
     * Test cases for the updateJobDestructionTime end-point.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class UpdateJobExecutionDurationTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            // testAppender = Log4JTestAppender.createAppender();
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testIdForNonExistentJob() throws Exception
        {
            when(dataAccessJobRepository.findByRequestId(Mockito.any())).thenReturn(null);

            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(ResourceNotFoundException.class)));
            exception.expectCause(hasMessage(equalTo("No job with id 'foo'")));

            this.mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/foo/executionduration"));
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

            this.mockMvc.perform(
                    post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/executionduration"));
        }

        @Test
        public void testJobExecutionDuration() throws Exception
        {
            String jobId = RandomStringUtils.randomAlphanumeric(20);
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);

            MvcResult result =
                    this.mockMvc
                            .perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId
                                    + "/executionduration"))
                            .andExpect(status().isSeeOther()) //
                            .andExpect(content().string(is(emptyOrNullString()))) //
                            .andExpect(header().doesNotExist("Content-Type")) //
                            .andExpect(
                                    header().string("Location",
                                            is(equalTo("http://localhost"
                                                    + AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId))))
                    .andReturn();
            assertThat(result.getResponse().getContentAsString(), is(""));
        }

    }

    /**
     * Test cases for the validate parameters for params end-points.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class ValidateParametersTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        @Mock
        private DataAccessService dataAccessService;

        private AccessDataController controller;

        private MockMvc mockMvc;

        private String secretKey = RandomStringUtils.randomAscii(16);

        private String jobId;

        private DataAccessJob dataAccessJob;

        private UWSJob uwsJob;

        private String dataProductId;
        private String dataProductId2;
        private String imageCubeId;
        private ImageCube imageCube;
        private MeasurementSet measurementSet;

        @Before
        public void setUp() throws Exception
        {
            // testAppender = Log4JTestAppender.createAppender();
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    dataAccessService, accessJobManager, dataAccessJobRepository, "http://localhost:8088/foo",
                    secretKey, RandomUtils.nextInt(10, 20), 500, 5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();

            jobId = UUID.randomUUID().toString();
            dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.setUserIdent("differentUserId");
            dataAccessJob.setUserLoginSystem("OPAL");
            dataAccessJob.setSizeKb(1023L);

            imageCube = new ImageCube();
            imageCube.setId(1L);
            imageCube.setFilesize(12L);

            measurementSet = new MeasurementSet();
            measurementSet.setId(1L);
            measurementSet.setFilesize(17L);

            imageCubeId = Utils.encryptAesUrlSafe("cube-1|differentUserId|OPAL|WEB|123", secretKey);
            dataAccessJob.getParamMap().add("id", imageCubeId);

            uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.PENDING, 0l, 0l, null,
                    dataAccessJob.getParamMap(), null, null);
            dataProductId = Utils.encryptAesUrlSafe("cube-2|myUserId|NEXUS|WEB|123", secretKey);
            dataProductId2 = Utils.encryptAesUrlSafe("cube-2|differentUserId|OPAL|WEB|123", secretKey);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);
            when(dataAccessService.findDataProduct(new DataAccessDataProduct("cube-1"))).thenReturn(imageCube);
            when(dataAccessService.findDataProduct(new DataAccessDataProduct("visibility-1")))
                    .thenReturn(measurementSet);
        }

        @Test
        public void testInvalidIdsAddParameter() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id 'cube-1' is not valid;id 'invalid' is not valid;id '"
                    + dataProductId + "' is not valid;No resource found matching id '" + dataProductId2 + "'")));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters/id")
                    .param("value", "cube-1").param("value", "invalid").param("value", dataProductId)
                    .param("value", dataProductId2));
        }

        @Test
        public void testInvalidIdsUpdateParameters() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("id 'cube-1' is not valid;id 'invalid' is not valid;id '"
                    + dataProductId + "' is not valid;No resource found matching id '" + dataProductId2 + "'")));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters")
                    .param("id", "cube-1").param("id", "invalid").param("id", dataProductId)
                    .param("id", dataProductId2));
        }

        @Test
        public void testInvalidPosPolCoordParams() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(allOf(containsString("UsageFault: Invalid POS value invalidpos"),
                    containsString("UsageFault: Invalid POL value invalidpol"),
                    containsString("UsageFault: Invalid COORD value invalidcoord"))));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters")
                    .param("pos", "invalidpos").param("pol", "invalidpol").param("coord", "invalidcoord"));
        }

        @Test
        public void testInvalidPosNamedParams() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("UsageFault: Invalid POS value invalidpos")));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters/pos")
                    .param("value", "invalidpos"));
        }

        @Test
        public void testInvalidPolNamedParams() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("UsageFault: Invalid POL value invalidpol")));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters/pol")
                    .param("value", "invalidpol"));
        }

        @Test
        public void testInvalidCoordNamedParams() throws Exception
        {
            exception.expect(NestedServletException.class);
            exception.expectCause(is(instanceOf(BadRequestException.class)));
            exception.expectCause(hasMessage(equalTo("UsageFault: Invalid COORD value invalidcoord")));

            mockMvc.perform(post(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/parameters/COORD")
                    .param("value", "invalidcoord"));
        }

    }

    private static ImageCube createImageCube(int sbid, long id, String filename)
    {
        ImageCube imageCube = new ImageCube();
        imageCube.setId(id);
        imageCube.setFilename(filename);
        Observation observation = new Observation(sbid);
        observation.addImageCube(imageCube);
        return imageCube;
    }

    private static ImageCube createSampleImageCube()
    {
        return createImageCube(RandomUtils.nextInt(1, Integer.MAX_VALUE), RandomUtils.nextLong(1, Long.MAX_VALUE),
                RandomStringUtils.randomAlphanumeric(20));
    }

    private static MeasurementSet createMeasurementSet(int sbid, long id, String filename)
    {
        MeasurementSet measurementSet = new MeasurementSet();
        measurementSet.setId(id);
        measurementSet.setFilename(filename);
        Observation observation = new Observation(sbid);
        observation.addMeasurementSet(measurementSet);
        return measurementSet;
    }

    private static MeasurementSet createSampleMeasurementSet()
    {
        return createMeasurementSet(RandomUtils.nextInt(1, Integer.MAX_VALUE), RandomUtils.nextLong(1, Long.MAX_VALUE),
                RandomStringUtils.randomAlphanumeric(20));
    }

    private static DataAccessJob createDataAccessJob(String jobId, DataAccessJobStatus status, DateTime created,
            List<ImageCube> imageCubes, List<MeasurementSet> measurementSets, DateTime lastModified, DateTime finished,
            DateTime expiry)
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId(jobId);
        dataAccessJob.setCreatedTimestamp(created);
        dataAccessJob.setLastModified(lastModified);
        dataAccessJob.setAvailableTimestamp(finished);
        dataAccessJob.setExpiredTimestamp(expiry);
        dataAccessJob.setStatus(status);
        for (ImageCube imageCube : imageCubes)
        {
            dataAccessJob.addImageCube(imageCube);
        }
        for (MeasurementSet measurementSet : measurementSets)
        {
            dataAccessJob.addMeasurementSet(measurementSet);
        }
        return dataAccessJob;
    }

    /**
     * 
     * Test cases for Job informational end points.
     * <p>
     * Copyright 2015, CSIRO Australia All rights reserved.
     *
     */
    public static class JobInformationalEndPointsTest
    {
        @Rule
        public ExpectedException exception = ExpectedException.none();

        @Mock
        HealthEndpoint healthEndpoint;

        @Mock
        SystemStatus systemStatus;

        @Mock
        private DataAccessService dataAccessService;

        @Mock
        private GenerateFileService generateFileService;

        @Mock
        private AccessJobManager accessJobManager;

        @Mock
        private DataAccessJobRepository dataAccessJobRepository;

        private AccessDataController controller;

        private MockMvc mockMvc;

        @Before
        public void setUp() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            controller = new AccessDataController(mock(HealthEndpoint.class), mock(SystemStatus.class),
                    mock(DataAccessService.class), accessJobManager, dataAccessJobRepository,
                    "http://localhost:8088/foo", RandomStringUtils.randomAscii(16), RandomUtils.nextInt(10, 20), 500,
                    5000, 100000);
            this.mockMvc = MockMvcBuilders.standaloneSetup(controller)
                    .setHandlerExceptionResolvers(new ExceptionHandlerExceptionResolver()).build();
        }

        @Test
        public void testGetJobPhase() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.EXECUTING, 0l, 0l, null,
                    dataAccessJob.getParamMap(), null, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);

            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/phase"))
                    .andExpect(status().isOk()).andExpect(content().contentType(MediaType.TEXT_PLAIN))
                    .andExpect(content().string(ExecutionPhase.EXECUTING.name())).andDo(print()).andReturn();
        }

        // Currently Job owner is not implemented so always null
        @Test
        public void testGetJobOwnerNoOwner() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.EXECUTING, 0l, 0l, null,
                    dataAccessJob.getParamMap(), null, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);
            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/owner"))
                    .andExpect(status().isOk()).andExpect(content().string("")).andDo(print()).andReturn();
        }

        @Test
        public void testGetJobOwner() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = mock(DataAccessJob.class);
            when(dataAccessJob.isExpired()).thenReturn(false);
            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            UWSJob uwsJob = new UWSJob(new DefaultJobOwner("Vivek"), new UWSParameters());
            uwsJob.setPhase(ExecutionPhase.EXECUTING);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);

            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/owner"))
                    .andExpect(status().isOk())
                    .andExpect(content()
                            .string(allOf(containsString("\"id\":\"Vivek\""), containsString("\"pseudo\":\"Vivek\""),
                                    containsString("\"dataToSave\":null"), containsString("\"allUserData\":null"))))
                    .andDo(print()).andReturn();
        }

        @Test
        public void testGetExecutionduration() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.EXECUTING, 0l, 0l, null,
                    dataAccessJob.getParamMap(), null, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);
            this.mockMvc
                    .perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/executionduration"))
                    .andExpect(status().isOk()).andExpect(content().contentType(MediaType.TEXT_PLAIN))
                    .andExpect(content().string("0")).andDo(print()).andReturn();
        }

        // We Don't support quote yet so its alwasys -1 i.e, unknown
        @Test
        public void testGetQuote() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.EXECUTING, 0l, 0l, null,
                    dataAccessJob.getParamMap(), null, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);
            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/quote"))
                    .andExpect(status().isOk()).andExpect(content().contentType(MediaType.TEXT_PLAIN))
                    .andExpect(content().string("-1")).andDo(print()).andReturn();
        }

        @Test
        public void testGetDestruction() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.COMPLETED, 0l, 0l,
                    new DateTime("2015-12-07T11:15:23.632Z"), dataAccessJob.getParamMap(), null, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);

            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/destruction"))
                    .andExpect(status().isOk()).andExpect(content().contentType(MediaType.TEXT_PLAIN))
                    .andExpect(content().string("2015-12-07T11:15:23.632Z")).andDo(print()).andReturn();
        }

        @Test
        public void testGetError() throws Exception
        {
            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.ERROR, 0l, 0l,
                    new DateTime("2015-12-07T11:15:23.632Z"), dataAccessJob.getParamMap(), null, new ErrorSummary(
                            "A problem occured obtaining access to the requested item(s)", ErrorType.FATAL, "none"));

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);

            this.mockMvc.perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/error"))
                    .andExpect(status().isOk()).andExpect(content().contentType(MediaType.TEXT_PLAIN))
                    .andExpect(content().string("ERROR_SUMMARY {type: FATAL; message: \"A problem occured "
                            + "obtaining access to the requested item(s)\"; details: none}"))
                    .andDo(print()).andReturn();
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Test
        public void testGetResults() throws Exception
        {
            List<Result> results = new ArrayList();
            results.add(new Result("Result1", "extended", "http://www.w3.org/TR/xlink/#linking-elements", true));
            results.add(new Result("Result3", "locator", "http:/google.coms", false));
            results.add(new Result("Result4", "arc", "http://bing.com", true));
            results.add(new Result("Result5", "resource", "http://yahoo.com", false));
            results.add(new Result("Result6", "none", "http://www.csiro.au", true));
            results.add(new Result("Result7", "title", "http://www.csiro.au", false));

            String jobId = "IvvNSxBsTcgSSr0h8SU0";
            DataAccessJob dataAccessJob = new DataAccessJob();
            dataAccessJob.setRequestId(jobId);
            dataAccessJob.getParamMap().add("id", "secretid");
            UWSJob uwsJob = AccessJobManager.createUWSJob(jobId, ExecutionPhase.COMPLETED, 0l, 0l,
                    new DateTime("2015-12-07T11:15:23.632Z"), dataAccessJob.getParamMap(), results, null);

            when(dataAccessJobRepository.findByRequestId(jobId)).thenReturn(dataAccessJob);
            when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(uwsJob);

            MvcResult result = this.mockMvc
                    .perform(get(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + jobId + "/results"))
                    .andExpect(status().isOk()).andDo(print()).andReturn();

            checkXmlAgainstTestCaseFile("controller.get_job_results", result.getResponse().getContentAsString());
        }
    }

    private static void checkXmlAgainstTestCaseFile(String testCase, String xml) throws SAXException, IOException
    {
        XMLUnit.setIgnoreWhitespace(true);
        XMLUnit.setIgnoreAttributeOrder(true);

        DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(
                FileUtils.readFileToString(new File("src/test/resources/siap2/" + testCase + ".xml")), xml));

        List<?> allDifferences = diff.getAllDifferences();
        assertEquals("Differences found: " + diff.toString(), 0, allDifferences.size());
    }
    
    private static Map<String, Object> getPaging()
    {
    	Map<String, Object> paging = new LinkedHashMap<String, Object>();
    	paging.put("IMAGE_CUBE", 1L);
    	paging.put("MEASUREMENT_SET", 0L);
    	paging.put("SPECTRUM", 0L);
    	paging.put("MOMENT_MAP", 0L);
    	paging.put("CUBELET", 0L);
    	paging.put("IMAGE_CUTOUT", 0L);
    	paging.put("GENERATED_SPECTRUM", 0L);
    	paging.put("ENCAPSULATION_FILE", 0L);
    	paging.put("CATALOGUE", 0L);
    	paging.put("ERROR", 0L);
        paging.put("EVALUATION_FILE", 0L);
    	return paging;
    }
    
    private static List<Map<String, Object>> getImageCubes(long imageId, int sbid)
    {
        List<Map<String, Object>> imageCubes = new ArrayList<Map<String, Object>>();
        Map<String, Object> imageCube = new HashMap<String, Object>();
        imageCube.put("id", imageId);
        imageCube.put("filesize", 27L);
        imageCube.put("filename", "image_cube-123.fits");
        imageCube.put("obsid", sbid);
        imageCube.put("l7id", null);
        imageCubes.add(imageCube);
        return imageCubes;
    }
}
