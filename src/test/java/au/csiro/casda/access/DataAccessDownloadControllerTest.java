package au.csiro.casda.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.allOf;

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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.collection.IsEmptyCollection.empty;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.util.NestedServletException;

import au.csiro.TestUtils;
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
import au.csiro.casda.access.soda.AccessDataController;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;

/**
 * Tests for the DataAccessDownload Controller
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
@PrepareForTest({ Files.class, DataAccessDownloadController.class })
@RunWith(PowerMockRunner.class)
public class DataAccessDownloadControllerTest
{
    @Rule
    public TemporaryFolder cacheDir = new TemporaryFolder();

    @Mock
    private AccessJobManager accessJobManager;

    private DataAccessDownloadController controller;

    private Log4JTestAppender testAppender;

    private MockMvc mockMvc;

    @Mock
    private DataAccessJobRepository dataAccessJobRepository;

    @Mock
    private ImageCubeRepository imageCubeRepository;

    @Mock
    private MeasurementSetRepository measurementSetRepository;

    @Mock
    private SpectrumRepository spectrumRepository;

    @Mock
    private MomentMapRepository momentMapRepository;

    @Mock
    private CubeletRepository cubeletRepository;

    @Mock
    private EncapsulationFileRepository encapsulationFileRepository;

    @Mock
    private EvaluationFileRepository evaluationFileRepository;

    @Mock
    private ThumbnailRepository thumbnailRepository;

    @Mock
    private ImageCutoutRepository imageCutoutRepository;

    @Mock
    private GeneratedSpectrumRepository generatedSpectrumRepository;

    @Mock
    private DataAccessJdbcRepository dataAccessJdbcRepository;

    @Mock
    private CachedFileRepository cachedFileRepository;

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private NgasService ngasService;

    @Mock
    private CasdaMailService casdaMailService;

    @Mock
    private DownloadManager downloadManager;

    @Before
    public void setUp() throws Exception
    {
        testAppender = Log4JTestAppender.createAppender();
        MockitoAnnotations.initMocks(this);
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, thumbnailRepository, cachedFileRepository,
                ngasService, cacheDir.getRoot().getAbsolutePath(), 25, 1000, "", "", "", mock(ProcessJobFactory.class),
                mock(CacheManager.class), dataAccessJdbcRepository, imageCutoutRepository, generatedSpectrumRepository,
                casdaMailService, downloadManager);
        controller = new DataAccessDownloadController(dataAccessJobRepository, dataAccessService, accessJobManager,
                "https://myserver:8080/maps/<project>/<file>", "/path/to/coverage/", "moc.fits", "preview.jpg",
                "https://casda-dev-app.csiro.au/maps/<hips_path>", "/ASKAP/archive/dev/vol002/maps/active/", "sssh");
        this.mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void testDownloadOKWeb() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits";
        FileType fileType = FileType.IMAGE_CUBE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        MvcResult result = this.mockMvc.perform(get("/requests/" + requestId + "/" + filename))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)).andReturn();

        assertEquals("blah blah", result.getResponse().getContentAsString());

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E040]"), containsString(requestId), containsString(filename)),
                sameInstance((Throwable) null));

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E041]"), containsString(requestId), containsString(filename),
                        containsString("volumeKB: 1]"), containsString("downloadMode: " + CasdaDownloadMode.WEB.name()),
                        containsString("fileType: " + fileType.toString())),
                sameInstance((Throwable) null));
    }

    @Test
    public void testDownloadOKSiapAsyncWeb() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits";
        FileType fileType = FileType.CATALOGUE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_WEB);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        MvcResult result = this.mockMvc.perform(get("/requests/" + requestId + "/" + filename))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)).andReturn();

        assertEquals("blah blah", result.getResponse().getContentAsString());

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E040]"), containsString(requestId), containsString(filename)),
                sameInstance((Throwable) null));

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E041]"), containsString(requestId), containsString(filename),
                        containsString("volumeKB: 1]"),
                        containsString("downloadMode: " + CasdaDownloadMode.SODA_ASYNC_WEB.name()),
                        containsString("fileType: " + fileType)),
                sameInstance((Throwable) null));
    }

    @Test
    public void testDownloadNotOKSiapAsyncPawsey() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits";
        FileType fileType = FileType.CATALOGUE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_PAWSEY);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        MvcResult result = this.mockMvc.perform(get("/requests/" + requestId + "/" + filename))
                .andExpect(status().isNotFound()).andReturn();
        assertEquals("No valid request found with request id " + requestId, result.getResolvedException().getMessage());
    }

    @Test
    public void testDownloadOKPawsey() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.txt";
        FileType fileType = null;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.PAWSEY_HTTP);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        MvcResult result = this.mockMvc.perform(get("/pawsey/requests/" + requestId + "/" + filename))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)).andReturn();

        // this is the content of the test file
        assertEquals("blah blah", result.getResponse().getContentAsString());

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E040]"), containsString(requestId), containsString(filename)),
                sameInstance((Throwable) null));

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E041]"), containsString(requestId), containsString(filename),
                        containsString("volumeKB: 1]"),
                        containsString("downloadMode: " + CasdaDownloadMode.PAWSEY_HTTP.name()),
                        containsString("fileType: " + "unknown")),
                sameInstance((Throwable) null));
    }

    @Test
    public void testDownloadOKPawseySiap() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.txt";
        FileType fileType = null;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_PAWSEY);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        MvcResult result = this.mockMvc.perform(get("/pawsey/requests/" + requestId + "/" + filename))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)).andReturn();

        // this is the content of the test file
        assertEquals("blah blah", result.getResponse().getContentAsString());

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E040]"), containsString(requestId), containsString(filename)),
                sameInstance((Throwable) null));

        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("[E041]"), containsString(requestId), containsString(filename),
                        containsString("volumeKB: 1]"),
                        containsString("downloadMode: " + CasdaDownloadMode.SODA_ASYNC_PAWSEY.name()),
                        containsString("fileType: " + "unknown")),
                sameInstance((Throwable) null));
    }

    @Test
    public void testDownloadSetsContentTypeForFitsImage() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits";
        FileType fileType = FileType.IMAGE_CUBE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        this.mockMvc.perform(get("/requests/" + requestId + "/" + filename)) //
                .andExpect(status().isOk()) //
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)) //
                .andExpect(header().string("Content-Type", new MediaType("image", "x-fits").toString()));
    }

    @Test
    public void testDownloadSetsContentTypeForChecksumFile() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits.checksum";
        FileType fileType = FileType.IMAGE_CUBE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        this.mockMvc.perform(get("/requests/" + requestId + "/" + filename)) //
                .andExpect(status().isOk()) //
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));
    }

    @Test
    public void testDownloadSetsContentTypeForOtherfile() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.txt";
        FileType fileType = FileType.MEASUREMENT_SET;
        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.WEB);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        createCachedDataFile(requestId, filename, "blah blah", fileType);

        this.mockMvc.perform(get("/requests/" + requestId + "/" + filename)) //
                .andExpect(status().isOk()) //
                .andExpect(header().string("Content-Disposition", "attachment; filename=" + filename)) //
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_OCTET_STREAM_VALUE));
    }

    @Test
    public void testDownloadCantStreamFile() throws Exception
    {
        String requestId = "123-abc";

        DataAccessJob job = new DataAccessJob();
        job.setDownloadMode(CasdaDownloadMode.WEB);
        job.setRequestId(requestId);
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);
        File dataFile = createCachedDataFile(requestId, "doesntExist", "Nothing to see here", FileType.IMAGE_CUBE);
        TestUtils.makeFileUnreadable(dataFile);

        this.mockMvc.perform(get("/requests/123-abc/doesntExist")).andExpect(status().is4xxClientError());

        String logMessage = CasdaDataAccessEvents.E040.messageBuilder().add(requestId).add("doesntExist").toString();
        testAppender.verifyLogMessage(Level.INFO, logMessage);

        assertThat(logMessage, containsString(requestId));
        assertThat(logMessage, containsString("doesntExist"));

        testAppender.verifyLogMessage(Level.ERROR,
                allOf(containsString("[Exxx] "), containsString(dataFile.getName().toString())),
                instanceOf(IOException.class));
    }

    @Test
    public void testDownloadExpiredWebJobRedirectsToViewJob() throws Exception
    {
        String requestId = "123-abc";

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.WEB);
        job.setExpiredTimestamp(new DateTime(DateTimeZone.UTC));
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

        MvcResult result = this.mockMvc.perform(get("/requests/123-abc/test.txt"))
                .andExpect(status().is3xxRedirection()).andReturn();
        assertEquals("/requests/123-abc", result.getResponse().getRedirectedUrl());

        String logMessage = CasdaDataAccessEvents.E040.messageBuilder().add(requestId).add("test.txt").toString();
        testAppender.verifyLogMessage(Level.INFO, logMessage);

        assertThat(logMessage, containsString(requestId));
        assertThat(logMessage, containsString("test.txt"));

        testAppender.verifyNoMessages();
    }

    @Test
    public void testDownloadExpiredPawseyJobRedirectsToViewJob() throws Exception
    {
        String requestId = "123-abc";

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.PAWSEY_HTTP);
        job.setExpiredTimestamp(new DateTime(DateTimeZone.UTC));
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

        MvcResult result = this.mockMvc.perform(get("/pawsey/requests/123-abc/test.txt"))
                .andExpect(status().is3xxRedirection()).andReturn();
        assertEquals("/requests/123-abc", result.getResponse().getRedirectedUrl());

        String logMessage = CasdaDataAccessEvents.E040.messageBuilder().add(requestId).add("test.txt").toString();
        testAppender.verifyLogMessage(Level.INFO, logMessage);

        assertThat(logMessage, containsString(requestId));
        assertThat(logMessage, containsString("test.txt"));

        testAppender.verifyNoMessages();
    }

    @Test
    public void testDownloadExpiredSiapAsyncJobRedirectsToViewJob() throws Exception
    {
        String requestId = "123-abc";

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_WEB);
        job.setExpiredTimestamp(new DateTime(DateTimeZone.UTC));
        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

        MvcResult result = this.mockMvc.perform(get("/requests/123-abc/test.txt"))
                .andExpect(status().is3xxRedirection()).andReturn();
        assertEquals(AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/123-abc",
                result.getResponse().getRedirectedUrl());

        String logMessage = CasdaDataAccessEvents.E040.messageBuilder().add(requestId).add("test.txt").toString();
        testAppender.verifyLogMessage(Level.INFO, logMessage);

        assertThat(logMessage, containsString(requestId));
        assertThat(logMessage, containsString("test.txt"));

        testAppender.verifyNoMessages();
    }

    @Test
    public void testDownloadFileJobDoesntExist() throws Exception
    {
        when(dataAccessJobRepository.findByRequestId("123-abc")).thenReturn(null);
        try
        {
            this.mockMvc.perform(get("/requests/123-abc/test.txt")).andExpect(status().isNotFound()).andReturn();
        }
        catch (NestedServletException e)
        {
            assertEquals(ResourceNotFoundException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testGetCoverageLink() throws Exception
    {
        PowerMockito.mockStatic(Files.class);
        when(dataAccessService.isProjectExist("AS034")).thenReturn(true);
        when(dataAccessService.isProjectExist("BAD01")).thenReturn(false);
        // first 3 calls should pass, 4 & 5 should throw errors before this check,
        // so 4th return value returned by last call
        when(Files.exists(any(Path.class))).thenReturn(true, true, true, false);

        this.mockMvc.perform(get("/coverage/" + "AS034" + "/" + DataAccessDownloadController.LINK_TYPE_MOC)) //
                .andExpect(status().isOk()) //
                .andExpect(content().string("https://myserver:8080/maps/AS034/moc.fits")) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));

        this.mockMvc.perform(get("/coverage/" + "AS034" + "/" + DataAccessDownloadController.LINK_TYPE_PREVIEW)) //
                .andExpect(status().isOk()) //
                .andExpect(content().string("https://myserver:8080/maps/AS034/preview.jpg")) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));

        this.mockMvc.perform(get("/coverage/" + "AS034" + "/" + DataAccessDownloadController.LINK_TYPE_HIPS)) //
                .andExpect(status().isOk()) //
                .andExpect(content().string("https://myserver:8080/maps/AS034/")) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));

        // bad link type
        this.mockMvc.perform(get("/coverage/" + "AS034" + "/" + "steve")) //
                .andExpect(status().isBadRequest());

        // bad projet code
        this.mockMvc.perform(get("/coverage/" + "BAD01" + "/" + DataAccessDownloadController.LINK_TYPE_HIPS)) //
                .andExpect(status().isBadRequest());

        // not directory exists yet
        this.mockMvc.perform(get("/coverage/" + "AS034" + "/" + DataAccessDownloadController.LINK_TYPE_HIPS)) //
                .andExpect(status().isNotFound());
    }

    @Test
    public void testGetHipsCatalogueLink() throws Exception
    {
        PowerMockito.mockStatic(Files.class);
        when(Files.exists(any(Path.class))).thenReturn(true, true, false);

        this.mockMvc.perform(get("/catalogue/hips/cat-hips-cc")) //
                .andExpect(status().isOk()) //
                .andExpect(content().string("https://casda-dev-app.csiro.au/maps/cat-hips-cc")) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));

        this.mockMvc.perform(get("/catalogue/hips/" + "cat-hips-ci")) //
                .andExpect(status().isOk()) //
                .andExpect(content().string("https://casda-dev-app.csiro.au/maps/cat-hips-ci")) //
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE));

        // hips dir does not exist
        this.mockMvc.perform(get("/catalogue/hips/cat-hips-cc")) //
                .andExpect(status().isNotFound());
    }

    @Test
    public void testThumbnailDownloadOKPawsey() throws Exception
    {
        String fileId = "123-thumbnail-abc.jpg";
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get("/pawsey/thumbnail/" + fileId)).andReturn();
        verify(mockdataAccessService, times(1)).downloadThumbnailFromNgas(eq(fileId), any());
    }

    @Test
    public void testNonThumbnailDownloadForbiddenPawsey() throws Exception
    {
        String fileId = "123-abc.jpg";
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get("/pawsey/thumbnail/" + fileId)).andExpect(status().isForbidden()).andReturn();
    }

    @Test
    public void testDownloadCutoutPreviewExists() throws Exception
    {
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        //ra=334.051665&dec=-45.762682&radius=0.0048000000000000004        
        double radius = 0.1;
        String url = String.format("/preview/image/%d?ra=%f&dec=%f&radius=%f", imageId, ra, dec, radius); 
        
        when(mockdataAccessService.downloadCutoutPreview(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyObject()))
                .thenReturn(true);
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get(url)).andReturn();
        verify(mockdataAccessService).downloadCutoutPreview(eq(imageId), eq(ra), eq(dec), eq(radius), any());
    }

    @Test
    public void testDownloadCutoutPreviewCreatesJob() throws Exception
    {
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double radius = 0.1;
        String url = String.format("/preview/image/%d?ra=%f&dec=%f&radius=%f", imageId, ra, dec, radius); 
        
        DataAccessJob accessJob = new DataAccessJob();
        accessJob.addImageCutout(new ImageCutout());
        when(accessJobManager.createDataAccessJob(anyObject(), anyLong(), anyBoolean()))
                .thenReturn(accessJob);
        when(mockdataAccessService.downloadCutoutPreview(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyObject()))
        .thenReturn(false);
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get(url)).andExpect(status().isNoContent()).andReturn();
        
        verify(mockdataAccessService).downloadCutoutPreview(eq(imageId), eq(ra), eq(dec), eq(radius), any());

        ArgumentCaptor<JobDto> jobDtoCaptor = ArgumentCaptor.forClass(JobDto.class);
        verify(accessJobManager).createDataAccessJob(jobDtoCaptor.capture(), eq((Long)null), eq(true));
        JobDto dto = jobDtoCaptor.getValue();
        assertThat(dto.getDownloadFormat(), is("png"));
        assertThat(dto.getIds(), is(new String[]{"cube-42"}));
        Map<String, String[]> params = dto.getParams();
        assertThat(params, hasEntry(ParamKeyWhitelist.FORMAT.toString(), new String[]{"png"}));
        assertThat(params, hasEntry(ParamKeyWhitelist.CIRCLE.toString(),
                new String[] { String.format("%f %f %f", ra, dec, radius) }));
        assertThat(params, hasKey(ParamKeyWhitelist.CIRCLE.toString()));
        
        verify(accessJobManager).scheduleJob(anyString());
    }

    @Test
    public void testDownloadCutoutPreviewFindSmallRadiusJob() throws Exception
    {
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double radius = 0.1;
        String url = String.format("/preview/image/%d?ra=%f&dec=%f&radius=%f", imageId, ra, dec, radius); 
        
        DataAccessJob accessJob = new DataAccessJob();
        accessJob.addImageCutout(new ImageCutout());
        when(accessJobManager.createDataAccessJob(anyObject(), anyLong(), anyBoolean()))
                .thenReturn(accessJob);
        when(mockdataAccessService.downloadCutoutPreview(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyObject()))
        .thenReturn(false);
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get(url)).andExpect(status().isNoContent()).andReturn();
        
        verify(mockdataAccessService).downloadCutoutPreview(eq(imageId), eq(ra), eq(dec), eq(radius), any());

        ArgumentCaptor<JobDto> jobDtoCaptor = ArgumentCaptor.forClass(JobDto.class);
        verify(accessJobManager).createDataAccessJob(jobDtoCaptor.capture(), eq((Long)null), eq(true));
        JobDto dto = jobDtoCaptor.getValue();
        assertThat(dto.getDownloadFormat(), is("png"));
        assertThat(dto.getIds(), is(new String[]{"cube-42"}));
        Map<String, String[]> params = dto.getParams();
        assertThat(params, hasEntry(ParamKeyWhitelist.FORMAT.toString(), new String[]{"png"}));
        assertThat(params, hasEntry(ParamKeyWhitelist.CIRCLE.toString(),
                new String[] { String.format("%f %f %f", ra, dec, radius) }));
        assertThat(params, hasKey(ParamKeyWhitelist.CIRCLE.toString()));
        
        verify(accessJobManager).scheduleJob(anyString());
    }

    @Test
    public void testDownloadCutoutPreviewRejectsNoCutouts() throws Exception
    {
        DataAccessService mockdataAccessService = mock(DataAccessService.class);
        DataAccessDownloadController controller1 = new DataAccessDownloadController(dataAccessJobRepository,
                mockdataAccessService, accessJobManager, "", "", "", "", "", "", "");
        
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double radius = 0.1;
        String url = String.format("/preview/image/%d?ra=%f&dec=%f&radius=%f", imageId, ra, dec, radius); 
        
        DataAccessJob accessJob = new DataAccessJob();
        assertThat(accessJob.getImageCutouts(), is(empty()));
        when(accessJobManager.createDataAccessJob(anyObject(), anyLong(), anyBoolean()))
                .thenReturn(accessJob);
        when(mockdataAccessService.downloadCutoutPreview(anyLong(), anyDouble(), anyDouble(), anyDouble(), anyObject()))
        .thenReturn(false);
        MockMvc mockMvc1 = MockMvcBuilders.standaloneSetup(controller1).build();
        mockMvc1.perform(get(url)).andExpect(status().isNotFound()).andReturn();
        
        verify(mockdataAccessService).downloadCutoutPreview(eq(imageId), eq(ra), eq(dec), eq(radius), any());

        ArgumentCaptor<JobDto> jobDtoCaptor = ArgumentCaptor.forClass(JobDto.class);
        verify(accessJobManager).createDataAccessJob(jobDtoCaptor.capture(), eq((Long)null), eq(true));
        JobDto dto = jobDtoCaptor.getValue();
        assertThat(dto.getDownloadFormat(), is("png"));
        assertThat(dto.getIds(), is(new String[]{"cube-42"}));
        Map<String, String[]> params = dto.getParams();
        assertThat(params, hasEntry(ParamKeyWhitelist.FORMAT.toString(), new String[]{"png"}));
        assertThat(params, hasEntry(ParamKeyWhitelist.CIRCLE.toString(),
                new String[] { String.format("%f %f %f", ra, dec, radius) }));
        assertThat(params, hasKey(ParamKeyWhitelist.CIRCLE.toString()));
        
        verify(accessJobManager, never()).scheduleJob(anyString());
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
        File dataFile = new File(cacheDir.getRoot(), "jobs/" + requestId + "/" + filename);
        dataFile.getParentFile().mkdirs();
        FileUtils.writeStringToFile(dataFile, contents);
        return dataFile;
    }

}
