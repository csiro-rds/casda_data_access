package au.csiro.casda.access;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.util.NestedServletException;

import au.csiro.TestUtils;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.NgasService;
import au.csiro.casda.access.siap2.AccessDataController;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;

/**
 * Tests for the DataAccessDownload Controller
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
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
    private CachedFileRepository cachedFileRepository;

    @Mock
    private DataAccessService dataAccessService;

    @Before
    public void setUp() throws Exception
    {
        testAppender = Log4JTestAppender.createAppender();
        MockitoAnnotations.initMocks(this);
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, cachedFileRepository, mock(NgasService.class),
                cacheDir.getRoot().getAbsolutePath(), "", mock(ProcessJobFactory.class));
        controller = new DataAccessDownloadController(dataAccessJobRepository, dataAccessService);
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
    public void testDownloadOKSiapAsync() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.fits";
        FileType fileType = FileType.CATALOGUE;

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        job.setDownloadMode(CasdaDownloadMode.SIAP_ASYNC);
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
                        containsString("downloadMode: " + CasdaDownloadMode.SIAP_ASYNC.name()),
                        containsString("fileType: " + fileType)),
                sameInstance((Throwable) null));
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
        job.setDownloadMode(CasdaDownloadMode.SIAP_ASYNC);
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
