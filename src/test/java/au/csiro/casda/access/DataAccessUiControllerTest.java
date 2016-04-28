package au.csiro.casda.access;

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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;

import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;
import uws.job.ExecutionPhase;
import uws.job.UWSJob;

/**
 * Tests the Data Access UI Controller.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
public class DataAccessUiControllerTest
{

    @Autowired
    @InjectMocks
    private DataAccessUiController uiController;

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private AccessJobManager accessJobManager;

    @Mock
    private HttpServletRequest request;

    private MockMvc mockMvc;

    /**
     * Set up the ui controller before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        this.mockMvc = MockMvcBuilders.standaloneSetup(uiController).build();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJob() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob dataAccessJob = getDummyDataAccessJobImages();
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        when(accessJobManager.getJobStatus("12345")).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345");

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        Map<String, Object> modelAttribs = model.asMap();
        Collection<DownloadFile> downloadFiles = (Collection<DownloadFile>) modelAttribs.get("downloadFiles");
        DownloadFile downloadFile = downloadFiles.iterator().next();
        assertThat(downloadFile.getFileId(), is("file1-id"));
        assertThat(downloadFile.getDisplayName(), is("file1.fits"));
        assertThat(downloadFiles.size(), is(3));
        assertEquals(" totalling 5 MB.", model.asMap().get("totalSize"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobIndividual() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob dataAccessJob = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.CSV_INDIVIDUAL);
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        when(accessJobManager.getJobStatus("12345")).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345");

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        List<DownloadFile> catalogueDownloadFiles = (List<DownloadFile>) model.asMap().get("downloadFiles");
        assertFalse(catalogueDownloadFiles.isEmpty());

        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) catalogueDownloadFiles.get(0);
        assertEquals("AS123_Continuum_Component_Catalogue_12345_12.csv", downloadFile.getFilename());
        assertEquals(".", model.asMap().get("totalSize"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobMeasurementSets() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob dataAccessJob = getDummyDataAccessJobMeasurementSets();
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        when(accessJobManager.getJobStatus("12345")).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345");

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        List<DownloadFile> measurementSetFiles = (List<DownloadFile>) model.asMap().get("downloadFiles");
        assertEquals(1, measurementSetFiles.size());

        DownloadFile measurementSetFile = measurementSetFiles.get(0);
        assertEquals("observations-12345-measurement_sets-filename", measurementSetFile.getFileId());
        assertEquals("filename", measurementSetFile.getDisplayName());
        assertEquals(1024l, measurementSetFile.getSizeKb());
        assertEquals(" totalling 1 MB.", model.asMap().get("totalSize"));
    }

    @Test
    public void testViewExpiredJob() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob expiredDataAccessJob = getExpiredDataAccessJob();
        when(dataAccessService.getExistingJob("12345")).thenReturn(expiredDataAccessJob);
        UWSJob status = AccessJobManager.createUWSJob("12345", ExecutionPhase.COMPLETED,
                new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                expiredDataAccessJob.getParamMap(), Arrays.asList(), null);
        status.setDestructionTime(expiredDataAccessJob.getExpiredTimestamp().toDate());
        when(accessJobManager.getJobStatus("12345")).thenReturn(status);

        assertThat(expiredDataAccessJob.getStatus(), is(DataAccessJobStatus.READY));
        String result = uiController.viewJob(request, model, "12345");

        assertEquals("viewJob", result);
        assertThat(expiredDataAccessJob.isExpired(), is(true));
    }

    @Test
    public void testViewCancelledJob() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob cancelledDataAccessJob = getCancelledDataAccessJob();
        when(dataAccessService.getExistingJob("12345")).thenReturn(cancelledDataAccessJob);
        when(accessJobManager.getJobStatus("12345")).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                cancelledDataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(cancelledDataAccessJob.getStatus(), is(DataAccessJobStatus.CANCELLED));
        String result = uiController.viewJob(request, model, "12345");

        assertEquals("viewJob", result);
        assertThat(cancelledDataAccessJob.getStatus(), is(DataAccessJobStatus.CANCELLED));
    }

    @Test
    public void testGetJobOK() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        String generatedUuid = "generated_UUID";
        job.setRequestId(generatedUuid);
        when(dataAccessService.getExistingJob(generatedUuid)).thenReturn(job);
        when(accessJobManager.getJobStatus(generatedUuid)).thenReturn(AccessJobManager.createUWSJob(generatedUuid,
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                job.getParamMap(), Arrays.asList(), null));
        this.mockMvc.perform(get("/requests/" + generatedUuid)).andExpect(status().isOk());
        verify(dataAccessService).getExistingJob(generatedUuid);

    }

    @Test
    public void testGetJobMissing() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        String generatedUuid = "generated_UUID";
        job.setRequestId(generatedUuid);
        when(dataAccessService.getExistingJob(generatedUuid)).thenReturn(null);
        this.mockMvc.perform(get("/requests/" + generatedUuid)).andExpect(status().isNotFound());
        verify(dataAccessService).getExistingJob(generatedUuid);

    }

    /**
     * Constructs a dummy DataAccessJob which is past its expiry date but still in the ready state.
     * 
     * @return a barely populated DataAccessJob object
     */
    public DataAccessJob getExpiredDataAccessJob()
    {
        DataAccessJob expiredJob = getDummyDataAccessJobImages();
        expiredJob.setStatus(DataAccessJobStatus.READY);
        LocalDateTime expiry = LocalDateTime.of(2014, Month.DECEMBER, 14, 17, 22);
        Instant expInstant = expiry.atZone(ZoneId.systemDefault()).toInstant();

        DateTime expiredTimestamp = new DateTime(expInstant.toEpochMilli());
        expiredJob.setExpiredTimestamp(expiredTimestamp);

        return expiredJob;
    }

    /**
     * Constructs a dummy DataAccessJob which is cancelled and has an expiry date set.
     * 
     * @return a barely populated DataAccessJob object
     */
    public DataAccessJob getCancelledDataAccessJob()
    {
        DataAccessJob expiredJob = getDummyDataAccessJobImages();
        expiredJob.setStatus(DataAccessJobStatus.CANCELLED);
        LocalDateTime expiry = LocalDateTime.of(2014, Month.DECEMBER, 14, 17, 22);
        Instant expInstant = expiry.atZone(ZoneId.systemDefault()).toInstant();

        DateTime expiredTimestamp = new DateTime(expInstant.toEpochMilli());
        expiredJob.setExpiredTimestamp(expiredTimestamp);

        return expiredJob;
    }

    /**
     * Constructs a dummy DataAccessJob
     * 
     * @return a barely populated DataAccessJob object
     */
    public DataAccessJob getDummyDataAccessJobImages()
    {
        DataAccessJob dataAccessJob = createBasicDataAccessJob();

        ImageCube imageCube = spy(new ImageCube());
        imageCube.setFilename("file1.fits");
        imageCube.setFilesize(1000l);
        doReturn("file1-id").when(imageCube).getFileId();

        ImageCube imageCube2 = spy(new ImageCube());
        imageCube2.setFilename("file2.fits");
        imageCube2.setFilesize(2000l);
        doReturn("file2-id").when(imageCube2).getFileId();

        ImageCube imageCube3 = spy(new ImageCube());
        imageCube3.setFilename("file3.fits");
        imageCube3.setFilesize(3000l);
        doReturn("file3-id").when(imageCube3).getFileId();

        dataAccessJob.addImageCube(imageCube);
        dataAccessJob.addImageCube(imageCube2);
        dataAccessJob.addImageCube(imageCube3);

        return dataAccessJob;
    }

    private static DataAccessJob getDummyDataAccessJobCatalogues(CatalogueDownloadFormat downloadFormat)
    {
        DataAccessJob dataAccessJob = createBasicDataAccessJob();
        dataAccessJob.setDownloadFormat(downloadFormat.name());

        Project project = new Project();
        project.setOpalCode("AS123");

        Observation observation = new Observation();
        observation.setSbid(12345);

        Catalogue catalogue = new Catalogue();
        catalogue.setId(12L);
        catalogue.setCatalogueType(CatalogueType.CONTINUUM_COMPONENT);
        catalogue.setProject(project);
        catalogue.setParent(observation);

        dataAccessJob.addCatalogue(catalogue);

        return dataAccessJob;
    }

    private static DataAccessJob getDummyDataAccessJobMeasurementSets()
    {
        DataAccessJob dataAccessJob = createBasicDataAccessJob();

        Project project = new Project();
        project.setOpalCode("AS123");

        Observation observation = new Observation();
        observation.setSbid(12345);

        MeasurementSet measurementSet = new MeasurementSet();
        measurementSet.setProject(project);
        measurementSet.setParent(observation);
        measurementSet.setId(12L);
        measurementSet.setFilename("filename");
        measurementSet.setFilesize(1024l);

        dataAccessJob.addMeasurementSet(measurementSet);

        return dataAccessJob;
    }

    private static DataAccessJob createBasicDataAccessJob()
    {
        DataAccessJob dataAccessJob = new DataAccessJob()
        {
            public long getFileCount()
            {
                return 1;
            }
        };
        dataAccessJob.setCreatedTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));
        dataAccessJob.setAvailableTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));

        dataAccessJob.setSizeKb(1l);
        dataAccessJob.setRequestId("12345");
        dataAccessJob.setStatus(DataAccessJobStatus.PREPARING);
        return dataAccessJob;
    }
}
