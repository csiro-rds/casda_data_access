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

import static org.mockito.Matchers.any;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.ui.ExtendedModelMap;
import org.springframework.ui.Model;

import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.observation.CatalogueType;
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
    private DataAccessUiController uiController;

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private AccessJobManager accessJobManager;

    @Mock
    private HttpServletRequest request;
    
    @Mock
    private HttpSession session;

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
        uiController = new DataAccessUiController(dataAccessService, accessJobManager, "gaid", 
        		"http://localhost/download", 25);
        this.mockMvc = MockMvcBuilders.standaloneSetup(uiController).build();
        
        //paging
        Map<FileType, Integer[]> pageDetails = new HashMap<FileType, Integer[]>();
        pageDetails.put(FileType.MEASUREMENT_SET, new Integer[]{1,25});
        List<Map<FileType, Integer[]>> pageList = new ArrayList<Map<FileType, Integer[]>>();
        pageList.add(pageDetails);
        when(request.getSession()).thenReturn(session);
        when(session.getAttribute(any(String.class))).thenReturn(pageList);
        when(dataAccessService.isImageCubeOrMeasurementSetJob(any(Long.class))).thenReturn(true);
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(pageList);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJob() throws Exception
    {
        Model model = new ExtendedModelMap();
        DataAccessJob dataAccessJob = getDummyDataAccessJob();
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345", 1);

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        Map<String, Object> modelAttribs = model.asMap();
        Collection<DownloadFile> downloadFiles = (Collection<DownloadFile>) modelAttribs.get("downloadFiles");
        DownloadFile downloadFile = downloadFiles.iterator().next();
        assertThat(downloadFile.getFileId(), is("observation-12345-image-234"));
        assertThat(downloadFile.getDisplayName(), is("image_1_display"));
        assertThat(downloadFiles.size(), is(3));
        assertEquals(" totalling 126 KB.", model.asMap().get("totalSize"));
        assertEquals(false, modelAttribs.get("pawsey"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobIndividual() throws Exception
    {
        Model model = new ExtendedModelMap();
        DataAccessJob dataAccessJob = getDummyDataAccessJob();
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);     
        
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class)))
        .thenReturn(getCatalogue(CatalogueDownloadFormat.CSV_INDIVIDUAL));
        when(dataAccessService.isImageCubeOrMeasurementSetJob(any(Long.class))).thenReturn(false);
        when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345", 1);

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        List<DownloadFile> catalogueDownloadFiles = (List<DownloadFile>) model.asMap().get("downloadFiles");
        assertFalse(catalogueDownloadFiles.isEmpty());

        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) catalogueDownloadFiles.get(0);
        assertEquals("my_catalogue_1", downloadFile.getFilename());
        assertEquals(".", model.asMap().get("totalSize"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobPawsey() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob dataAccessJob = getDummyDataAccessJob();
        dataAccessJob.setDownloadMode(CasdaDownloadMode.SODA_ASYNC_PAWSEY);
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345", 1);

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        Map<String, Object> modelAttribs = model.asMap();
        Collection<DownloadFile> downloadFiles = (Collection<DownloadFile>) modelAttribs.get("downloadFiles");
        DownloadFile downloadFile = downloadFiles.iterator().next();
        assertThat(downloadFile.getFileId(), is("observation-12345-image-234"));
        assertThat(downloadFile.getDisplayName(), is("image_1_display"));
        assertThat(downloadFiles.size(), is(3));
        assertEquals(" totalling 126 KB.", model.asMap().get("totalSize"));
        assertEquals(true, modelAttribs.get("pawsey"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testViewJobMeasurementSets() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob dataAccessJob = getDummyDataAccessJob();
        when(dataAccessService.getExistingJob("12345")).thenReturn(dataAccessJob);
        when(accessJobManager.getJobStatus(dataAccessJob)).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                dataAccessJob.getParamMap(), Arrays.asList(), null));
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getMeasurementSets());
        
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        String result = uiController.viewJob(request, model, "12345", 1);

        assertEquals("viewJob", result);
        assertThat(dataAccessJob.getStatus(), is(DataAccessJobStatus.PREPARING));
        List<DownloadFile> measurementSetFiles = (List<DownloadFile>) model.asMap().get("downloadFiles");
        assertEquals(3, measurementSetFiles.size());

        DownloadFile measurementSetFile = measurementSetFiles.get(0);
        assertEquals("observation-12345-mSet-234", measurementSetFile.getFileId());
        assertEquals("mSet_1_display", measurementSetFile.getDisplayName());
        assertEquals(27l, measurementSetFile.getSizeKb());
        assertEquals(" totalling 126 KB.", model.asMap().get("totalSize"));
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
        when(accessJobManager.getJobStatus(expiredDataAccessJob)).thenReturn(status);

        assertThat(expiredDataAccessJob.getStatus(), is(DataAccessJobStatus.READY));
        String result = uiController.viewJob(request, model, "12345", 1);

        assertEquals("viewJob", result);
        assertThat(expiredDataAccessJob.isExpired(), is(true));
    }

    @Test
    public void testViewCancelledJob() throws Exception
    {
        Model model = new ExtendedModelMap();

        DataAccessJob cancelledDataAccessJob = getCancelledDataAccessJob();
        when(dataAccessService.getExistingJob("12345")).thenReturn(cancelledDataAccessJob);
        when(accessJobManager.getJobStatus(cancelledDataAccessJob)).thenReturn(AccessJobManager.createUWSJob("12345",
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                cancelledDataAccessJob.getParamMap(), Arrays.asList(), null));

        assertThat(cancelledDataAccessJob.getStatus(), is(DataAccessJobStatus.CANCELLED));
        String result = uiController.viewJob(request, model, "12345", 1);

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
        when(accessJobManager.getJobStatus(job)).thenReturn(AccessJobManager.createUWSJob(generatedUuid,
                ExecutionPhase.EXECUTING, new DateTime(DateTimeZone.UTC).minusHours(3).getMillis(),
                new DateTime(DateTimeZone.UTC).minusHours(2).getMillis(), new DateTime(DateTimeZone.UTC).plusDays(2),
                job.getParamMap(), Arrays.asList(), null));
        this.mockMvc.perform(get("/requests/" + generatedUuid + "/page/1")).andExpect(status().isOk());
        verify(dataAccessService).getExistingJob(generatedUuid);

    }

    @Test
    public void testGetJobMissing() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        String generatedUuid = "generated_UUID";
        job.setRequestId(generatedUuid);
        when(dataAccessService.getExistingJob(generatedUuid)).thenReturn(null);
        this.mockMvc.perform(get("/requests/" + generatedUuid + "/page/1")).andExpect(status().isNotFound());
        verify(dataAccessService).getExistingJob(generatedUuid);

    }

    /**
     * Constructs a dummy DataAccessJob which is past its expiry date but still in the ready state.
     * 
     * @return a barely populated DataAccessJob object
     */
    public DataAccessJob getExpiredDataAccessJob()
    {
        DataAccessJob expiredJob = getDummyDataAccessJob();
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
        DataAccessJob expiredJob = getDummyDataAccessJob();
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
    public DataAccessJob getDummyDataAccessJob()
    {
        DataAccessJob dataAccessJob = createBasicDataAccessJob();
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
    
    private List<DownloadFile> getImageCubes()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	DownloadFile file1 = new FileDescriptor("observation-12345-image-234", 27L, FileType.IMAGE_CUBE, "image_1_display"); 
    	DownloadFile file2 = new FileDescriptor("observation-12345-image-235", 44L, FileType.IMAGE_CUBE, "image_2_display"); 
    	DownloadFile file3 = new FileDescriptor("observation-12345-image-236", 55L, FileType.IMAGE_CUBE, "image_3_display"); 

    	fileList.add(file1);
    	fileList.add(file2);
    	fileList.add(file3);
        return fileList; 
    }
    
    private List<DownloadFile> getMeasurementSets()
    {      
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	DownloadFile file1 = 
    			new FileDescriptor("observation-12345-mSet-234", 27L, FileType.MEASUREMENT_SET, "mSet_1_display"); 
    	DownloadFile file2 = 
    			new FileDescriptor("observation-12345-mSet-235", 44L, FileType.MEASUREMENT_SET, "mSet_2_display"); 
    	DownloadFile file3 = 
    			new FileDescriptor("observation-12345-mSet-236", 55L, FileType.MEASUREMENT_SET, "mSet_3_display"); 

    	fileList.add(file1);
    	fileList.add(file2);
    	fileList.add(file3);
        return fileList; 
    }
    
    private List<DownloadFile> getCatalogue(CatalogueDownloadFormat downloadFormat)
    {      
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	CatalogueDownloadFile file1 = new CatalogueDownloadFile("catalogue_1", 27l, "my_catalogue_1", 
    			new ArrayList<Long>(), downloadFormat, CatalogueType.CONTINUUM_COMPONENT, "tablename");

    	fileList.add(file1);
        return fileList; 
    }
}
