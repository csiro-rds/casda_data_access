package au.csiro.casda.access.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import au.csiro.casda.access.BaseTest;
import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.CatalogueDownloadFile;
import au.csiro.casda.access.CatalogueDownloadFormat;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.EncapsulatedFileDescriptor;
import au.csiro.casda.access.ErrorFileDescriptor;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.GeneratedFileDescriptor;
import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.cache.Packager.Result;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.GeneratedSpectrumRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.jpa.ThumbnailRepository;
import au.csiro.casda.access.rest.VoToolsCataloguePackager;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;
import au.csiro.casda.jobmanager.CasdaToolProcessJobBuilder;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.logging.DataLocation;

/**
 * Unit tests for the Packager
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class PackagerTest extends BaseTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private CachedFileRepository cachedFileRepository;

    @Mock
    private DataAccessJobRepository jobRepository;

    @Mock
    private DataAccessJobRepository dataAccessJobRepository;

    @Mock
    private ImageCutoutRepository imageCutoutRepository;

    @Mock
    private GeneratedSpectrumRepository generatedSpectrumRepository;

    @Mock
    private SpectrumRepository spectrumRepository;

    @Mock
    private MomentMapRepository momentMapRepository;

    @Mock
    private CubeletRepository cubeletRepository;

    @Mock
    private ThumbnailRepository thumbnailRepository;

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private VoToolsCataloguePackager voToolsCataloguePackager;

    @Mock
    private JobManager jobManager;

    @Mock
    private CasdaToolProcessJobBuilder processBuilder;

    @Mock
    private InlineScriptService inlineScriptService;

    private DownloadManager downloadManager;

    private CacheManager cacheManager;

    private Packager packager;

    private static final int DEFAULT_EXPIRY = 24 * 7;

    private static final int SYNC_EXPIRY = 1;

    private static final int MAX_RETRIES = 3;

    private Log4JTestAppender testAppender;

    public PackagerTest() throws Exception
    {
        super();
    }

    @Before
    public void setup() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        testAppender = Log4JTestAppender.createAppender();
        cacheManager = spy(new CacheManager(400l, MAX_RETRIES, tempFolder.getRoot().getCanonicalPath(),
                cachedFileRepository, dataAccessJobRepository));
        JavaProcessJobFactory processJobFactory = new JavaProcessJobFactory();

        String downloadCommandAndArgs =
                "{ " + "\"download\", " + "\"fileId=<fileId>\", " + "\"destination=<destination>\" }";
        String cutoutCommandAndArgs = "{ \"cmd\", \"arg1\", \"<source_file>\", \"<dest_file>\", \"-D3\", "
                + "\"<dim3_range>\", \"-D4\", \"<dim4_range>\", \"<ra>\", \"<dec>\", \"<xsize>\", \"<ysize>\" }";
        String pngCutoutCommandAndArgs = "{ \"cmd\", \"arg1\", \"<source_file>\", \"<dest_file>\", \"-D3\", "
                + "\"<dim3_range>\", \"-D4\", \"<dim4_range>\", \"<ra>\", \"<dec>\", \"<xsize>\", \"<ysize>\" }";
        String generateSpectrumCommand = "{ \"cmd\", \"arg1\", \"<source_file>\", \"<dest_file>\", \"-D3\", " 
                + "\"<dim3_range>\", \"-D4\", \"<dim4_range>\", \"<ra>\", \"<dec>\", \"<xsize>\", \"<ysize>\" }";
        String encapCommandAndArgs = "{ \"unencapsulate\", \"<tarFileName>\"}";
        downloadManager = spy(new DownloadManager(cachedFileRepository, imageCutoutRepository,
                generatedSpectrumRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                thumbnailRepository, jobManager, "depositToolsWorkingDirectory", 3,
                mock(CasdaToolProcessJobBuilderFactory.class), DownloadManager.ProcessJobType.SIMPLE.toString(),
                downloadCommandAndArgs, null, cutoutCommandAndArgs, pngCutoutCommandAndArgs, generateSpectrumCommand,
                encapCommandAndArgs, processJobFactory, inlineScriptService, ""));
        packager = spy(new Packager(cacheManager, voToolsCataloguePackager, dataAccessService, 1, downloadManager));
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
    
    private List<Map<FileType, Integer[]>> getPagingDetails()
    {
        Map<FileType, Integer[]> pageDetails = new HashMap<FileType, Integer[]>();
        pageDetails.put(FileType.IMAGE_CUBE, new Integer[]{1,3});
        List<Map<FileType, Integer[]>> pageList = new ArrayList<Map<FileType, Integer[]>>();
        pageList.add(pageDetails);
        return pageList;
    }

    @Test
    public void testNoFilesToDownloadDoesntProcessAnyFiles() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-A");
        job.setDownloadMode(CasdaDownloadMode.PAWSEY_HTTP);

        Result result = packager.pack(job, DEFAULT_EXPIRY);

        assertEquals(System.currentTimeMillis(), result.getExpiryDate().getMillis(), 100);
        assertEquals(0L, result.getCachedSizeKb());
        assertEquals(0L, result.getTotalSizeKb());
        verify(cacheManager, never()).reserveSpaceAndRegisterFilesForDownload(any(), any());
    }

    @Test
    public void testPackWorkflowWebDownload() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.WEB);
    }

    @Test
    public void testPackWorkflowPawseyDownload() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.PAWSEY_HTTP);
    }

    @Test
    public void testPackWorkflowSiapAsyncWeb() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.SODA_ASYNC_WEB);
    }

    @Test
    public void testPackWorkflowSiapSyncWeb() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.SODA_SYNC_WEB);
    }
    
    @Test
    public void testPackWorkflowSiapAsyncPawsey() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.SODA_ASYNC_PAWSEY);
    }

    @Test
    public void testPackWorkflowSiapSyncPawsey() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        testPackWorkflow(CasdaDownloadMode.SODA_SYNC_PAWSEY);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private void testPackWorkflow(CasdaDownloadMode downloadMode) throws Exception
    {

        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-B");
        job.setDownloadMode(downloadMode);
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getFileId()).thenReturn("image_cube_file_id");
        when(imageCube.getFilesize()).thenReturn(15L);
        job.addImageCube(imageCube);

        MeasurementSet measurementSet = mock(MeasurementSet.class);
        when(measurementSet.getFileId()).thenReturn("measurement_set_file_id");
        job.addMeasurementSet(measurementSet);

        Catalogue catalogue = new Catalogue(CatalogueType.CONTINUUM_COMPONENT);
        job.addCatalogue(catalogue);
        Project project = mock(Project.class);
        when(project.getOpalCode()).thenReturn("ABC123");
        catalogue.setProject(project);
        Observation observation = mock(Observation.class);
        when(observation.getSbid()).thenReturn(12345);
        catalogue.setParent(observation);
        job.setDownloadFormat(CatalogueDownloadFormat.CSV_INDIVIDUAL.name());

        doReturn(new Object[] { 15L, null }).when(cacheManager).reserveSpaceAndRegisterFilesForDownload(any(), eq(job));
        doNothing().when(packager).pollUntilFileDownloadComplete(any(DataAccessJob.class), any(Collection.class),
                any(Integer.class));
        doNothing().when(cacheManager).createDataAccessJobDirectory(any(), any());
        doNothing().when(cacheManager).updateUnlockForFiles(any(), any());
        doNothing().when(dataAccessService).updateFileSizeForGeneratedFiles(any());

        DateTime approximateExpiryDateTime;
        int hoursToExpiryForJob = Utils.SYNC_DOWNLOADS.contains(downloadMode) ? SYNC_EXPIRY : DEFAULT_EXPIRY;
        approximateExpiryDateTime = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob);

        Result response = packager.pack(job, hoursToExpiryForJob);

        ArgumentCaptor<Collection> downloadFilesCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<DateTime> timeCaptor = ArgumentCaptor.forClass(DateTime.class);
        verify(cacheManager, times(1)).reserveSpaceAndRegisterFilesForDownload(downloadFilesCaptor.capture(), eq(job));
        verify(packager, times(1)).pollUntilFileDownloadComplete(eq(job), downloadFilesCaptor.capture(),
                eq(hoursToExpiryForJob));
        verify(cacheManager, times(1)).createDataAccessJobDirectory(eq(job), downloadFilesCaptor.capture());
        verify(cacheManager, times(1)).updateUnlockForFiles(downloadFilesCaptor.capture(), timeCaptor.capture());
        verify(dataAccessService, times(1)).updateFileSizeForGeneratedFiles(downloadFilesCaptor.capture());
        // job expiry same as the unlock time because it's web download
        DateTime unlockTime = timeCaptor.getValue();
        assertEquals(response.getExpiryDate().getMillis(), unlockTime.getMillis());
        assertThat(new Period(approximateExpiryDateTime, timeCaptor.getValue()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        assertEquals(15L, response.getCachedSizeKb());
        assertEquals(126L, response.getTotalSizeKb());
        // make sure all download files args are the same list
        for (Collection<DownloadFile> dowloadFiles : downloadFilesCaptor.getAllValues())
        {
            assertEquals(downloadFilesCaptor.getValue(), dowloadFiles);
        }
        // }
    }

    @Test
    public void testAssembleDataAccessJobDownloadFiles() throws Exception
    {
        assertThat(packager.assembleDataAccessJobDownloadFiles(new ArrayList<DownloadFile>()), is(empty()));

        List<DownloadFile> fileList = new ArrayList<DownloadFile>();
        
        List<Map<String, Object>> artefactDetails = new ArrayList<Map<String, Object>>();
        
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-C1");
        job.setDownloadFormat(CatalogueDownloadFormat.CSV_INDIVIDUAL.name());
        
        Map<String, Object> imageCube = new HashMap<String, Object>();
        imageCube.put(DataAccessUtil.ID, 14L);
        imageCube.put(DataAccessUtil.OBSERVATION_ID, 330055);
        imageCube.put(DataAccessUtil.LEVEL7_ID, null);
        imageCube.put(DataAccessUtil.FILENAME, "my_image_cube");
        imageCube.put(DataAccessUtil.FILE_SIZE, 15L);
        artefactDetails.add(imageCube);
        
        DataAccessUtil.getFileDescriptors(artefactDetails, fileList, FileType.IMAGE_CUBE);

        artefactDetails.clear();
        Map<String, Object> measurementSet = new HashMap<String, Object>();
        measurementSet.put(DataAccessUtil.ID, 45L);
        measurementSet.put(DataAccessUtil.OBSERVATION_ID, 330055);
        measurementSet.put(DataAccessUtil.LEVEL7_ID, null);
        measurementSet.put(DataAccessUtil.FILENAME, "my_measurement_set");
        measurementSet.put(DataAccessUtil.FILE_SIZE, 4L);
        artefactDetails.add(measurementSet);

        DataAccessUtil.getFileDescriptors(artefactDetails, fileList, FileType.MEASUREMENT_SET);
        
        artefactDetails.clear();
        Map<String, Object> cutout = new HashMap<String, Object>();
        cutout.put(DataAccessUtil.ID, 17L);
        cutout.put(DataAccessUtil.OBSERVATION_ID, 330055);
        cutout.put(DataAccessUtil.LEVEL7_ID, null);
        cutout.put(DataAccessUtil.FILENAME, "my_cutout");
        cutout.put(DataAccessUtil.FILE_SIZE, 6L);
        cutout.put(DataAccessUtil.IMAGE_CUBE_ID, 55L);
        cutout.put(DataAccessUtil.IMAGE_CUBE_SIZE, 5854L);
        artefactDetails.add(cutout);
        
        DataAccessUtil.getGeneratedImageFiles(artefactDetails, fileList, FileType.IMAGE_CUTOUT);
        
        artefactDetails.clear();
        Map<String, Object> genSpectrum = new HashMap<String, Object>();
        genSpectrum.put(DataAccessUtil.ID, 21L);
        genSpectrum.put(DataAccessUtil.OBSERVATION_ID, 330055);
        genSpectrum.put(DataAccessUtil.LEVEL7_ID, null);
        genSpectrum.put(DataAccessUtil.FILENAME, "my_cutout");
        genSpectrum.put(DataAccessUtil.FILE_SIZE, 22L);
        genSpectrum.put(DataAccessUtil.IMAGE_CUBE_ID, 55L);
        genSpectrum.put(DataAccessUtil.IMAGE_CUBE_SIZE, 5854L);
        artefactDetails.add(genSpectrum);
        
        DataAccessUtil.getGeneratedImageFiles(artefactDetails, fileList, FileType.GENERATED_SPECTRUM);
        
        artefactDetails.clear();
        Map<String, Object> momentMapEncap = new HashMap<String, Object>();
        momentMapEncap.put(DataAccessUtil.ID, 65L);
        momentMapEncap.put(DataAccessUtil.OBSERVATION_ID, 330055);
        momentMapEncap.put(DataAccessUtil.LEVEL7_ID, null);
        momentMapEncap.put(DataAccessUtil.FILENAME, "my_encap_moment_map");
        momentMapEncap.put(DataAccessUtil.FILE_SIZE, 32L);
        momentMapEncap.put(DataAccessUtil.ENCAPSULATION_FILENAME, "encap_1.tar");
        momentMapEncap.put(DataAccessUtil.ENCAPSULATION_FILE_SIZE, 345L);
        momentMapEncap.put(DataAccessUtil.ENCAPSULATION_ID, 2L);
        artefactDetails.add(momentMapEncap);
        
        DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, FileType.MOMENT_MAP);
        
        artefactDetails.clear();
        Map<String, Object> momentMapOrig = new HashMap<String, Object>();
        momentMapOrig.put(DataAccessUtil.ID, 65L);
        momentMapOrig.put(DataAccessUtil.OBSERVATION_ID, 330055);
        momentMapOrig.put(DataAccessUtil.LEVEL7_ID, null);
        momentMapOrig.put(DataAccessUtil.FILENAME, "my_orig_moment_map");
        momentMapOrig.put(DataAccessUtil.FILE_SIZE, 32L);
        artefactDetails.add(momentMapOrig);
        
        DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, FileType.MOMENT_MAP);
        
        artefactDetails.clear();
        Map<String, Object> cubeletEncap = new HashMap<String, Object>();
        cubeletEncap.put(DataAccessUtil.ID, 108L);
        cubeletEncap.put(DataAccessUtil.OBSERVATION_ID, 330055);
        cubeletEncap.put(DataAccessUtil.LEVEL7_ID, null);
        cubeletEncap.put(DataAccessUtil.FILENAME, "my_encap_cubelet");
        cubeletEncap.put(DataAccessUtil.FILE_SIZE, 37L);
        cubeletEncap.put(DataAccessUtil.ENCAPSULATION_FILENAME, "encap_2.tar");
        cubeletEncap.put(DataAccessUtil.ENCAPSULATION_FILE_SIZE, 345L);
        cubeletEncap.put(DataAccessUtil.ENCAPSULATION_ID, 3L);
        artefactDetails.add(cubeletEncap);
        
        DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, FileType.CUBELET);
        
        artefactDetails.clear();
        Map<String, Object> spectrumEncap = new HashMap<String, Object>();
        spectrumEncap.put(DataAccessUtil.ID, 108L);
        spectrumEncap.put(DataAccessUtil.OBSERVATION_ID, 330055);
        spectrumEncap.put(DataAccessUtil.LEVEL7_ID, null);
        spectrumEncap.put(DataAccessUtil.FILENAME, "my_encap_spectrum");
        spectrumEncap.put(DataAccessUtil.FILE_SIZE, 37L);
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_FILENAME, "encap_2.tar");
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_FILE_SIZE, 345L);
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_ID, 3L);
        artefactDetails.add(spectrumEncap);
        
        DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, FileType.SPECTRUM);
        
        artefactDetails.clear();
        Map<String, Object> spectrumOrig = new HashMap<String, Object>();
        spectrumOrig.put(DataAccessUtil.ID, 99L);
        spectrumOrig.put(DataAccessUtil.OBSERVATION_ID, 330055);
        spectrumOrig.put(DataAccessUtil.LEVEL7_ID, null);
        spectrumOrig.put(DataAccessUtil.FILENAME, "my_orig_spectrum");
        spectrumOrig.put(DataAccessUtil.FILE_SIZE, 20L);
        artefactDetails.add(spectrumOrig);
        
        DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, FileType.SPECTRUM);
        
        artefactDetails.clear();
        Map<String, Object> catalogue = new HashMap<String, Object>();
        catalogue.put(DataAccessUtil.ID, 33L);
        catalogue.put(DataAccessUtil.OBSERVATION_ID, 330055);
        catalogue.put(DataAccessUtil.PROJECT_ID, "AS031");
        catalogue.put(DataAccessUtil.FILENAME, "my_catalogue");
        catalogue.put(DataAccessUtil.CATALOGUE_TYPE, CatalogueType.CONTINUUM_COMPONENT.name());
        catalogue.put(DataAccessUtil.TABLE_NAME, null);
        artefactDetails.add(catalogue);

        DataAccessUtil.getCatalogueDownloadFiles(artefactDetails, fileList, new File("C:\\hello"), job);

        artefactDetails.clear();
        
        Collection<DownloadFile> job2DownloadFiles = packager.assembleDataAccessJobDownloadFiles(fileList);
        for (DownloadFile downloadFile : job2DownloadFiles)
        {
            if (downloadFile.getFileType() == FileType.CATALOGUE)
            {
                CatalogueDownloadFile catalogueDownloadFile = (CatalogueDownloadFile) downloadFile;
                assertEquals("ABC-123-C1-AS031_Continuum_Component_Catalogue_330055_33.csv",
                        catalogueDownloadFile.getFileId());
                assertEquals("AS031_Continuum_Component_Catalogue_330055_33.csv", catalogueDownloadFile.getFilename());
                assertEquals(0L, catalogueDownloadFile.getSizeKb());
                assertThat(catalogueDownloadFile.getCatalogueIds(), contains(33L));
                assertEquals(CatalogueType.CONTINUUM_COMPONENT, catalogueDownloadFile.getCatalogueType());
                assertEquals(CatalogueDownloadFormat.CSV_INDIVIDUAL, catalogueDownloadFile.getDownloadFormat());
            }
        	else if (downloadFile.getFileType() == FileType.IMAGE_CUBE)
            {
                assertEquals("observations-330055-image_cubes-my_image_cube", downloadFile.getFileId());
                assertEquals(15L, downloadFile.getSizeKb());
            }
            else if (downloadFile.getFileType() == FileType.IMAGE_CUTOUT
                    && "cutout-17-imagecube-55.fits".equals(downloadFile.getFileId()))
            {
                GeneratedFileDescriptor cutoutDownloadFile = (GeneratedFileDescriptor) downloadFile;
                assertEquals(6L, cutoutDownloadFile.getSizeKb());
                assertNull(cutoutDownloadFile.getOriginalImageFilePath());
                assertEquals("cutout-17-imagecube-55.fits", cutoutDownloadFile.getDisplayName());
                assertEquals("cutout-17-imagecube-55.fits", cutoutDownloadFile.getFilename());
                assertEquals("observations-330055-image_cubes-my_cutout", 
                		cutoutDownloadFile.getOriginalImageDownloadFile().getFileId());
                assertEquals(5854L, cutoutDownloadFile.getOriginalImageDownloadFile().getSizeKb());
                assertEquals(FileType.IMAGE_CUBE, cutoutDownloadFile.getOriginalImageDownloadFile().getFileType());

            }
            else if (downloadFile.getFileType() == FileType.GENERATED_SPECTRUM
                    && "spectrum-21-imagecube-55.fits".equals(downloadFile.getFileId()))
            {
                GeneratedFileDescriptor cgeneratedSpectrumFile = (GeneratedFileDescriptor) downloadFile;
                assertEquals(22L, cgeneratedSpectrumFile.getSizeKb());
                assertNull(cgeneratedSpectrumFile.getOriginalImageFilePath());
                assertEquals("spectrum-21-imagecube-55.fits", cgeneratedSpectrumFile.getDisplayName());
                assertEquals("spectrum-21-imagecube-55.fits", cgeneratedSpectrumFile.getFilename());
                assertEquals("observations-330055-image_cubes-my_cutout", 
                		cgeneratedSpectrumFile.getOriginalImageDownloadFile().getFileId());
                assertEquals(5854L, cgeneratedSpectrumFile.getOriginalImageDownloadFile().getSizeKb());
                assertEquals(FileType.IMAGE_CUBE, cgeneratedSpectrumFile.getOriginalImageDownloadFile().getFileType());

            }
            else if (downloadFile.getFileType() == FileType.IMAGE_CUTOUT
                    && "cutout-15-imagecube-18.fits".equals(downloadFile.getFileId()))
            {
                GeneratedFileDescriptor cutoutDownloadFile = (GeneratedFileDescriptor) downloadFile;
                assertEquals(13L, cutoutDownloadFile.getSizeKb());
                assertEquals(Paths.get("/dir/file").toAbsolutePath().toString(),
                        cutoutDownloadFile.getOriginalImageFilePath());
                assertEquals("cutout-15-imagecube-18.fits", cutoutDownloadFile.getDisplayName());
                assertEquals("cutout-15-imagecube-18.fits", cutoutDownloadFile.getFilename());
                assertEquals("image_cube_in_cache_file_id",
                        cutoutDownloadFile.getOriginalImageDownloadFile().getFileId());
                assertEquals(20L, cutoutDownloadFile.getOriginalImageDownloadFile().getSizeKb());
                assertEquals(FileType.IMAGE_CUBE, cutoutDownloadFile.getOriginalImageDownloadFile().getFileType());

            }
            else if (downloadFile.getFileType() == FileType.ERROR)
            {
                ErrorFileDescriptor errFileDesc = (ErrorFileDescriptor) downloadFile;
                assertEquals("error-01.txt", errFileDesc.getFileId());
                assertEquals(1L, errFileDesc.getSizeKb());
                assertEquals("Test error", errFileDesc.getErrorMessage());
            }
            else if(downloadFile.getFileType() == FileType.SPECTRUM && 
            		"observations-330055-spectra-my_encap_spectrum".equals(downloadFile.getFileId()))
            {
            	EncapsulatedFileDescriptor encapsulatedFileDescriptor = (EncapsulatedFileDescriptor) downloadFile;
                assertEquals("observations-330055-spectra-my_encap_spectrum", encapsulatedFileDescriptor.getFileId());
                assertEquals(37L, encapsulatedFileDescriptor.getSizeKb());
                assertEquals(FileType.SPECTRUM, encapsulatedFileDescriptor.getFileType());
                assertEquals("observations-330055-encapsulation_files-encap_2.tar", 
                		encapsulatedFileDescriptor.getEncapsulationFile().getFileId());
                assertEquals(345L, encapsulatedFileDescriptor.getEncapsulationFile().getSizeKb());   
            }
            else if(downloadFile.getFileType() == FileType.SPECTRUM && 
            		"observations-330055-spectra-my_orig_spectrum".equals(downloadFile.getFileId()))
            {
            	EncapsulatedFileDescriptor encapsulatedFileDescriptor = (EncapsulatedFileDescriptor) downloadFile;
                assertEquals("observations-330055-spectra-my_orig_spectrum", encapsulatedFileDescriptor.getFileId());
                assertEquals(20L, encapsulatedFileDescriptor.getSizeKb());
                assertEquals(FileType.SPECTRUM, encapsulatedFileDescriptor.getFileType());
                assertNull(encapsulatedFileDescriptor.getEncapsulationFile());
            }
            else if(downloadFile.getFileType() == FileType.MOMENT_MAP && 
            		"observations-330055-moment_maps-my_encap_moment_map".equals(downloadFile.getFileId()))
            {
            	EncapsulatedFileDescriptor encapsulatedFileDescriptor = (EncapsulatedFileDescriptor) downloadFile;
                assertEquals("observations-330055-moment_maps-my_encap_moment_map", encapsulatedFileDescriptor.getFileId());
                assertEquals(32L, encapsulatedFileDescriptor.getSizeKb());
                assertEquals(FileType.MOMENT_MAP, encapsulatedFileDescriptor.getFileType());
                assertEquals("observations-330055-encapsulation_files-encap_1.tar", 
                		encapsulatedFileDescriptor.getEncapsulationFile().getFileId());
                assertEquals(345L, encapsulatedFileDescriptor.getEncapsulationFile().getSizeKb());    
            }
            else if(downloadFile.getFileType() == FileType.MOMENT_MAP && 
            		"observations-330055-moment_maps-my_orig_moment_map".equals(downloadFile.getFileId()))
            {
            	EncapsulatedFileDescriptor encapsulatedFileDescriptor = (EncapsulatedFileDescriptor) downloadFile;
                assertEquals("observations-330055-moment_maps-my_orig_moment_map", encapsulatedFileDescriptor.getFileId());
                assertEquals(32L, encapsulatedFileDescriptor.getSizeKb());
                assertEquals(FileType.MOMENT_MAP, encapsulatedFileDescriptor.getFileType());
                assertNull(encapsulatedFileDescriptor.getEncapsulationFile());
            }
            else if(downloadFile.getFileType() == FileType.CUBELET 
            		&& "observations-330055-cubelets-my_encap_cubelet".equals(downloadFile.getFileId()))
            {
            	EncapsulatedFileDescriptor encapsulatedFileDescriptor = (EncapsulatedFileDescriptor) downloadFile;
                assertEquals("observations-330055-cubelets-my_encap_cubelet", encapsulatedFileDescriptor.getFileId());
                assertEquals(37L, encapsulatedFileDescriptor.getSizeKb());
                assertEquals(FileType.CUBELET, encapsulatedFileDescriptor.getFileType());
                assertEquals("observations-330055-encapsulation_files-encap_2.tar", 
                		encapsulatedFileDescriptor.getEncapsulationFile().getFileId());
                assertEquals(345L, encapsulatedFileDescriptor.getEncapsulationFile().getSizeKb());    
            }
            else
            {
                assertEquals(FileType.MEASUREMENT_SET, downloadFile.getFileType());
                assertEquals("observations-330055-measurement_sets-my_measurement_set", downloadFile.getFileId());
                assertEquals(4l, downloadFile.getSizeKb());
            }
        }
        assertThat(job2DownloadFiles.size(), is(10));  
    }

    @Test
    public void testPollUntilFileDownloadCompleteAllSuccessful() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-X");

        Collection<DownloadFile> downloadFiles = new ArrayList<>();

        DownloadFile measurementSet = new FileDescriptor("measurement-set-file-id", 15, FileType.MEASUREMENT_SET);
        DownloadFile imageCube = new FileDescriptor("image-cube-file-id", 16, FileType.IMAGE_CUBE);
        ArrayList<Long> catalogueIds = new ArrayList<>();
        catalogueIds.add(13L);
        catalogueIds.add(16L);
        CatalogueDownloadFile catalogueDownloadFile =
                new CatalogueDownloadFile("catalogue-file-id", 17, "catalogue-filename", catalogueIds,
                        CatalogueDownloadFormat.CSV_GROUPED, CatalogueType.CONTINUUM_COMPONENT, null);
        GeneratedFileDescriptor cutout1GFD =
                new GeneratedFileDescriptor(1L, "cutout-1-image-4", 14L, "image-4", 20L, FileType.IMAGE_CUTOUT);
        GeneratedFileDescriptor cutout2GFD =
                new GeneratedFileDescriptor(2L, "cutout-2-image-5", 14L, "image-5", 20L, FileType.IMAGE_CUTOUT);
        GeneratedFileDescriptor spectrum1GFD =
                new GeneratedFileDescriptor(1L, "spectrum-1-image-4", 14L, "image-4", 20L, FileType.GENERATED_SPECTRUM);
        GeneratedFileDescriptor spectrum2GFD =
                new GeneratedFileDescriptor(2L, "spectrum-2-image-5", 14L, "image-5", 20L, FileType.GENERATED_SPECTRUM);
        cutout2GFD.setOriginalImageFilePath(Paths.get("/dir/file").toAbsolutePath().toString());
        spectrum2GFD.setOriginalImageFilePath(Paths.get("/dir/file").toAbsolutePath().toString());
        downloadFiles.add(measurementSet);
        downloadFiles.add(imageCube);
        downloadFiles.add(catalogueDownloadFile);
        downloadFiles.add(cutout1GFD);
        downloadFiles.add(cutout2GFD);
        downloadFiles.add(spectrum1GFD);
        downloadFiles.add(spectrum2GFD);

        for (DownloadFile downloadFile : downloadFiles)
        {
            assertFalse(downloadFile.isComplete());
        }

        doNothing().when(voToolsCataloguePackager).generateCatalogueAndChecksumFile(any(), any(), any());
        doAnswer(new Answer<Long>()
        {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable
            {
                DownloadFile dto = invocation.getArgumentAt(1, DownloadFile.class);
                return dto.getSizeKb();
            }
        }).when(cacheManager).updateSizeForCachedFile(any(), any());

        doReturn(false).doReturn(false).doReturn(true).when(cacheManager).isCachedFileAvailable(measurementSet);
        doReturn(false).doReturn(true).when(cacheManager).isCachedFileAvailable(imageCube);

        doReturn(false).doReturn(false).doReturn(false).doReturn(true).when(cacheManager)
                .isCachedFileAvailable(cutout1GFD);
        doReturn(false).doReturn(true).when(cacheManager)
                .isCachedFileAvailable(cutout1GFD.getOriginalImageDownloadFile());
        doReturn(false).doReturn(true).when(cacheManager).isCachedFileAvailable(cutout2GFD);

        doReturn(false).doReturn(false).doReturn(false).doReturn(true).when(cacheManager)
                .isCachedFileAvailable(spectrum1GFD);
        doReturn(false).doReturn(true).when(cacheManager)
                .isCachedFileAvailable(spectrum1GFD.getOriginalImageDownloadFile());
        doReturn(false).doReturn(true).when(cacheManager).isCachedFileAvailable(spectrum2GFD);

        String tempDirPath = tempFolder.newFolder("dir").getCanonicalPath();
        CachedFile measurementSetCachedFile = createMockCachedFile("measurement-set-file-id",
                Paths.get(tempDirPath, "ms").toAbsolutePath().toString(), 15L, DateTime.now().plusDays(1), false, false,
                true);
        CachedFile imageCubeCachedFile = createMockCachedFile("image-cube-file-id",
                Paths.get(tempDirPath, "ic").toAbsolutePath().toString(), 15L, DateTime.now().plusDays(1), false, true);
        CachedFile imageCachedFile = createMockCachedFile("image-4",
                Paths.get("src/test/resources/testfile/test.txt").toAbsolutePath().toString(), 12L,
                DateTime.now().plusDays(1), false, true);
        CachedFile image5CachedFile = createMockCachedFile("image-5",
                Paths.get("src/test/resources/testfile/test.txt").toAbsolutePath().toString(), 12L,
                DateTime.now().plusDays(1), false, true);
        CachedFile cutout1CachedFile =
                createMockCachedFile("cutout-1-image-4", Paths.get(tempDirPath, "cutout1").toAbsolutePath().toString(),
                        14L, DateTime.now().plusDays(1), false, false, false, true);
        CachedFile cutout2CachedFile =
                createMockCachedFile("cutout-2-image-5", Paths.get(tempDirPath, "cutout2").toAbsolutePath().toString(),
                        14L, DateTime.now().plusDays(1), false, true);
        CachedFile spectrum1CachedFile = createMockCachedFile("spectrum-1-image-4",
                Paths.get(tempDirPath, "cutout1").toAbsolutePath().toString(), 14L, DateTime.now().plusDays(1), false,
                false, false, true);
        CachedFile spectrum2CachedFile = createMockCachedFile("spectrum-2-image-5",
                Paths.get(tempDirPath, "cutout2").toAbsolutePath().toString(), 14L, DateTime.now().plusDays(1), false,
                true);

        when(cacheManager.getCachedFile("image-4")).thenReturn(imageCachedFile);
        when(cacheManager.getCachedFile("image-5")).thenReturn(image5CachedFile);
        when(cacheManager.getCachedFile("cutout-1-image-4")).thenReturn(cutout1CachedFile);
        when(cacheManager.getCachedFile("cutout-2-image-5")).thenReturn(cutout2CachedFile);
        when(cacheManager.getCachedFile("spectrum-1-image-4")).thenReturn(spectrum1CachedFile);
        when(cacheManager.getCachedFile("spectrum-2-image-5")).thenReturn(spectrum2CachedFile);
        when(cacheManager.getCachedFile("measurement-set-file-id")).thenReturn(measurementSetCachedFile);
        when(cacheManager.getCachedFile("image-cube-file-id")).thenReturn(imageCubeCachedFile);

        when(inlineScriptService.callScriptInline(anyString(), anyString())).thenReturn("Dummy checksum");

        packager.pollUntilFileDownloadComplete(job, downloadFiles, DEFAULT_EXPIRY);

        ArgumentCaptor<DateTime> timeCaptor = ArgumentCaptor.forClass(DateTime.class);
        verify(voToolsCataloguePackager, times(1)).generateCatalogueAndChecksumFile(
                eq(cacheManager.getJobDirectory(job)), eq(catalogueDownloadFile), timeCaptor.capture());
        assertEquals(DateTime.now().plusHours(DEFAULT_EXPIRY).getMillis(), timeCaptor.getValue().getMillis(), 1000L);
        verify(cacheManager, times(1)).updateSizeForCachedFile(eq(job), eq(catalogueDownloadFile));
        verify(measurementSetCachedFile, times(3)).isFileAvailableFlag();
        verify(imageCubeCachedFile, times(2)).isFileAvailableFlag();
        verify(cutout1CachedFile, times(4)).isFileAvailableFlag();

        for (DownloadFile downloadFile : downloadFiles)
        {
            assertTrue(downloadFile.isComplete());
            if ("cutout-1-image-4".equals(downloadFile.getFileId()))
            {
                assertEquals(imageCachedFile.getPath(),
                        ((GeneratedFileDescriptor) downloadFile).getOriginalImageFilePath());
            }
            else if ("spectrum-1-image-4".equals(downloadFile.getFileId()))
            {
                assertEquals(imageCachedFile.getPath(),
                        ((GeneratedFileDescriptor) downloadFile).getOriginalImageFilePath());
            }
        }

        String completeLogMessage = CasdaDataAccessEvents.E134.messageBuilder().addTimeTaken(123456789L)
                .add(DateTime.now().minusMillis(9876)).add(DateTime.now()).add(DataLocation.VO_TOOLS)
                .add(DataLocation.DATA_ACCESS).add(17L).add(catalogueDownloadFile.getFileId()).toString();

        testAppender.verifyLogMessage(Level.INFO, "Polling for downloaded files for job request id");
        testAppender.verifyLogMessage(Level.INFO,
                "[source: VO_TOOLS] [destination: DATA_ACCESS] [volumeKB: 17] " + "[fileId: catalogue-file-id]");
        assertThat(completeLogMessage, containsString("[E134] "));
        assertThat(completeLogMessage, containsString("[Data retrieved successfully]"));
        assertThat(completeLogMessage, containsString("[duration:123456789]"));

    }

    private CachedFile createMockCachedFile(String fileId, String path, long sizeKb, DateTime unlock, Boolean... available)
    {
        CachedFile mockFile = mock(CachedFile.class);
        when(mockFile.getFileId()).thenReturn(fileId);
        when(mockFile.getPath()).thenReturn(path);
        when(mockFile.getSizeKb()).thenReturn(sizeKb);
        when(mockFile.getUnlock()).thenReturn(unlock);
        if (available.length > 1)
        {
            when(mockFile.isFileAvailableFlag()).thenReturn(available[0],
                    Arrays.copyOfRange(available, 1, available.length));
        }
        else
        {
            when(mockFile.isFileAvailableFlag()).thenReturn(available[0]);
        }
        return mockFile;
    }

    @Test
    public void testPollUntilFileDownloadCompleteOneFailed() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("Space hasn't been reserved for file ");

        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-X");

        Collection<DownloadFile> downloadFiles = new ArrayList<>();

        DownloadFile measurementSet1 = new FileDescriptor("measurement-set-file-id1", 18, FileType.MEASUREMENT_SET);
        DownloadFile measurementSet2 = new FileDescriptor("measurement-set-file-id2", 19, FileType.MEASUREMENT_SET);

        downloadFiles.add(measurementSet1);
        downloadFiles.add(measurementSet2);
        
        CachedFile cachedFileMs1 = new CachedFile("measurement-set-file-id1", "", 5L, new DateTime());
        cachedFileMs1.setFileAvailableFlag(true);
        CachedFile cachedFileMs2 = new CachedFile("measurement-set-file-id2", "", 5L, new DateTime());
        cachedFileMs2.setFileAvailableFlag(true);

        doReturn(cachedFileMs1).when(cacheManager).getCachedFile(measurementSet1.getFileId());
        doReturn(null).when(cacheManager).getCachedFile(measurementSet2.getFileId());

        packager.pollUntilFileDownloadComplete(job, downloadFiles, DEFAULT_EXPIRY);
    }

    @Test
    public void testCacheExceptionThrownOnReserveSpaceThrowsException() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-D");
        job.setDownloadMode(CasdaDownloadMode.WEB);
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getFileId()).thenReturn("imageCubeFileId");
        when(imageCube.getFilesize()).thenReturn(400L);
        job.addImageCube(imageCube);

        CacheException cacheException = new CacheException("test exception");
        doThrow(cacheException).when(cacheManager).reserveSpaceAndRegisterFilesForDownload(any(), any());

        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        thrown.expect(CacheException.class);
        thrown.expectMessage(cacheException.getMessage());

        packager.pack(job, DEFAULT_EXPIRY);
    }

    @Test
    public void testCacheExceptionThrownOnDownloadThrowsException() throws Exception
    {
        when(dataAccessService.getPaging(any(String.class), any(Boolean.class))).thenReturn(getPagingDetails());
        when(dataAccessService.getPageOfFiles(any(Map.class), any(DataAccessJob.class))).thenReturn(getImageCubes());
        
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-E");
        job.setDownloadMode(CasdaDownloadMode.WEB);
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getFileId()).thenReturn("imageCubeFileId");
        when(imageCube.getFilesize()).thenReturn(400L);
        job.addImageCube(imageCube);

        doReturn(new Object[] { 0L, null }).when(cacheManager).reserveSpaceAndRegisterFilesForDownload(any(), any());
        CacheException cacheException = new CacheException("test exception");
        doThrow(cacheException).when(packager).pollUntilFileDownloadComplete(eq(job), any(), eq(DEFAULT_EXPIRY));

        thrown.expect(CacheException.class);
        thrown.expectMessage(cacheException.getMessage());

        packager.pack(job, DEFAULT_EXPIRY);
    }

    @Test
    public void testWriteErrorFileAndChecksum() throws Exception
    {
        String expectedChecksum = "Dummy checksum";
        when(inlineScriptService.callScriptInline(anyString(), anyString())).thenReturn(expectedChecksum);

        File jobDir = tempFolder.newFolder("job");
        String fileId = "error-01.txt";
        String expectedMessage = "This is an error";
        ErrorFileDescriptor errorFileDesc = new ErrorFileDescriptor(fileId, expectedMessage);
        packager.writeErrorFileAndChecksum(jobDir, errorFileDesc);

        File errFile = new File(jobDir, fileId);
        assertThat(errFile.exists(), is(true));
        assertThat(FileUtils.readFileToString(errFile), is(expectedMessage));
        File checksumFile = new File(jobDir, fileId + ".checksum");
        assertThat(checksumFile.exists(), is(true));
        assertThat(FileUtils.readFileToString(checksumFile), is(expectedChecksum));
    }

}
