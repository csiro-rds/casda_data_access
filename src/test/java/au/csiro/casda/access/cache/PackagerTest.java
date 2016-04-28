package au.csiro.casda.access.cache;

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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

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
import au.csiro.casda.access.CutoutFileDescriptor;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.cache.Packager.Result;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.rest.VoToolsCataloguePackager;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;
import au.csiro.casda.jobmanager.CasdaToolProcessJobBuilder;
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
    private DataAccessService dataAccessService;

    @Mock
    private VoToolsCataloguePackager voToolsCataloguePackager;

    @Mock
    private JobManager jobManager;

    @Mock
    private CasdaToolProcessJobBuilder processBuilder;

    @Mock
    private InlineScriptService inlineScriptService;

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

        packager = spy(
                new Packager(cacheManager, voToolsCataloguePackager, dataAccessService, 1, inlineScriptService, ""));
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
        testPackWorkflow(CasdaDownloadMode.WEB);
    }

    @Test
    public void testPackWorkflowPawseyDownload() throws Exception
    {
        testPackWorkflow(CasdaDownloadMode.PAWSEY_HTTP);
    }

    @Test
    public void testPackWorkflowSiapAsync() throws Exception
    {
        testPackWorkflow(CasdaDownloadMode.SIAP_ASYNC);
    }

    @Test
    public void testPackWorkflowSiapSync() throws Exception
    {
        testPackWorkflow(CasdaDownloadMode.SIAP_SYNC);
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

        doReturn(15L).when(cacheManager).reserveSpaceAndRegisterFilesForDownload(any(), eq(job));
        doNothing().when(packager).pollUntilFileDownloadComplete(any(DataAccessJob.class), any(Collection.class),
                any(Integer.class));
        doNothing().when(cacheManager).createDataAccessJobDirectory(any(), any());
        doNothing().when(cacheManager).updateUnlockForFiles(any(), any());
        doNothing().when(dataAccessService).updateFileSizeForCutouts(any(), any());

        DateTime approximateExpiryDateTime;
        int hoursToExpiryForJob = CasdaDownloadMode.SIAP_SYNC == downloadMode ? SYNC_EXPIRY : DEFAULT_EXPIRY;
        approximateExpiryDateTime = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob);

        Result response = packager.pack(job, hoursToExpiryForJob);

        ArgumentCaptor<Collection> downloadFilesCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<DateTime> timeCaptor = ArgumentCaptor.forClass(DateTime.class);
        verify(cacheManager, times(1)).reserveSpaceAndRegisterFilesForDownload(downloadFilesCaptor.capture(), eq(job));
        verify(packager, times(1)).pollUntilFileDownloadComplete(eq(job), downloadFilesCaptor.capture(),
                eq(hoursToExpiryForJob));
        verify(cacheManager, times(1)).createDataAccessJobDirectory(eq(job), downloadFilesCaptor.capture());
        verify(cacheManager, times(1)).updateUnlockForFiles(downloadFilesCaptor.capture(), timeCaptor.capture());
        verify(dataAccessService, times(1)).updateFileSizeForCutouts(eq(job.getRequestId()),
                downloadFilesCaptor.capture());
        // job expiry same as the unlock time because it's web download
        DateTime unlockTime = timeCaptor.getValue();
        assertEquals(response.getExpiryDate().getMillis(), unlockTime.getMillis());
        assertThat(new Period(approximateExpiryDateTime, timeCaptor.getValue()).toStandardSeconds().getSeconds(),
                allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        assertEquals(15L, response.getCachedSizeKb());
        assertEquals(15L, response.getTotalSizeKb());
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
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-C1");
        assertThat(packager.assembleDataAccessJobDownloadFiles(job), is(empty()));

        DataAccessJob job2 = new DataAccessJob();
        job2.setRequestId("ABC-123-C2");
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getId()).thenReturn(14L);
        when(imageCube.getFileId()).thenReturn("image_cube_file_id");
        when(imageCube.getFilesize()).thenReturn(15L);
        job2.addImageCube(imageCube);

        MeasurementSet measurementSet = mock(MeasurementSet.class);
        when(measurementSet.getFileId()).thenReturn("measurement_set_file_id");
        job2.addMeasurementSet(measurementSet);

        Catalogue catalogue = new Catalogue(CatalogueType.CONTINUUM_COMPONENT);
        catalogue.setId(17L);
        job2.addCatalogue(catalogue);
        Project project = mock(Project.class);
        when(project.getOpalCode()).thenReturn("ABC123");
        catalogue.setProject(project);
        Observation observation = mock(Observation.class);
        when(observation.getSbid()).thenReturn(12345);
        catalogue.setParent(observation);
        job2.setDownloadFormat(CatalogueDownloadFormat.CSV_INDIVIDUAL.name());

        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setId(12L);
        imageCutout.setImageCube(imageCube);
        imageCutout.setFilesize(12L);
        job2.addImageCutout(imageCutout);

        ImageCube imageCubeInCache = mock(ImageCube.class);
        when(imageCubeInCache.getId()).thenReturn(18L);
        when(imageCubeInCache.getFileId()).thenReturn("image_cube_in_cache_file_id");
        when(imageCubeInCache.getFilesize()).thenReturn(20L);
        ImageCutout imageCutout2 = new ImageCutout();
        imageCutout2.setId(15L);
        imageCutout2.setImageCube(imageCubeInCache);
        imageCutout2.setFilesize(13L);
        job2.addImageCutout(imageCutout2);
        when(dataAccessService.findFileInNgasIfOnDisk("image_cube_in_cache_file_id"))
                .thenReturn(Paths.get("/dir/file"));

        Collection<DownloadFile> job2DownloadFiles = packager.assembleDataAccessJobDownloadFiles(job2);
        assertThat(job2DownloadFiles.size(), is(5));
        for (DownloadFile downloadFile : job2DownloadFiles)
        {
            if (downloadFile.getFileType() == FileType.CATALOGUE)
            {
                CatalogueDownloadFile catalogueDownloadFile = (CatalogueDownloadFile) downloadFile;
                assertEquals("ABC-123-C2-ABC123_Continuum_Component_Catalogue_12345_17.csv",
                        catalogueDownloadFile.getFileId());
                assertEquals("ABC123_Continuum_Component_Catalogue_12345_17.csv", catalogueDownloadFile.getFilename());
                assertEquals(0L, catalogueDownloadFile.getSizeKb());
                assertThat(catalogueDownloadFile.getCatalogueIds(), contains(17L));
                assertEquals(CatalogueType.CONTINUUM_COMPONENT, catalogueDownloadFile.getCatalogueType());
                assertEquals(CatalogueDownloadFormat.CSV_INDIVIDUAL, catalogueDownloadFile.getDownloadFormat());
            }
            else if (downloadFile.getFileType() == FileType.IMAGE_CUBE)
            {
                assertEquals("image_cube_file_id", downloadFile.getFileId());
                assertEquals(15L, downloadFile.getSizeKb());
            }
            else if (downloadFile.getFileType() == FileType.IMAGE_CUTOUT
                    && "cutout-12-imagecube-14.fits".equals(downloadFile.getFileId()))
            {
                CutoutFileDescriptor cutoutDownloadFile = (CutoutFileDescriptor) downloadFile;
                assertEquals(12L, cutoutDownloadFile.getSizeKb());
                assertNull(cutoutDownloadFile.getOriginalImageFilePath());
                assertEquals("cutout-12-imagecube-14.fits", cutoutDownloadFile.getDisplayName());
                assertEquals("cutout-12-imagecube-14.fits", cutoutDownloadFile.getFilename());
                assertEquals("image_cube_file_id", cutoutDownloadFile.getOriginalImageDownloadFile().getFileId());
                assertEquals(15L, cutoutDownloadFile.getOriginalImageDownloadFile().getSizeKb());
                assertEquals(FileType.IMAGE_CUBE, cutoutDownloadFile.getOriginalImageDownloadFile().getFileType());

            }
            else if (downloadFile.getFileType() == FileType.IMAGE_CUTOUT
                    && "cutout-15-imagecube-18.fits".equals(downloadFile.getFileId()))
            {
                CutoutFileDescriptor cutoutDownloadFile = (CutoutFileDescriptor) downloadFile;
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
            else
            {
                assertEquals(FileType.MEASUREMENT_SET, downloadFile.getFileType());
                assertEquals("measurement_set_file_id", downloadFile.getFileId());
                assertEquals(0l, downloadFile.getSizeKb());
            }

        }
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
        CutoutFileDescriptor cutoutFileDescriptor =
                new CutoutFileDescriptor(1L, "cutout-1-image-4", 14L, "image-4", 20L);
        CutoutFileDescriptor cutoutFileDescriptorImageOnDisk =
                new CutoutFileDescriptor(2L, "cutout-2-image-5", 14L, "image-5", 20L);
        cutoutFileDescriptorImageOnDisk.setOriginalImageFilePath(Paths.get("/dir/file").toAbsolutePath().toString());
        downloadFiles.add(measurementSet);
        downloadFiles.add(imageCube);
        downloadFiles.add(catalogueDownloadFile);
        downloadFiles.add(cutoutFileDescriptor);
        downloadFiles.add(cutoutFileDescriptorImageOnDisk);

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
                .isCachedFileAvailable(cutoutFileDescriptor);
        doReturn(false).doReturn(true).when(cacheManager)
                .isCachedFileAvailable(cutoutFileDescriptor.getOriginalImageDownloadFile());
        doReturn(false).doReturn(true).when(cacheManager).isCachedFileAvailable(cutoutFileDescriptorImageOnDisk);

        CachedFile imageCachedFile =
                new CachedFile("image-4", Paths.get("src/test/resources/testfile/test.txt").toAbsolutePath().toString(),
                        12L, DateTime.now().plusDays(1));
        String tempDirPath = tempFolder.newFolder("dir").getCanonicalPath();
        CachedFile cutout1CachedFile = new CachedFile("cutout-1-image-4",
                Paths.get(tempDirPath, "cutout1").toAbsolutePath().toString(), 14L, DateTime.now().plusDays(1));
        CachedFile cutout2CachedFile = new CachedFile("cutout-2-image-5",
                Paths.get(tempDirPath, "cutout2").toAbsolutePath().toString(), 14L, DateTime.now().plusDays(1));

        when(cacheManager.getCachedFile("image-4")).thenReturn(imageCachedFile);
        when(cacheManager.getCachedFile("cutout-1-image-4")).thenReturn(cutout1CachedFile);
        when(cacheManager.getCachedFile("cutout-2-image-5")).thenReturn(cutout2CachedFile);
        when(inlineScriptService.callScriptInline(anyString(), anyString())).thenReturn("Dummy checksum");

        packager.pollUntilFileDownloadComplete(job, downloadFiles, DEFAULT_EXPIRY);

        ArgumentCaptor<DateTime> timeCaptor = ArgumentCaptor.forClass(DateTime.class);
        verify(voToolsCataloguePackager, times(1)).generateCatalogueAndChecksumFile(
                eq(cacheManager.getJobDirectory(job)), eq(catalogueDownloadFile), timeCaptor.capture());
        assertEquals(DateTime.now().plusHours(DEFAULT_EXPIRY).getMillis(), timeCaptor.getValue().getMillis(), 1000L);
        verify(cacheManager, times(1)).updateSizeForCachedFile(eq(job), eq(catalogueDownloadFile));
        verify(cacheManager, times(3)).isCachedFileAvailable(eq(measurementSet));
        verify(cacheManager, times(2)).isCachedFileAvailable(eq(imageCube));
        verify(cacheManager, times(4)).isCachedFileAvailable(eq(cutoutFileDescriptor));
        verify(cacheManager, never())
                .isCachedFileAvailable(eq(cutoutFileDescriptorImageOnDisk.getOriginalImageDownloadFile()));
        verify(cacheManager, times(1)).updateOriginalFilePath(eq(cutoutFileDescriptor), eq(imageCachedFile.getPath()));
        verify(cacheManager, never()).updateOriginalFilePath(eq(cutoutFileDescriptorImageOnDisk), any());

        for (DownloadFile downloadFile : downloadFiles)
        {
            assertTrue(downloadFile.isComplete());
            if ("cutout-1-image-4".equals(downloadFile.getFileId()))
            {
                assertEquals(imageCachedFile.getPath(),
                        ((CutoutFileDescriptor) downloadFile).getOriginalImageFilePath());
            }
        }

        String completeLogMessage = CasdaDataAccessEvents.E134.messageBuilder().addTimeTaken(123456789L)
                .add(DateTime.now().minusMillis(9876)).add(DateTime.now()).add(DataLocation.VO_TOOLS)
                .add(DataLocation.DATA_ACCESS).add(17L).add(catalogueDownloadFile.getFileId()).toString();

        testAppender.verifyLogMessage(Level.INFO,
                "[source: VO_TOOLS] [destination: DATA_ACCESS] [volumeKB: 17] " + "[fileId: catalogue-file-id]");
        assertThat(completeLogMessage, containsString("[E134] "));
        assertThat(completeLogMessage, containsString("[Data retrieved successfully]"));
        assertThat(completeLogMessage, containsString("[duration:123456789]"));
    }

    @Test
    public void testPollUntilFileDownloadCompleteOneFailed() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("some message");

        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-X");

        Collection<DownloadFile> downloadFiles = new ArrayList<>();

        DownloadFile measurementSet1 = new FileDescriptor("measurement-set-file-id1", 18, FileType.MEASUREMENT_SET);
        DownloadFile measurementSet2 = new FileDescriptor("measurement-set-file-id2", 19, FileType.MEASUREMENT_SET);

        downloadFiles.add(measurementSet1);
        downloadFiles.add(measurementSet2);

        doReturn(true).when(cacheManager).isCachedFileAvailable(measurementSet1);
        doThrow(new CacheException("some message")).when(cacheManager).isCachedFileAvailable(measurementSet2);

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

        thrown.expect(CacheException.class);
        thrown.expectMessage(cacheException.getMessage());

        packager.pack(job, DEFAULT_EXPIRY);
    }

    @Test
    public void testCacheExceptionThrownOnDownloadThrowsException() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-E");
        job.setDownloadMode(CasdaDownloadMode.WEB);
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getFileId()).thenReturn("imageCubeFileId");
        when(imageCube.getFilesize()).thenReturn(400L);
        job.addImageCube(imageCube);

        doReturn(0L).when(cacheManager).reserveSpaceAndRegisterFilesForDownload(any(), any());
        CacheException cacheException = new CacheException("test exception");
        doThrow(cacheException).when(packager).pollUntilFileDownloadComplete(eq(job), any(), eq(DEFAULT_EXPIRY));

        thrown.expect(CacheException.class);
        thrown.expectMessage(cacheException.getMessage());

        packager.pack(job, DEFAULT_EXPIRY);
    }

}
