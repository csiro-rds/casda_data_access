package au.csiro.casda.access.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

import java.io.File;

import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.GeneratedSpectrumRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.jpa.ThumbnailRepository;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.Cubelet;
import au.csiro.casda.entity.observation.MomentMap;
import au.csiro.casda.entity.observation.Spectrum;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.jobmanager.JobManager.Job;
import au.csiro.casda.jobmanager.JobManager.JobStatus;
import au.csiro.casda.jobmanager.ProcessJob;

/**
 * Tests the download manager
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
public class DownloadManagerTest
{

    @Mock
    private JobManager jobManager;

    @Mock
    private CachedFileRepository cachedFileRepository;

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
    private InlineScriptService inlineScriptService;
    
    @Mock
    private JobStatus mockSuccess;
    @Mock
    private JobStatus mockFailed;
    @Mock
    private JobStatus mockRunning;

    private DownloadManager downloadManager;

    private Log4JTestAppender testAppender;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final int MAX_DOWNLOAD_ATTEMPTS = 3;

    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        testAppender = Log4JTestAppender.createAppender();
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
                thumbnailRepository, jobManager, "depositToolsWorkingDirectory", MAX_DOWNLOAD_ATTEMPTS,
                mock(CasdaToolProcessJobBuilderFactory.class), DownloadManager.ProcessJobType.SIMPLE.toString(),
                downloadCommandAndArgs, null, cutoutCommandAndArgs, pngCutoutCommandAndArgs, generateSpectrumCommand,
                encapCommandAndArgs, processJobFactory, inlineScriptService, ""));

        when(mockSuccess.isFailed()).thenReturn(false);
        when(mockSuccess.isFinished()).thenReturn(true);

        when(mockFailed.isFailed()).thenReturn(true);
        when(mockFailed.isFinished()).thenReturn(true);
        when(mockFailed.getFailureCause()).thenReturn("failure cause");
        when(mockFailed.getJobOutput()).thenReturn("job output");

        when(mockRunning.isFailed()).thenReturn(false);
        when(mockRunning.isFinished()).thenReturn(false);
    }

    @Test
    public void testPollJobManagerNewFileStartsJob() throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId("file-id-1");
        newFile.setPath("dest/file-id-1");
        when(jobManager.getJobStatus(anyString())).thenReturn(mockRunning);
        when(jobManager.getJobStatus("")).thenReturn(null);

        downloadManager.pollJobManagerForDownloadJob(newFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the new file was started to download
        assertEquals(0, newFile.getDownloadJobRetryCount());
        assertThat(newFile.getDownloadJobId(), startsWith("DataAccess-file-id-1-"));
        assertThat(newFile.getDownloadJobId(), endsWith("-0"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=file-id-1", processJob.getCommandAndArgs()[1]);
        assertEquals("destination=dest/file-id-1", processJob.getCommandAndArgs()[2]);
        assertEquals(newFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository).save(newFile);
    }

    @Test
    public void testPollJobManagerNewEncapsulatedFileStartsJob() throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId("observations-333436-moment_maps-mom1_2.fits");
        newFile.setPath("/ASKAP/access/dev/vol001/cache/data/2016-12-05/observations-333436-moment_maps-mom1_2.fits");
        newFile.setOriginalFilePath(
                "ASKAPArchive/2016-11-29/1/observations-333436-encapsulation_files-encaps-mom-7.tar");
        newFile.setFileType(FileType.MOMENT_MAP);
        when(jobManager.getJobStatus(anyString())).thenReturn(mockRunning);
        when(jobManager.getJobStatus("")).thenReturn(null);

        downloadManager.pollJobManagerForDownloadJob(newFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the new file was started to download
        assertEquals(0, newFile.getDownloadJobRetryCount());
        assertThat(newFile.getDownloadJobId(), startsWith("DataAccess-observations-333436-moment_maps-mom1_2.fits-"));
        assertThat(newFile.getDownloadJobId(), endsWith("-0"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("unencapsulate", processJob.getCommandAndArgs()[0]);
        assertEquals("ASKAPArchive/2016-11-29/1/observations-333436-encapsulation_files-encaps-mom-7.tar",
                processJob.getCommandAndArgs()[1]);
        assertEquals("mom1_2.fits", processJob.getCommandAndArgs()[2]);
        assertEquals("observations-333436-moment_maps-mom1_2.fits", processJob.getCommandAndArgs()[3]);
        assertEquals(newFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository).save(newFile);
    }

    @Test
    public void testPollJobManagerNewUnencapsulatedFileStartsJob() throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId("observations-333436-moment_maps-mom1_2.fits");
        newFile.setPath("/ASKAP/access/dev/vol001/cache/data/2016-12-05/observations-333436-moment_maps-mom1_2.fits");
        newFile.setOriginalFilePath(
                "ASKAPArchive/2016-11-29/1/observations-333436-moment_maps-mom1_2.fits");
        newFile.setFileType(FileType.MOMENT_MAP);
        when(jobManager.getJobStatus(anyString())).thenReturn(mockRunning);
        when(jobManager.getJobStatus("")).thenReturn(null);

        downloadManager.pollJobManagerForDownloadJob(newFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the new file was started to download
        assertEquals(0, newFile.getDownloadJobRetryCount());
        assertThat(newFile.getDownloadJobId(), startsWith("DataAccess-observations-333436-moment_maps-mom1_2.fits-"));
        assertThat(newFile.getDownloadJobId(), endsWith("-0"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=observations-333436-moment_maps-mom1_2.fits", processJob.getCommandAndArgs()[1]);
        assertEquals(
                "destination=/ASKAP/access/dev/vol001/cache/data/2016-12-05/observations-333436-moment_maps-mom1_2.fits",
                processJob.getCommandAndArgs()[2]);
        assertEquals(newFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository).save(newFile);
    }

    @Test
    public void testPollJobManagerNewFileStartFailsIncrementsDownloadRetries() throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId("file-id-1");
        newFile.setPath("dest/file-id-1");
        // will not find the status with the job manager for a new file because the job id is empty

        doThrow(new RuntimeException("exception")).when(jobManager).startJob(any(Job.class));

        downloadManager.pollJobManagerForDownloadJob(newFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the new file was started to download
        assertEquals(1, newFile.getDownloadJobRetryCount());
        assertThat(newFile.getDownloadJobId(), startsWith("DataAccess-file-id-1-"));
        assertThat(newFile.getDownloadJobId(), endsWith("-0"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=file-id-1", processJob.getCommandAndArgs()[1]);
        assertEquals("destination=dest/file-id-1", processJob.getCommandAndArgs()[2]);
        assertEquals(newFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository).save(newFile);
    }

    @Test
    public void testPollJobManagerNewFileThrottled() throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId("file-id-1");
        newFile.setPath("dest/file-id-1");
        when(jobManager.getJobStatus("")).thenReturn(null);

        downloadManager.pollJobManagerForDownloadJob(newFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the new file was prepared for download
        assertEquals(0, newFile.getDownloadJobRetryCount());
        assertThat(newFile.getDownloadJobId(), startsWith("DataAccess-file-id-1-"));
        assertThat(newFile.getDownloadJobId(), endsWith("-0"));

        // Verify the request to start the job
        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=file-id-1", processJob.getCommandAndArgs()[1]);
        assertEquals("destination=dest/file-id-1", processJob.getCommandAndArgs()[2]);
        assertEquals(newFile.getDownloadJobId(), processJob.getId());

        // Verify the job was not saved as it was not started.
        verify(cachedFileRepository, never()).save(newFile);
    }

    @Test
    public void testPollJobManagerCurrentlyDownloadingFileDoesNothing() throws CacheException
    {

        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileId("file-id-2");
        downloadingFile.setPath("dest/file-id-2");
        downloadingFile.setDownloadJobId("running-job-id");
        downloadingFile.setDownloadJobRetryCount(1);

        doReturn(mockRunning).when(jobManager).getJobStatus("running-job-id");

        downloadManager.pollJobManagerForDownloadJob(downloadingFile);

        verify(jobManager, never()).startJob(any());

        // verify that the running file is skipped
        assertEquals(1, downloadingFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, never()).save(downloadingFile);
    }

    @Test
    public void testPollJobManagerCurrentlyDownloadingEncapsFileDoesNothing() throws CacheException
    {

        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileId("file-id-2");
        downloadingFile.setPath("dest/file-id-2");
        downloadingFile.setOriginalFilePath("dest/encaps-spectrum-1.tar");
        downloadingFile.setDownloadJobId("running-job-id");
        downloadingFile.setDownloadJobRetryCount(1);

        doReturn(mockRunning).when(jobManager).getJobStatus("running-job-id");

        downloadManager.pollJobManagerForDownloadJob(downloadingFile);

        verify(jobManager, never()).startJob(any());

        // verify that the running file is skipped
        assertEquals(1, downloadingFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, never()).save(downloadingFile);
    }

    @Test
    public void testPollJobManagerCompletedFileUpdatesCache() throws Exception
    {
        CachedFile completedFile = new CachedFile();
        completedFile.setFileId("test.txt");
        completedFile.setPath("src/test/resources/testfile/test.txt");
        completedFile.setDownloadJobId("completed-job-id");
        completedFile.setDownloadJobRetryCount(2);

        doReturn(mockSuccess).when(jobManager).getJobStatus("completed-job-id");

        downloadManager.pollJobManagerForDownloadJob(completedFile);

        verify(jobManager, never()).startJob(any());
        verify(inlineScriptService, never()).callScriptInline(anyString(), anyString());


        // verify that the completed job is updated
        assertTrue(completedFile.isFileAvailableFlag());
        assertEquals(2, completedFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, times(1)).save(completedFile);
    }

    @Test
    public void testPollJobManagerFailedFileRestartsDownload() throws CacheException
    {
        CachedFile failedFile = new CachedFile();
        failedFile.setFileId("file-id-3");
        failedFile.setPath("dest/file-id-3");
        failedFile.setDownloadJobId("failed-job-id");
        failedFile.setDownloadJobRetryCount(1);

        when(jobManager.getJobStatus(anyString())).thenReturn(mockFailed);

        downloadManager.pollJobManagerForDownloadJob(failedFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the failed job is restarted
        assertEquals(2, failedFile.getDownloadJobRetryCount());
        assertThat(failedFile.getDownloadJobId(), startsWith("DataAccess-file-id-3-"));
        assertThat(failedFile.getDownloadJobId(), endsWith("-2"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=file-id-3", processJob.getCommandAndArgs()[1]);
        assertEquals("destination=dest/file-id-3", processJob.getCommandAndArgs()[2]);
        assertEquals(failedFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository, times(2)).save(failedFile);

        testAppender.verifyLogMessage(Level.ERROR, allOf(containsString("Exxx"), containsString("failed-job-id"),
                containsString("failure cause"), containsString("job output")), sameInstance((Throwable) null));
        testAppender.verifyLogMessage(Level.INFO,
                allOf(containsString("Retried"), containsString("DataAccess-file-id-3-")),
                sameInstance((Throwable) null));
        testAppender.verifyNoMessages();
    }

    @Test
    public void testPollJobManagerNoMoreAttemptsAllowedUpdatesCacheAndDoesntRestart() throws CacheException
    {

        CachedFile noMoreAttemptsFile = new CachedFile();
        noMoreAttemptsFile.setFileId("file-id-3a");
        noMoreAttemptsFile.setPath("dest/file-id-3a");
        noMoreAttemptsFile.setDownloadJobId("no-more-attempts-job-id");
        noMoreAttemptsFile.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS);

        doReturn(mockFailed).when(jobManager).getJobStatus("no-more-attempts-job-id");

        try
        {
            downloadManager.pollJobManagerForDownloadJob(noMoreAttemptsFile);
            fail("Expected the call to to fail");
        }
        catch (CacheException ce)
        {
            assertThat(ce.getMessage(), is("File file-id-3a could not be retrieved."));
        }

        verify(jobManager, never()).startJob(any());

        // verify that the no more attempts left is updated
        assertEquals(MAX_DOWNLOAD_ATTEMPTS + 1, noMoreAttemptsFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, times(1)).save(noMoreAttemptsFile);

        testAppender
                .verifyLogMessage(Level.ERROR,
                        allOf(containsString("Exxx"), containsString("no-more-attempts-job-id"),
                                containsString("failure cause"), containsString("job output")),
                        sameInstance((Throwable) null));
        testAppender.verifyLogMessage(Level.WARN,
                allOf(containsString("All download attempts failed"), containsString("file-id-3a")),
                sameInstance((Throwable) null));
        testAppender.verifyNoMessages();
    }

    @Test
    public void testPollJobManagerJobCompleteButFileMissingRestartsDownload() throws CacheException
    {

        CachedFile missingFile = new CachedFile();
        missingFile.setFileId("file-id-4");
        missingFile.setPath("dest/file-id-4");
        missingFile.setDownloadJobId("missing-job-id");
        missingFile.setDownloadJobRetryCount(1);

        when(jobManager.getJobStatus(anyString())).thenReturn(mockSuccess);

        downloadManager.pollJobManagerForDownloadJob(missingFile);

        ArgumentCaptor<ProcessJob> processJobCaptor = ArgumentCaptor.forClass(ProcessJob.class);
        verify(jobManager, times(1)).startJob(processJobCaptor.capture());

        // verify that the missing file is restarted
        assertEquals(2, missingFile.getDownloadJobRetryCount());
        assertThat(missingFile.getDownloadJobId(), startsWith("DataAccess-file-id-4-"));
        assertThat(missingFile.getDownloadJobId(), endsWith("-2"));

        ProcessJob processJob = processJobCaptor.getValue();
        assertEquals("download", processJob.getCommandAndArgs()[0]);
        assertEquals("fileId=file-id-4", processJob.getCommandAndArgs()[1]);
        assertEquals("destination=dest/file-id-4", processJob.getCommandAndArgs()[2]);
        assertEquals(missingFile.getDownloadJobId(), processJob.getId());
        verify(cachedFileRepository, times(2)).save(missingFile);
    }

    @Test
    public void testBuildCutoutJob2D()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileType(FileType.IMAGE_CUTOUT);
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildImageCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "12.0", "-34.0", "5", "5" }));
    }

    @Test
    public void testBuildCutoutJobSinglePLane()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileType(FileType.IMAGE_CUTOUT);
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.5 6.0 6.0 D null null N 1");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildImageCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "12.0", "-34.5", "6.0", "6.0" }));
    }

    @Test
    public void testBuildCutoutJob3D()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileType(FileType.IMAGE_CUTOUT);
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5 D 1:3");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildImageCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "-D3", "1:3", "12.0", "-34.0", "5", "5" }));
    }

    @Test
    public void testBuildCutoutJob4D()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileType(FileType.IMAGE_CUTOUT);
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5 D 1:3 2");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildImageCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(), is(new String[] { "cmd", "arg1", sourcePath, destPath, "-D3", "1:3",
                "-D4", "2", "12.0", "-34.0", "5", "5" }));
    }
    
    @Test
    public void testgetFileNameInArchive()
    {
        CachedFile testFile = new CachedFile();
        testFile.setFileId("observations-333436-spectra-spec_1.fits");
        assertThat(downloadManager.getFileNameInArchive(testFile), is("spec_1.fits"));
        testFile.setFileId("observations-333436-moment_map-mom0_3.fits");
        assertThat(downloadManager.getFileNameInArchive(testFile), is("mom0_3.fits"));
        testFile.setFileId("tribble.fits");
        assertThat(downloadManager.getFileNameInArchive(testFile), is(""));
        
        Spectrum spectrum = new Spectrum();
        spectrum.setFilename("spec_1.fits");
        when(spectrumRepository.findOne(1019L)).thenReturn(spectrum);
        testFile.setFileId("observations-333436-spectra-1019.fits");
        testFile.setFileType(FileType.SPECTRUM);
        assertThat(downloadManager.getFileNameInArchive(testFile), is("spec_1.fits"));

        testFile.setFileId("level7-1741-spectra-1019.fits");
        testFile.setFileType(FileType.SPECTRUM);
        assertThat(downloadManager.getFileNameInArchive(testFile), is("spec_1.fits"));
        
        MomentMap momentMap = new MomentMap();
        momentMap.setFilename("mom0_3.fits");
        when(momentMapRepository.findOne(4200L)).thenReturn(momentMap);
        testFile.setFileType(FileType.MOMENT_MAP);
        testFile.setFileId("observations-333436-moment_map-4200.fits");
        assertThat(downloadManager.getFileNameInArchive(testFile), is("mom0_3.fits"));

        testFile.setFileId("level7-1741-moment_map-4200.fits");
        testFile.setFileType(FileType.MOMENT_MAP);
        assertThat(downloadManager.getFileNameInArchive(testFile), is("mom0_3.fits"));
        
        Cubelet cubelet = new Cubelet();
        cubelet.setFilename("cube_123.fits");
        when(cubeletRepository.findOne(1234L)).thenReturn(cubelet);
        testFile.setFileType(FileType.CUBELET);
        testFile.setFileId("observations-333436-cubelet-1234.fits");
        assertThat(downloadManager.getFileNameInArchive(testFile), is("cube_123.fits"));

        testFile.setFileId("level7-1741-cubelet-1234.fits");
        testFile.setFileType(FileType.CUBELET);
        assertThat(downloadManager.getFileNameInArchive(testFile), is("cube_123.fits"));
    }
    
    @Test
    public void testCutoutJobBuildsChecksum() throws Exception
    {
        File testFile = tempFolder.newFile("test.txt");
        CachedFile completedFile = new CachedFile();
        completedFile.setFileId(testFile.getName());
        completedFile.setPath(testFile.getPath());
        completedFile.setDownloadJobId("completed-job-id");
        completedFile.setDownloadJobRetryCount(2);
        completedFile.setFileType(FileType.IMAGE_CUTOUT);
        String path = testFile.getCanonicalPath();
        when(inlineScriptService.callScriptInline("", path)).thenReturn("a b c");

        doReturn(mockSuccess).when(jobManager).getJobStatus("completed-job-id");

        downloadManager.pollJobManagerForDownloadJob(completedFile);

        verify(jobManager, never()).startJob(any());
        verify(inlineScriptService).callScriptInline(anyString(), anyString());
    }
}
