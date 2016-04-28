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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.Page;

import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.ImageCutout;
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
    ImageCutoutRepository imageCutoutRepository;

    @Mock
    private JobStatus mockSuccess;
    @Mock
    private JobStatus mockFailed;
    @Mock
    private JobStatus mockRunning;

    private DownloadManager downloadManager;

    private Log4JTestAppender testAppender;

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
        downloadManager = spy(new DownloadManager(cachedFileRepository, imageCutoutRepository, jobManager,
                "depositToolsWorkingDirectory", MAX_DOWNLOAD_ATTEMPTS, mock(CasdaToolProcessJobBuilderFactory.class),
                DownloadManager.ProcessJobType.SIMPLE.toString(), downloadCommandAndArgs, null, cutoutCommandAndArgs,
                processJobFactory));

        when(mockSuccess.isFailed()).thenReturn(false);
        when(mockSuccess.isFinished()).thenReturn(true);

        when(mockFailed.isFailed()).thenReturn(true);
        when(mockFailed.isFinished()).thenReturn(true);
        when(mockFailed.getFailureCause()).thenReturn("failure cause");
        when(mockFailed.getJobOutput()).thenReturn("job output");

        when(mockRunning.isFailed()).thenReturn(false);
        when(mockRunning.isFinished()).thenReturn(false);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPollJobManagerNoFilesDownloading()
    {
        Page<CachedFile> downloadingFiles = mock(Page.class);
        doReturn(false).when(downloadingFiles).hasContent();
        doReturn(downloadingFiles).when(cachedFileRepository).findDownloadingCachedFiles(eq(MAX_DOWNLOAD_ATTEMPTS),
                any());

        downloadManager.pollJobManagerForDownloadJobs();

        verify(jobManager, never()).getJobStatus(anyString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPollJobManagerPaging()
    {
        Page<CachedFile> downloadingFiles = mock(Page.class);
        doReturn(true).doReturn(true).doReturn(false).when(downloadingFiles).hasContent();
        doReturn(true).doReturn(false).when(downloadingFiles).hasNext();

        List<CachedFile> cachedPage1 = new ArrayList<>();
        CachedFile cachedFile1 = new CachedFile("file1", "", 14L, DateTime.now());
        CachedFile cachedFile2 = new CachedFile("file2", "", 15L, DateTime.now());
        cachedPage1.add(cachedFile1);
        cachedPage1.add(cachedFile2);
        List<CachedFile> cachedPage2 = new ArrayList<>();
        CachedFile cachedFile3 = new CachedFile("file3", "", 16L, DateTime.now());
        CachedFile cachedFile4 = new CachedFile("file4", "", 17L, DateTime.now());
        cachedPage2.add(cachedFile3);
        cachedPage2.add(cachedFile4);

        doReturn(cachedPage1).doReturn(cachedPage2).doReturn(null).when(downloadingFiles).getContent();

        doReturn(downloadingFiles).when(cachedFileRepository).findDownloadingCachedFiles(eq(MAX_DOWNLOAD_ATTEMPTS),
                any());
        doNothing().when(downloadManager).pollJobManagerForDownloadJob(any());

        downloadManager.pollJobManagerForDownloadJobs();

        verify(downloadManager, times(4)).pollJobManagerForDownloadJob(any());
        verify(downloadManager).pollJobManagerForDownloadJob(eq(cachedFile1));
        verify(downloadManager).pollJobManagerForDownloadJob(eq(cachedFile2));
        verify(downloadManager).pollJobManagerForDownloadJob(eq(cachedFile3));
        verify(downloadManager).pollJobManagerForDownloadJob(eq(cachedFile4));
    }

    @Test
    public void testPollJobManagerNewFileStartsJob()
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
    public void testPollJobManagerNewFileStartFailsIncrementsDownloadRetries()
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
    public void testPollJobManagerNewFileThrottled()
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
    public void testPollJobManagerCurrentlyDownloadingFileDoesNothing()
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
    public void testPollJobManagerCompletedFileUpdatesCache()
    {
        CachedFile completedFile = new CachedFile();
        completedFile.setFileId("test.txt");
        completedFile.setPath("src/test/resources/testfile/test.txt");
        completedFile.setDownloadJobId("completed-job-id");
        completedFile.setDownloadJobRetryCount(2);

        doReturn(mockSuccess).when(jobManager).getJobStatus("completed-job-id");

        downloadManager.pollJobManagerForDownloadJob(completedFile);

        verify(jobManager, never()).startJob(any());

        // verify that the completed job is updated
        assertTrue(completedFile.isFileAvailableFlag());
        assertEquals(2, completedFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, times(1)).save(completedFile);
    }

    @Test
    public void testPollJobManagerFailedFileRestartsDownload()
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
    public void testPollJobManagerNoMoreAttemptsAllowedUpdatesCacheAndDoesntRestart()
    {

        CachedFile noMoreAttemptsFile = new CachedFile();
        noMoreAttemptsFile.setFileId("file-id-3a");
        noMoreAttemptsFile.setPath("dest/file-id-3a");
        noMoreAttemptsFile.setDownloadJobId("no-more-attempts-job-id");
        noMoreAttemptsFile.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS);

        doReturn(mockFailed).when(jobManager).getJobStatus("no-more-attempts-job-id");

        downloadManager.pollJobManagerForDownloadJob(noMoreAttemptsFile);

        verify(jobManager, never()).startJob(any());

        // verify that the no more attempts left is updated
        assertEquals(MAX_DOWNLOAD_ATTEMPTS + 1, noMoreAttemptsFile.getDownloadJobRetryCount());
        verify(cachedFileRepository, times(2)).save(noMoreAttemptsFile);

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
    public void testPollJobManagerJobCompleteButFileMissingRestartsDownload()
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
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "12.0", "-34.0", "5", "5" }));
    }

    @Test
    public void testBuildCutoutJobSinglePLane()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.5 6.0 6.0 D null null N 1");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "12.0", "-34.5", "6.0", "6.0" }));
    }

    @Test
    public void testBuildCutoutJob3D()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5 D 1:3");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(),
                is(new String[] { "cmd", "arg1", sourcePath, destPath, "-D3", "1:3", "12.0", "-34.0", "5", "5" }));
    }

    @Test
    public void testBuildCutoutJob4D()
    {
        String jobId = "bruce";
        CachedFile downloadingFile = new CachedFile();
        downloadingFile.setFileId("cutout-4-image-99");
        String destPath = "dest/cutout-4";
        downloadingFile.setPath(destPath);
        String sourcePath = "source/image-99";
        downloadingFile.setOriginalFilePath(sourcePath);
        downloadingFile.setDownloadJobId(jobId);
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds("12.0 -34.0 5 5 D 1:3 2");
        when(imageCutoutRepository.findOne(4L)).thenReturn(imageCutout);

        ProcessJob cutoutJob = downloadManager.buildCutoutJob(jobId, downloadingFile);
        assertThat(cutoutJob.getId(), is(jobId));
        assertThat(cutoutJob.getCommandAndArgs(), is(new String[] { "cmd", "arg1", sourcePath, destPath, "-D3", "1:3",
                "-D4", "2", "12.0", "-34.0", "5", "5" }));
    }
}
