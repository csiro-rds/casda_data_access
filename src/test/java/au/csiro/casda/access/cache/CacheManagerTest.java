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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyList;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.Page;

import au.csiro.casda.access.GeneratedFileDescriptor;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.EncapsulatedFileDescriptor;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.observation.EncapsulationFile;
import au.csiro.casda.entity.observation.Observation;

/**
 * Tests for the cache manager
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class CacheManagerTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Mock
    private CachedFileRepository cachedFileRepositoryMock;

    @Mock
    private DataAccessJobRepository dataAccessJobRepositoryMock;

    private CacheManager cacheManager;

    private final static String TEST_WORKING_DIR = "build/tempTest";

    private static final long MAX_CACHE_SIZE = 400l;

    private static final int MAX_DOWNLOAD_ATTEMPTS = 2;

    private File jobBaseFolder;
    private File dataBaseFolder;

    @Before
    public void setup() throws Exception
    {
        jobBaseFolder = new File(TEST_WORKING_DIR, "jobs");
        dataBaseFolder = new File(TEST_WORKING_DIR, "data");
        MockitoAnnotations.initMocks(this);
        cacheManager = spy(new CacheManager(MAX_CACHE_SIZE, MAX_DOWNLOAD_ATTEMPTS, TEST_WORKING_DIR,
                cachedFileRepositoryMock, dataAccessJobRepositoryMock));
    }

    @Test
    public void testAllFilesAvailableInCache()
    {
        DownloadFile file1 = new FileDescriptor("file_1_id", 100L, FileType.IMAGE_CUBE);
        CachedFile cachedFile1 = new CachedFile("file_1_id", "/path/to/file1", 100L, DateTime.now().plusDays(1));
        cachedFile1.setFileAvailableFlag(true);
        DownloadFile file2 = new FileDescriptor("file_2_id", 123L, FileType.MEASUREMENT_SET);
        CachedFile cachedFile2 = new CachedFile("file_2_id", "/path/to/file2", 123L, DateTime.now().plusDays(1));
        cachedFile2.setFileAvailableFlag(false);
        DownloadFile file3 = new FileDescriptor("file_3_id", 111L, FileType.CATALOGUE);

        when(cachedFileRepositoryMock.findByFileId("file_1_id")).thenReturn(cachedFile1);
        when(cachedFileRepositoryMock.findByFileId("file_2_id")).thenReturn(cachedFile2);
        when(cachedFileRepositoryMock.findByFileId("file_3_id")).thenReturn(null);

        assertTrue(cacheManager.allFilesAvailableInCache(new ArrayList<>()));
        assertTrue(cacheManager.allFilesAvailableInCache(Arrays.asList(file1)));
        assertFalse(cacheManager.allFilesAvailableInCache(Arrays.asList(file1, file2)));
        assertFalse(cacheManager.allFilesAvailableInCache(Arrays.asList(file3, file1)));
    }

    // Nothing to do, all files are in the cache
    @SuppressWarnings("unchecked")
    @Test
    public void testReserveSpaceAllPresent() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        CachedFile cachedFile1 = new CachedFile("file-id-1", TEST_WORKING_DIR + "/jobs/ABC/file-id-1", 12L,
                DateTime.now(DateTimeZone.UTC).plusHours(7));
        cachedFile1.setFileAvailableFlag(true);
        DownloadFile file2 = new FileDescriptor("file-id-2", 22, FileType.IMAGE_CUBE);
        CachedFile cachedFile2 = new CachedFile("file-id-2", TEST_WORKING_DIR + "/jobs/ABC/file-id-2", 22L,
                DateTime.now(DateTimeZone.UTC).plusHours(6));
        cachedFile2.setFileAvailableFlag(false);
        files.add(file1);
        files.add(file2);

        doReturn(cachedFile1).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(cachedFile2).when(cachedFileRepositoryMock).findByFileId("file-id-2");

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);

        verify(cachedFileRepositoryMock, times(1)).save(cachedFile1);
        verify(cachedFileRepositoryMock, times(1)).save(cachedFile2);
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile1.getUnlock().getMillis(),
                1000L);
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile2.getUnlock().getMillis(),
                1000L);
        verify(cacheManager, never()).getUsedCacheSizeKb();
        verify(cachedFileRepositoryMock, never()).sumUnlockedCachedFileSize(any());
        verify(cachedFileRepositoryMock, never()).delete(any(CachedFile.class));
        verify(cachedFileRepositoryMock, never()).save(anyList());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testReserveSpaceOneMissing() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        CachedFile cachedFile1 = new CachedFile("file-id-1", TEST_WORKING_DIR + "/jobs/ABC/file-id-1", 12L,
                DateTime.now(DateTimeZone.UTC).plusHours(7));
        cachedFile1.setFileAvailableFlag(true);
        DownloadFile file2 = new FileDescriptor("file-id-2", 22, FileType.IMAGE_CUBE);
        files.add(file1);
        files.add(file2);

        doReturn(cachedFile1).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-2");
        doReturn(Optional.of(12L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);

        verify(cachedFileRepositoryMock, times(1)).save(cachedFile1);
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile1.getUnlock().getMillis(),
                1000L);
        verify(cacheManager, times(1)).getUsedCacheSizeKb();
        verify(cachedFileRepositoryMock, times(1)).sumUnlockedCachedFileSize(any());
        verify(cachedFileRepositoryMock, never()).delete(any(CachedFile.class));
        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());
        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        assertEquals("file-id-2", newCachedFiles.get(0).getFileId());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceWithEmptyCacheBelowMax() throws CacheException
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size less than max
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        files.add(file1);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);

        verify(cacheManager, times(1)).getUsedCacheSizeKb();
        verify(cachedFileRepositoryMock, times(1)).sumUnlockedCachedFileSize(any());
        verify(cachedFileRepositoryMock, never()).delete(any(CachedFile.class));
        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());
        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        CachedFile cachedFile = newCachedFiles.get(0);
        assertEquals(null, cachedFile.getDataAccessJobs());
        assertEquals(null, cachedFile.getDownloadJobId());
        assertEquals(0, cachedFile.getDownloadJobRetryCount());
        assertEquals("file-id-1", cachedFile.getFileId());
        assertEquals(FileType.IMAGE_CUBE, cachedFile.getFileType());
        assertEquals(
                new File(((CacheManager) cacheManager).getCurrentDateDir(), cachedFile.getFileId()).getAbsolutePath(),
                cachedFile.getPath());
        assertEquals(12L, cachedFile.getSizeKb().longValue());
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile.getUnlock().getMillis(),
                1000L);
    }

    @Test
    public void testReserveSpaceWithEmptyCacheAboveMax() throws CacheException
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage(containsString("needed: 401 kb"));
        thrown.expectMessage(containsString("available: 400 kb"));

        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size greater than max
        DownloadFile file1 = new FileDescriptor("file-id-1", MAX_CACHE_SIZE + 1, FileType.IMAGE_CUBE);
        files.add(file1);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceWithCacheHitsBelowMax() throws CacheException
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size less than max
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        DownloadFile file2 = new FileDescriptor("file-id-2", 15, FileType.IMAGE_CUBE);
        files.add(file1);
        files.add(file2);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-2");
        doReturn(Optional.of(373L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);

        verify(cacheManager, times(1)).getUsedCacheSizeKb();
        verify(cachedFileRepositoryMock, times(1)).sumUnlockedCachedFileSize(any());
        verify(cachedFileRepositoryMock, never()).delete(any(CachedFile.class));
        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(2, newCachedFiles.size());

        CachedFile cachedFile = newCachedFiles.get(0);
        assertEquals(null, cachedFile.getDataAccessJobs());
        assertEquals(null, cachedFile.getDownloadJobId());
        assertEquals(0, cachedFile.getDownloadJobRetryCount());
        assertEquals("file-id-1", cachedFile.getFileId());
        assertEquals(FileType.IMAGE_CUBE, cachedFile.getFileType());
        assertEquals(new File(cacheManager.getCurrentDateDir(), cachedFile.getFileId()).getAbsolutePath(),
                cachedFile.getPath());
        assertEquals(12L, cachedFile.getSizeKb().longValue());
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile.getUnlock().getMillis(),
                1000L);

        CachedFile cachedFile2 = newCachedFiles.get(1);
        assertEquals(null, cachedFile2.getDataAccessJobs());
        assertEquals(null, cachedFile2.getDownloadJobId());
        assertEquals(0, cachedFile2.getDownloadJobRetryCount());
        assertEquals("file-id-2", cachedFile2.getFileId());
        assertEquals(FileType.IMAGE_CUBE, cachedFile2.getFileType());
        assertEquals(new File(cacheManager.getCurrentDateDir(), cachedFile2.getFileId()).getAbsolutePath(),
                cachedFile2.getPath());
        assertEquals(15L, cachedFile2.getSizeKb().longValue());
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile2.getUnlock().getMillis(),
                1000L);
    }

    @Test
    public void testReserveSpaceWithCacheHitsAboveMax() throws CacheException
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage(containsString("needed: 27 kb"));
        thrown.expectMessage(containsString("available: 26 kb"));

        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size less than max
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        DownloadFile file2 = new FileDescriptor("file-id-2", 15, FileType.IMAGE_CUBE);
        files.add(file1);
        files.add(file2);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-2");
        doReturn(Optional.of(374L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.empty()).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceWithCacheHitsRemoveOK() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size less than max
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        files.add(file1);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(Optional.of(MAX_CACHE_SIZE)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(12L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        List<CachedFile> cachedFilesCanRemove = new ArrayList<>();
        CachedFile cachedFileCanRemove =
                new CachedFile("remove-me", TEST_WORKING_DIR + "/1/remove-me", 12L, DateTime.now().minusMillis(100));
        cachedFilesCanRemove.add(cachedFileCanRemove);

        Page<CachedFile> cachedFilesToRemove = mock(Page.class);
        doReturn(1).doReturn(0).when(cachedFilesToRemove).getNumberOfElements();
        doReturn(cachedFilesCanRemove).when(cachedFilesToRemove).getContent();

        doReturn(cachedFilesToRemove).when(cachedFileRepositoryMock).findCachedFilesToUnlock(any(), any());

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);

        verify(cacheManager, times(1)).removeCachedFile(eq(TEST_WORKING_DIR + "/1/remove-me"));
        verify(cacheManager, times(1)).removeJobsUsingFile(eq(cachedFileCanRemove));

        verify(cachedFileRepositoryMock).delete(cachedFileCanRemove);
        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());

        CachedFile cachedFile = newCachedFiles.get(0);
        assertEquals(null, cachedFile.getDataAccessJobs());
        assertEquals(null, cachedFile.getDownloadJobId());
        assertEquals(0, cachedFile.getDownloadJobRetryCount());
        assertEquals("file-id-1", cachedFile.getFileId());
        assertEquals(FileType.IMAGE_CUBE, cachedFile.getFileType());
        assertEquals(new File(cacheManager.getCurrentDateDir(), cachedFile.getFileId()).getAbsolutePath(),
                cachedFile.getPath());
        assertEquals(12L, cachedFile.getSizeKb().longValue());
        assertEquals(DateTime.now(DateTimeZone.UTC).plusWeeks(1).getMillis(), cachedFile.getUnlock().getMillis(),
                1000L);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testReserveSpaceWithNoCacheHitsRemoveFail() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("Unable to remove file from cache: " + TEST_WORKING_DIR + "/1/remove-me");

        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        // download file size less than max
        DownloadFile file1 = new FileDescriptor("file-id-1", 12, FileType.IMAGE_CUBE);
        files.add(file1);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("file-id-1");
        doReturn(Optional.of(MAX_CACHE_SIZE)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(12L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        List<CachedFile> cachedFilesCanRemove = new ArrayList<>();
        CachedFile cachedFileCanRemove =
                new CachedFile("remove-me", TEST_WORKING_DIR + "/1/remove-me", 12L, DateTime.now().minusMillis(100));
        cachedFilesCanRemove.add(cachedFileCanRemove);

        Page<CachedFile> cachedFilesToRemove = mock(Page.class);
        doReturn(1).doReturn(0).when(cachedFilesToRemove).getNumberOfElements();
        doReturn(cachedFilesCanRemove).when(cachedFilesToRemove).getContent();

        doReturn(cachedFilesToRemove).when(cachedFileRepositoryMock).findCachedFilesToUnlock(any(), any());

        doThrow(new IOException("can't delete")).when(cacheManager).removeCachedFile(TEST_WORKING_DIR + "/1/remove-me");

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceCutoutForImageOnDisk() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        GeneratedFileDescriptor cutout = 
        		new GeneratedFileDescriptor(1L, "cutout-1-image-12", 11L, "image-12", 13L, FileType.IMAGE_CUTOUT);
        cutout.setOriginalImageFilePath("original-image-path");
        files.add(cutout);

        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        assertEquals("cutout-1-image-12", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(0).getSizeKb());
        assertEquals("original-image-path", newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        assertEquals(
                new File(new File(jobBaseFolder, "Hello"), "cutout-1-image-12").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.IMAGE_CUTOUT, newCachedFiles.get(0).getFileType());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceCutoutForImageInCache() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        GeneratedFileDescriptor cutout = 
        		new GeneratedFileDescriptor(1L, "cutout-1-image-12", 11L, "image-12", 13L, FileType.IMAGE_CUTOUT);
        files.add(cutout);

        CachedFile cachedFile = new CachedFile("image-12", "path-to-image-12", 13L, DateTime.now());
        doReturn(cachedFile).when(cachedFileRepositoryMock).findByFileId("image-12");
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<CachedFile> cachedFileCaptor = ArgumentCaptor.forClass(CachedFile.class);
        verify(cachedFileRepositoryMock, times(1)).save(cachedFileCaptor.capture());
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        assertEquals("cutout-1-image-12", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(0).getSizeKb());
        assertNull(newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        assertEquals(new File(new File(jobBaseFolder, "Hello"), "cutout-1-image-12").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.IMAGE_CUTOUT, newCachedFiles.get(0).getFileType());

        CachedFile imageFileInCache = cachedFileCaptor.getValue();
        assertTrue(imageFileInCache.getUnlock().isAfter(DateTime.now().plusDays(3)));
        assertEquals("image-12", imageFileInCache.getFileId());
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceGeneratedSpectrumOnDisk() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        GeneratedFileDescriptor cutout = new GeneratedFileDescriptor(
        		1L, "spectrum-1-image-12", 11L, "image-12", 13L, FileType.GENERATED_SPECTRUM);
        cutout.setOriginalImageFilePath("original-image-path");
        files.add(cutout);

        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        assertEquals("spectrum-1-image-12", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(0).getSizeKb());
        assertEquals("original-image-path", newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        assertEquals(
                new File(new File(jobBaseFolder, "Hello"), "spectrum-1-image-12").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.GENERATED_SPECTRUM, newCachedFiles.get(0).getFileType());
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceforGeneratedSpectrumInCache() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        GeneratedFileDescriptor spectrum = new GeneratedFileDescriptor(
        		1L, "spectrum-1-image-12", 11L, "image-12", 13L, FileType.GENERATED_SPECTRUM);
        files.add(spectrum);

        CachedFile cachedFile = new CachedFile("image-12", "path-to-image-12", 13L, DateTime.now());
        doReturn(cachedFile).when(cachedFileRepositoryMock).findByFileId("image-12");
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<CachedFile> cachedFileCaptor = ArgumentCaptor.forClass(CachedFile.class);
        verify(cachedFileRepositoryMock, times(1)).save(cachedFileCaptor.capture());
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(1, newCachedFiles.size());
        assertEquals("spectrum-1-image-12", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(0).getSizeKb());
        assertNull(newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        assertEquals(new File(new File(jobBaseFolder, "Hello"), "spectrum-1-image-12").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.GENERATED_SPECTRUM, newCachedFiles.get(0).getFileType());

        CachedFile imageFileInCache = cachedFileCaptor.getValue();
        assertTrue(imageFileInCache.getUnlock().isAfter(DateTime.now().plusDays(3)));
        assertEquals("image-12", imageFileInCache.getFileId());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceCutoutForImageNewForCache() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        GeneratedFileDescriptor cutout = 
        		new GeneratedFileDescriptor(1L, "cutout-1-image-12", 11L, "image-12", 13L, FileType.IMAGE_CUTOUT);
        GeneratedFileDescriptor cutout2 = 
        		new GeneratedFileDescriptor(2L, "cutout-2-image-12", 11L, "image-12", 13L, FileType.IMAGE_CUTOUT);
        files.add(cutout);
        files.add(cutout2);

        doReturn(null).when(cachedFileRepositoryMock).findByFileId("image-12");
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(3, newCachedFiles.size());
        assertEquals("cutout-1-image-12", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(0).getSizeKb());
        assertNull(newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        File jobFolder = new File(jobBaseFolder, "Hello");
        assertEquals(new File(jobFolder, "cutout-1-image-12").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.IMAGE_CUTOUT, newCachedFiles.get(0).getFileType());

        CachedFile imageFileInCache = newCachedFiles.get(1);
        assertTrue(imageFileInCache.getUnlock().isAfter(DateTime.now().plusDays(3)));
        assertEquals("image-12", imageFileInCache.getFileId());
        assertEquals(Long.valueOf(13), imageFileInCache.getSizeKb());
        
        assertEquals("cutout-2-image-12", newCachedFiles.get(2).getFileId());
        assertEquals(Long.valueOf(11), newCachedFiles.get(2).getSizeKb());
        assertNull(newCachedFiles.get(2).getOriginalFilePath());
        assertFalse(newCachedFiles.get(2).isFileAvailableFlag());
        assertEquals(new File(jobFolder, "cutout-2-image-12").getAbsolutePath(),
                newCachedFiles.get(2).getPath());
        assertEquals(FileType.IMAGE_CUTOUT, newCachedFiles.get(2).getFileType());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testReserveSpaceEncapsulationNewForCache() throws Exception
    {
        Collection<DownloadFile> files = new ArrayList<DownloadFile>();
        Observation obs = new Observation();
        obs.setSbid(5);
        EncapsulationFile encaps = new EncapsulationFile();
        encaps.setFilename("observation-5-encaps-spectrum-2.tar");
        encaps.setFormat("tar");
        encaps.setFilesize(30L);
        encaps.setId(20L);
        encaps.setParent(obs);
        EncapsulatedFileDescriptor efd1 = new EncapsulatedFileDescriptor("observation-5-spectrum-spect1-noise.fits", 5, 
                FileType.SPECTRUM, "spect1-noise.fits", encaps);
        files.add(efd1);

        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumCachedFileSize();
        doReturn(Optional.of(0L)).when(cachedFileRepositoryMock).sumUnlockedCachedFileSize(any());

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId("Hello");
        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, dataAccessJob);

        ArgumentCaptor<List> filesListCaptor = ArgumentCaptor.forClass(List.class);
        verify(cachedFileRepositoryMock, times(1)).save(filesListCaptor.capture());

        List<CachedFile> newCachedFiles = filesListCaptor.getValue();
        assertEquals(2, newCachedFiles.size());
        assertEquals("observation-5-spectrum-spect1-noise.fits", newCachedFiles.get(0).getFileId());
        assertEquals(Long.valueOf(5), newCachedFiles.get(0).getSizeKb());
        assertNull(newCachedFiles.get(0).getOriginalFilePath());
        assertFalse(newCachedFiles.get(0).isFileAvailableFlag());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        File destFolder = new File(dataBaseFolder, dateFormat.format(new Date()));
        assertEquals(new File(destFolder, "observation-5-spectrum-spect1-noise.fits").getAbsolutePath(),
                newCachedFiles.get(0).getPath());
        assertEquals(FileType.SPECTRUM, newCachedFiles.get(0).getFileType());

        CachedFile encapsFileInCache = newCachedFiles.get(1);
        assertTrue(encapsFileInCache.getUnlock().isAfter(DateTime.now().plusDays(3)));
        assertEquals("observations-5-encapsulation_files-20.tar", encapsFileInCache.getFileId());
        assertEquals(Long.valueOf(30), encapsFileInCache.getSizeKb());
        
        assertEquals(new File(destFolder, "observations-5-encapsulation_files-20.tar").getAbsolutePath(),
                efd1.getOriginalEncapsulationFilePath());
        // C:\Projects\casda\casda_data_access\build\tempTest\data\2016-12-12\observations-5-encapsulation_files-20.tar
    }

    @Test
    public void testGetCachedFile()
    {
        cacheManager.getCachedFile("FILE-55");
        verify(cachedFileRepositoryMock).findByFileId("FILE-55");
    }

    public void testLinkJob(DateTime cachedFileUnlock, DateTime newJobUnlock, DateTime assertValue)
            throws CacheException
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("bob-4");
        // param is detached entity
        CachedFile cachedFileParam = new CachedFile();
        cachedFileParam.setId(77L);
        // a fresh entity is requested.
        CachedFile cachedFile = new CachedFile();
        cachedFile.setUnlock(new DateTime(cachedFileUnlock, DateTimeZone.UTC));
        cachedFile.setDataAccessJobs(new ArrayList<DataAccessJob>());
        when(cachedFileRepositoryMock.findOne(77L)).thenReturn(cachedFile);

        cacheManager.linkJob(job, cachedFileParam, new File("src/test/resources/testfile/test.txt"));

        ArgumentCaptor<CachedFile> argCaptor = ArgumentCaptor.forClass(CachedFile.class);
        verify(cachedFileRepositoryMock).save(argCaptor.capture());
        CachedFile file = argCaptor.getValue();
        assertThat(file.getDataAccessJobs(), contains(job));
        assertThat(file.getUnlock(), is(new DateTime(assertValue, DateTimeZone.UTC)));

    }

    @Test
    public void testCreateSymLinkNoChecksum() throws CacheException
    {
        File expectedFile = new File(TEST_WORKING_DIR + "/jobs/bob-5/test.txt");
        expectedFile.delete();
        File checksumFile = new File(TEST_WORKING_DIR + "/jobs/bob-5/test.txt.checksum");
        checksumFile.delete();

        assertFalse(expectedFile.exists());
        assertFalse(checksumFile.exists());

        File testfile = new File("src/test/resources/testfile/test.txt");
        cacheManager.createSymLink("bob-5", testfile, false);

        assertTrue(expectedFile.exists());
        assertFalse(checksumFile.exists());
    }

    @Test
    public void testCreateSymLinkWithChecksum() throws CacheException
    {
        File expectedFile = new File(TEST_WORKING_DIR + "/jobs/bob-5/test.txt");
        expectedFile.delete();
        File checksumFile = new File(TEST_WORKING_DIR + "/jobs/bob-5/test.txt.checksum");
        checksumFile.delete();

        assertFalse(expectedFile.exists());
        assertFalse(checksumFile.exists());

        File testfile = new File("src/test/resources/testfile/test.txt");
        cacheManager.createSymLink("bob-5", testfile, true);

        assertTrue(expectedFile.exists());
        assertTrue(checksumFile.exists());
    }

    @Test
    public void testCreateSymLinkFails() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("Unable to create link from");

        File testfile = new File("src/test/resources/testfile/invalid.txt");
        assertFalse(testfile.exists());

        cacheManager.createSymLink("bob-6", testfile, false);
    }

    @Test
    public void testCreateSymLinkFailsIfTryToCreateTwice() throws Exception
    {

        File expectedFile = new File(TEST_WORKING_DIR + "/jobs/bob-7/test.txt");
        expectedFile.delete();

        assertFalse(expectedFile.exists());

        File testfile = new File("src/test/resources/testfile/test.txt");
        cacheManager.createSymLink("bob-7", testfile, false);

        thrown.expect(CacheException.class);
        thrown.expectMessage("Unable to create link from");

        cacheManager.createSymLink("bob-7", testfile, false);
    }

    @Test
    public void testUpdateUnlockForFilesAfter() throws CacheException
    {
        DateTime time = DateTime.now(DateTimeZone.UTC);
        CachedFile cachedFile = new CachedFile();
        cachedFile.setId(15L);
        cachedFile.setUnlock(time.minusMillis(100));
        cachedFile.setDataAccessJobs(new ArrayList<DataAccessJob>());
        Collection<DownloadFile> files =
                IntStream.range(1, 2).mapToObj(i -> new FileDescriptor(String.valueOf(i), 100l, FileType.IMAGE_CUBE))
                        .collect(Collectors.toList());

        when(cachedFileRepositoryMock.findByFileId("1")).thenReturn(cachedFile);
        when(dataAccessJobRepositoryMock.findLatestJobExpiryForCachedFile(15)).thenReturn(time.minusMillis(10));

        cacheManager.updateUnlockForFiles(files, time);

        ArgumentCaptor<CachedFile> argCaptor = ArgumentCaptor.forClass(CachedFile.class);
        verify(cachedFileRepositoryMock).save(argCaptor.capture());
        CachedFile file = argCaptor.getValue();
        assertThat(file.getUnlock(), is(new DateTime(time, DateTimeZone.UTC)));

    }

    @Test
    public void testUpdateUnlockForLatestJobExpiry() throws CacheException
    {
        DateTime time = DateTime.now(DateTimeZone.UTC);
        CachedFile cachedFile = new CachedFile();
        cachedFile.setId(15L);
        cachedFile.setUnlock(time.plusMillis(10000));
        cachedFile.setDataAccessJobs(new ArrayList<DataAccessJob>());
        Collection<DownloadFile> files =
                IntStream.range(1, 2).mapToObj(i -> new FileDescriptor(String.valueOf(i), 100l, FileType.IMAGE_CUBE))
                        .collect(Collectors.toList());

        when(cachedFileRepositoryMock.findByFileId("1")).thenReturn(cachedFile);
        when(dataAccessJobRepositoryMock.findLatestJobExpiryForCachedFile(15)).thenReturn(time.plusMillis(1000));

        cacheManager.updateUnlockForFiles(files, time);

        ArgumentCaptor<CachedFile> argCaptor = ArgumentCaptor.forClass(CachedFile.class);
        verify(cachedFileRepositoryMock).save(argCaptor.capture());
        CachedFile file = argCaptor.getValue();
        assertThat(file.getUnlock(), is(time.plusMillis(1000)));

    }

    @Test
    public void testGetCurrentDateDir() throws CacheException, IOException
    {
        // this should create the directory
        File dir = cacheManager.getCurrentDateDir();
        assertThat(dir.isDirectory(), is(true));
        // this should reuse the same directory
        File dir2 = cacheManager.getCurrentDateDir();
        assertThat(dir.getAbsolutePath(), is(dir2.getAbsolutePath()));
        dir2.delete();
        dir.createNewFile();
        dir.deleteOnExit();
        thrown.expect(CacheException.class);

        cacheManager.getCurrentDateDir();
    }

    @Test
    public void testIsCachedFileAvailableMaxDownloadAttemptsExceededThrowsCacheException() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("Maximum number of download attempts failed for file: file-id");

        DownloadFile file = new FileDescriptor("file-id", 14, FileType.IMAGE_CUBE);
        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId("file-id");
        cachedFile.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS + 1);

        doReturn(cachedFile).when(cacheManager).getCachedFile("file-id");

        cacheManager.isCachedFileAvailable(file);
    }

    @Test
    public void testIsCachedFileAvailable() throws Exception
    {
        DownloadFile file = new FileDescriptor("file-id", 14, FileType.IMAGE_CUBE);
        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId("file-id");
        cachedFile.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS);

        doReturn(cachedFile).when(cacheManager).getCachedFile("file-id");

        cachedFile.setFileAvailableFlag(true);
        assertTrue(cacheManager.isCachedFileAvailable(file));

        cachedFile.setFileAvailableFlag(false);
        assertFalse(cacheManager.isCachedFileAvailable(file));
    }

    @Test
    public void testIsCachedFileAvailableRecordMissingThrowsCacheException() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("Space hasn't been reserved for file file-id");

        DownloadFile file = new FileDescriptor("file-id", 14, FileType.IMAGE_CUBE);
        doReturn(null).when(cacheManager).getCachedFile("file-id");

        cacheManager.isCachedFileAvailable(file);
    }

    @Test
    public void testCreateDataAccessJobDirectoryRecordMissingThrowsCacheException() throws Exception
    {
        thrown.expect(CacheException.class);
        thrown.expectMessage("File was not saved to the cache: file-id");

        DataAccessJob job = new DataAccessJob();
        DownloadFile file = new FileDescriptor("file-id", 14, FileType.IMAGE_CUBE);
        doReturn(null).when(cacheManager).getCachedFile("file-id");
        List<DownloadFile> files = new ArrayList<>();
        files.add(file);

        cacheManager.createDataAccessJobDirectory(job, files);
    }

    @Test
    public void testCreateDataAccessJobDirectoryWorkflow() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("ABC-123-Y");

        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId("file-id");
        cachedFile.setPath(tempFolder + "/1/file-id-1");
        cachedFile.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS);
        cachedFile.setFileAvailableFlag(true);

        CachedFile cachedFile2 = new CachedFile();
        cachedFile2.setFileId("file-id2");
        cachedFile2.setPath(tempFolder + "/1/file-id-2");
        cachedFile2.setDownloadJobRetryCount(MAX_DOWNLOAD_ATTEMPTS);
        cachedFile2.setFileAvailableFlag(true);

        DownloadFile file = new FileDescriptor("file-id", 14, FileType.IMAGE_CUBE);
        DownloadFile file2 = new FileDescriptor("file-id2", 17, FileType.CATALOGUE);
        doReturn(cachedFile).when(cacheManager).getCachedFile("file-id");
        doReturn(cachedFile2).when(cacheManager).getCachedFile("file-id2");

        doNothing().when(cacheManager).linkJob(any(), any(), any());
        doNothing().when(cacheManager).createSymLink(anyString(), any(), anyBoolean());

        List<DownloadFile> files = new ArrayList<>();
        files.add(file);
        files.add(file2);

        cacheManager.createDataAccessJobDirectory(job, files);

        verify(cacheManager, times(1)).getCachedFile(eq("file-id"));
        verify(cacheManager, times(1)).getCachedFile(eq("file-id2"));
        verify(cacheManager, times(1)).linkJob(eq(job), eq(cachedFile), eq(new File(cachedFile.getPath())));
        verify(cacheManager, times(1)).createSymLink(eq("ABC-123-Y"), eq(new File(cachedFile.getPath())), eq(true));
        verify(cacheManager, times(1)).linkJob(eq(job), eq(cachedFile2), eq(new File(cachedFile2.getPath())));
        verify(cacheManager, never()).createSymLink(eq("ABC-123-Y"), eq(new File(cachedFile2.getPath())), anyBoolean());
    }

    @Test
    public void testRemoveCachedFileAndRemoveFolderIfEmpty() throws Exception
    {
        String testFilename = "removeCachedFileTestA.txt";

        String jobFolder = TEST_WORKING_DIR + "/testJob";
        new File(jobFolder).mkdirs();

        Path testFile = Paths.get(jobFolder, testFilename);
        Path testFileChecksum = Paths.get(jobFolder, testFilename);

        FileUtils.touch(testFile.toFile());
        FileUtils.touch(testFileChecksum.toFile());

        assertTrue(Files.exists(testFile));
        assertTrue(Files.exists(testFileChecksum));

        cacheManager.removeCachedFile(jobFolder + "/" + testFilename);

        assertFalse(Files.exists(testFile));
        assertFalse(Files.exists(testFileChecksum));

        // it should not throw an exception if neither exist
        cacheManager.removeCachedFile(jobFolder + "/" + testFilename);

        assertFalse(Files.exists(testFile));
        assertFalse(Files.exists(testFileChecksum));
        assertFalse(Files.exists(Paths.get(jobFolder)));
    }

    @Test
    public void testRemoveCachedFileShouldntRemoveFolderIfNotEmpty() throws Exception
    {
        String testFilename = "removeCachedFileTestA.txt";

        String jobFolder = TEST_WORKING_DIR + "/testJob2";
        new File(jobFolder).mkdirs();

        Path testFile = Paths.get(jobFolder, testFilename);
        Path testFileChecksum = Paths.get(jobFolder, testFilename);
        Path testFileExtra = Paths.get(jobFolder, testFilename + "2");

        FileUtils.touch(testFile.toFile());
        FileUtils.touch(testFileChecksum.toFile());
        FileUtils.touch(testFileExtra.toFile());

        assertTrue(Files.exists(testFile));
        assertTrue(Files.exists(testFileChecksum));
        assertTrue(Files.exists(testFileExtra));

        cacheManager.removeCachedFile(jobFolder + "/" + testFilename);

        assertFalse(Files.exists(testFile));
        assertFalse(Files.exists(testFileChecksum));
        assertTrue(Files.exists(testFileExtra));
        assertTrue(Files.exists(Paths.get(jobFolder)));
    }
    
    @Test
    public void testDeleteAllCache() throws Exception
    {          
        String testFilename = "deleteCachedFileTestA.txt";

        jobBaseFolder.mkdirs();
        dataBaseFolder.mkdirs();

        Path testFile = Paths.get(jobBaseFolder.getPath(), testFilename);
        Path testFileChecksum = Paths.get(jobBaseFolder.getPath(), testFilename);
        
        Path testDataFile = Paths.get(dataBaseFolder.getPath(), testFilename);
        Path testDataFileChecksum = Paths.get(dataBaseFolder.getPath(), testFilename);

        FileUtils.touch(testFile.toFile());
        FileUtils.touch(testFileChecksum.toFile());
        
        FileUtils.touch(testDataFile.toFile());
        FileUtils.touch(testDataFileChecksum.toFile());

        assertTrue(Files.exists(testFile));
        assertTrue(Files.exists(testFileChecksum));
        
        assertTrue(Files.exists(testDataFile));
        assertTrue(Files.exists(testDataFileChecksum));

        cacheManager.deleteAllCache();

        assertFalse(Files.exists(testFile));
        assertFalse(Files.exists(testFileChecksum));
        
        assertFalse(Files.exists(testDataFile));
        assertFalse(Files.exists(testDataFileChecksum));


        assertFalse(Files.exists(testFile));
        assertFalse(Files.exists(testFileChecksum));
        assertTrue(Files.exists(Paths.get(jobBaseFolder.getPath())));
        assertTrue(Files.exists(Paths.get(dataBaseFolder.getPath())));
    }

}
