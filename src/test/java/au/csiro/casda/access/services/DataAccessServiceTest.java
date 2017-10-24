package au.csiro.casda.access.services;

import static au.csiro.casda.access.uws.AccessJobManagerTest.createImageCube;
import static au.csiro.casda.access.uws.AccessJobManagerTest.createMeasurementSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

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

import au.csiro.TestUtils;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.GeneratedFileDescriptor;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.cache.CacheManager;
import au.csiro.casda.access.cache.DownloadManager;
import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CatalogueRepository;
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
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.services.NgasService.Status;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.GeneratedSpectrum;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.EncapsulationFile;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Thumbnail;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;

/**
 * Tests for the data access service.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
public class DataAccessServiceTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder cacheDir = new TemporaryFolder();

    @Mock
    private DataAccessJobRepository dataAccessJobRepository;

    @Mock
    private ImageCubeRepository imageCubeRepository;

    @Mock
    private CatalogueRepository catalogueRepository;

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
    private CachedFileRepository cachedFileRepository;

    @Mock
    private ImageCutoutRepository imageCutoutRepository;

    @Mock
    private GeneratedSpectrumRepository generatedSpectrumRepository;

    @Mock
    private DataAccessJdbcRepository dataAccessJdbcRepository;

    @Mock
    private AccessJobManager accessJobManager;

    @Mock
    private NgasService ngasService;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private CasdaMailService casdaMailService;

    @Mock
    private DownloadManager downloadManager;

    private DataAccessService dataAccessService;

    /**
     * Set up the service before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        String archiveStatusCommandAndArgs = TestUtils.getCommandAndArgsElStringForEchoOutput("DUL");
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, thumbnailRepository, cachedFileRepository,
                ngasService, cacheDir.getRoot().getAbsolutePath(), 25, 1000, archiveStatusCommandAndArgs, "", "",
                new JavaProcessJobFactory(), cacheManager, dataAccessJdbcRepository, imageCutoutRepository,
                generatedSpectrumRepository, casdaMailService, downloadManager);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasStatus.getMountPoint()).thenReturn("mountpoint");
        when(ngasStatus.getFileName()).thenReturn("filename.jpg");

        when(ngasService.getStatus(any())).thenReturn(ngasStatus);
    }

    @Test
    public void testGetExistingJob()
    {
        String id = "test_ID";
        DataAccessJob job = new DataAccessJob();
        job.setRequestId(id);
        when(dataAccessJobRepository.findByRequestId(id)).thenReturn(job);
        DataAccessJob jobFound = dataAccessService.getExistingJob(id);
        assertThat(job, is(jobFound));
        assertThat(job.getRequestId(), is(id));
    }

    @Test(expected = IOException.class)
    public void findFileDoesntExist() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("123-abc");

        ImageCube imageCube = new ImageCube();
        imageCube.setId(12L);
        imageCube.setFilename("testfile/doesntExist");

        job.addImageCube(imageCube);
        dataAccessService.findFile(job, "doesntExist");
    }

    @Test(expected = IOException.class)
    public void findFileCantRead() throws Exception
    {
        String requestId = "123-abc";
        String filename = "doesntExist";

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        ImageCube imageCube = new ImageCube();
        imageCube.setId(12L);
        imageCube.setFilename(filename);
        job.addImageCube(imageCube);

        File file = createDataFile(requestId, filename, "something");
        TestUtils.makeFileUnreadable(file);

        dataAccessService.findFile(job, "doesntExist");
    }

    @Test
    public void findFileOk() throws Exception
    {
        String requestId = "123-abc";
        String filename = "test.txt";

        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);
        ImageCube imageCube = new ImageCube();
        imageCube.setId(12L);
        imageCube.setFilename(filename);
        job.addImageCube(imageCube);

        createDataFile(requestId, filename, "Weeee!");

        File response = dataAccessService.findFile(job, filename);
        assertEquals(filename, response.getName());
        Path expectedPath = Paths.get(cacheDir.getRoot().getPath(), "jobs", requestId, filename);
        assertThat(response.getPath().toString(), is(equalTo(expectedPath.toString())));
    }

    @Test
    public void testGetFileLocation()
    {
        Path expectedPath = Paths.get(cacheDir.getRoot().getPath(), "jobs", "123-abc", "test.txt");
        assertThat(dataAccessService.getFileLocation("123-abc", "test.txt").getPath().toString(),
                is(equalTo(expectedPath.toString())));
    }

    @Test
    public void testGetJobDirectory()
    {
        Path expectedPath = Paths.get(cacheDir.getRoot().getPath(), "jobs", "123-abc");
        assertThat(dataAccessService.getJobDirectory("123-abc").getPath().toString(),
                is(equalTo(expectedPath.toString())));
    }

    @Test
    public void markRequestCompleted()
    {
        String requestId = "123-abc";
        DateTime time = DateTime.now(DateTimeZone.UTC);
        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);

        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

        dataAccessService.markRequestCompleted(requestId, time);
        ArgumentCaptor<DataAccessJob> argCaptor = ArgumentCaptor.forClass(DataAccessJob.class);
        verify(dataAccessJobRepository).save(argCaptor.capture());
        DataAccessJob jobSaved = argCaptor.getValue();
        assertThat(jobSaved.getExpiredTimestamp(), is(time));
        assertThat(jobSaved.getStatus(), is(DataAccessJobStatus.READY));
        assertThat(jobSaved.getRequestId(), is(requestId));

        verify(casdaMailService, times(1)).sendEmail(any(DataAccessJob.class), eq(CasdaMailService.READY_EMAIL),
                eq(CasdaMailService.READY_EMAIL_SUBJECT));

    }

    @Test
    public void testMarkRequestError()
    {
        String requestId = "123-abc2";
        DataAccessJob job = new DataAccessJob();
        job.setRequestId(requestId);

        when(dataAccessJobRepository.findByRequestId(requestId)).thenReturn(job);

        dataAccessService.markRequestError(requestId, new DateTime(DateTimeZone.UTC));
        ArgumentCaptor<DataAccessJob> argCaptor = ArgumentCaptor.forClass(DataAccessJob.class);
        verify(dataAccessJobRepository).save(argCaptor.capture());
        DataAccessJob jobSaved = argCaptor.getValue();
        assertThat(jobSaved.getStatus(), is(DataAccessJobStatus.ERROR));
        assertThat(jobSaved.getRequestId(), is(requestId));
        verify(casdaMailService, times(1)).sendEmail(any(DataAccessJob.class), eq(CasdaMailService.FAILED_EMAIL),
                eq(CasdaMailService.FAILED_EMAIL_SUBJECT));
    }

    @Test
    public void testFindFileInNgasGetNgasStatusNoMountPoint() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 12L, "ABC111", 111111);

        exception.expect(ResourceNotFoundException.class);
        exception.expectMessage(measurementSet.getFileId() + " does not exist in NGAS");

        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(measurementSet.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("");
        when(ngasStatus.getFileName()).thenReturn(measurementSet.getFileId());

        dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
    }

    @Test
    public void testFindFileInNgasGetNgasStatusNoFilename() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 12L, "ABC111", 111111);

        exception.expect(ResourceNotFoundException.class);
        exception.expectMessage(measurementSet.getFileId() + " does not exist in NGAS");

        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(measurementSet.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn("");

        dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
    }

    @Test
    public void testFindFileInNgasGetFileStatusThrowsException() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 12L, "ABC111", 111111);

        exception.expect(ServiceCallException.class);
        exception.expectMessage("something");

        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(null);

        doThrow(new ServiceCallException("something")).when(ngasService).getStatus(measurementSet.getFileId());

        dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
    }

    @Test
    public void testFindFileInNgasGetFileStatusFails() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 12L, "ABC111", 111111);

        exception.expect(ServiceCallException.class);
        exception.expectMessage("Request to get status failed from NGAS for file id " + measurementSet.getFileId());

        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(false);
        when(ngasService.getStatus(measurementSet.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(measurementSet.getFileId());

        dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
    }

    @Test
    public void testFindFileInNgas() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 12L, "ABC111", 111111);
        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(measurementSet.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(measurementSet.getFileId());

        Path measurementSetPath = dataAccessService
                .findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
        assertEquals(Paths.get("/ngas/mount/point/" + measurementSet.getFileId()), measurementSetPath);
    }

    @Test
    public void testFindFileInNgasWithFileId() throws Exception
    {
        String fileId = "file-id";
        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(fileId)).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(fileId);

        Path ngasFilePath = dataAccessService.findFileInNgasIfOnDisk("file-id");
        assertEquals(Paths.get("/ngas/mount/point/" + fileId), ngasFilePath);
    }

    @Test
    public void testFindFileInNgasNotOnDisk() throws Exception
    {
        String archiveStatusCommandAndArgs = TestUtils.getCommandAndArgsElStringForEchoOutput("OFL");
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, thumbnailRepository, cachedFileRepository,
                ngasService, cacheDir.getRoot().getAbsolutePath(), 25, 1000, archiveStatusCommandAndArgs, "", "",
                new JavaProcessJobFactory(), mock(CacheManager.class), dataAccessJdbcRepository, imageCutoutRepository,
                generatedSpectrumRepository, casdaMailService, downloadManager);

        ImageCube imageCube = createImageCube(125L, "image_cube-125.fits", 12L, "ABC123", 123123);
        when(imageCubeRepository.findOne(125L)).thenReturn(imageCube);
        when(cachedFileRepository.findByFileId(imageCube.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(imageCube.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(imageCube.getFileId());

        Path imageCubePath = dataAccessService
                .findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.cube, 125L));
        assertNull(imageCubePath);
    }

    @Test
    public void testFindFileInNgasWithFileIdNotOnDisk() throws Exception
    {
        String archiveStatusCommandAndArgs = TestUtils.getCommandAndArgsElStringForEchoOutput("OFL");
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, spectrumRepository, momentMapRepository, cubeletRepository,
                encapsulationFileRepository, evaluationFileRepository, thumbnailRepository, cachedFileRepository,
                ngasService, cacheDir.getRoot().getAbsolutePath(), 25, 1000, archiveStatusCommandAndArgs, "", "",
                new JavaProcessJobFactory(), mock(CacheManager.class), dataAccessJdbcRepository, imageCutoutRepository,
                generatedSpectrumRepository, casdaMailService, downloadManager);

        String fileId = "file-id";

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(fileId)).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(fileId);

        Path imageCubePath = dataAccessService.findFileInNgasIfOnDisk("file-id");
        assertNull(imageCubePath);
    }

    @Test
    public void testFindFileInNgasInvalidDataProduct() throws Exception
    {
        exception.expect(ResourceNotFoundException.class);
        exception.expectMessage("cube with id 123 does not exist");

        when(imageCubeRepository.findOne(123L)).thenReturn(null);

        dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.cube, 123L));
    }

    @Test
    public void testUpdateSizeForCutout() throws Exception
    {
        ImageCutout cutout = new ImageCutout();
        when(imageCutoutRepository.findOne(any(Long.class))).thenReturn(cutout);
        List<DownloadFile> files = new ArrayList<>();
        files.add(
                new GeneratedFileDescriptor(15L, "fileid3", 102L, "originalImageFileId", 1000L, FileType.IMAGE_CUTOUT));

        dataAccessService.updateFileSizeForGeneratedFiles(files);

        assertEquals(102L, files.get(0).getSizeKb());
        verify(imageCutoutRepository).save(eq(cutout));
    }

    @Test
    public void testUpdateSizeForGeneratedSpectrum() throws Exception
    {
        GeneratedSpectrum spec = new GeneratedSpectrum();
        when(generatedSpectrumRepository.findOne(any(Long.class))).thenReturn(spec);
        List<DownloadFile> files = new ArrayList<>();
        files.add(new GeneratedFileDescriptor(15L, "fileid3", 102L, "originalImageFileId", 1000L,
                FileType.GENERATED_SPECTRUM));
        dataAccessService.updateFileSizeForGeneratedFiles(files);

        assertEquals(102L, files.get(0).getSizeKb());
        verify(generatedSpectrumRepository).save(eq(spec));
    }

    @Test
    public void testDownloadThumbnailFromNgas() throws Exception
    {

        // this service will be polled by dap repeatedly until the file is available. so this test will step through
        // each phase

        File test = createDataFile("id", "myFile", "hello this is contents");
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(mock(ServletOutputStream.class));

        // original unencapsulated thumbnail
        Thumbnail thumbOrig = mock(Thumbnail.class);
        when(thumbOrig.getEncapsulationFile()).thenReturn(null);

        // cachedfile
        CachedFile cacheFile = mock(CachedFile.class);
        when(cacheFile.getPath()).thenReturn(test.getPath());
        when(cacheFile.isFileAvailableFlag()).thenReturn(false, true);

        when(thumbnailRepository.findThumbnail(any(), any())).thenReturn(null, thumbOrig);

        // run with an null thumbnail
        dataAccessService.downloadThumbnailFromNgas("observations-111-thumbnail-id", response);
        verify(ngasService, times(1)).retrieveFile(eq("observations-111-thumbnail-id"), any());

        // run with an unencapsulated thumbnail
        dataAccessService.downloadThumbnailFromNgas("observations-111-thumbnail-id", response);
        verify(ngasService, times(2)).retrieveFile(eq("observations-111-thumbnail-id"), any());

    }

    @Test
    public void testDownloadThumbnailFromNgasEncapsulated() throws Exception
    {

        // this service will be polled by dap repeatedly until the file is available. so this test will step through
        // each phase

        File test = createDataFile("id", "myFile", "hello this is contents");
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(mock(ServletOutputStream.class));

        // original unencapsulated thumbnail
        Thumbnail thumbOrig = mock(Thumbnail.class);
        when(thumbOrig.getEncapsulationFile()).thenReturn(null);

        // encapsulated thumbnail
        Thumbnail thumbnail = mock(Thumbnail.class);
        EncapsulationFile encap = mock(EncapsulationFile.class);
        when(thumbnail.getEncapsulationFile()).thenReturn(encap);
        when(encap.getFileId()).thenReturn("observations-112-encaps-1");
        when(thumbnail.getFileId()).thenReturn("observations-112-thumbnail-id");

        // cachedfile
        CachedFile cacheFile = mock(CachedFile.class);
        when(cacheFile.getPath()).thenReturn(test.getPath());
        when(cacheFile.isFileAvailableFlag()).thenReturn(false, false, false, false, true);

        CachedFile encapsCacheFile = mock(CachedFile.class);
        when(encapsCacheFile.getPath()).thenReturn(test.getPath());
        when(encapsCacheFile.isFileAvailableFlag()).thenReturn(false, false, false, false, true);

        when(thumbnailRepository.findThumbnail(any(), any())).thenReturn(thumbnail);

        // this is triggered twice
        when(cacheManager.getCachedFile("observations-112-thumbnail-id")).thenReturn(null, null, null, cacheFile);
        when(cacheManager.getCachedFile("observations-112-encaps-1")).thenReturn(null, encapsCacheFile);

        // run with an encapsulated thumbnail
        dataAccessService.downloadThumbnailFromNgas("observations-112-thumbnail-id", response);
        // on first run this file's encapsulation is added to the cache table so it will be downloaded to cache
        verify(cachedFileRepository, times(1)).save(any(CachedFile.class));
        verify(downloadManager, times(1)).pollJobManagerForDownloadJob(any(CachedFile.class));
        verify(response).sendError(204);

        // on the second run through the cache table entry exists, but file is not available so nothing happens
        dataAccessService.downloadThumbnailFromNgas("observations-112-thumbnail-id", response);
        verify(cachedFileRepository, times(1)).save(any(CachedFile.class));
        verify(response, times(2)).sendError(204);

        // on the third run the thumbnail file is added to the cache table so it will be downloaded to cache
        dataAccessService.downloadThumbnailFromNgas("observations-112-thumbnail-id", response);
        verify(cachedFileRepository, times(2)).save(any(CachedFile.class));
        verify(response, times(3)).sendError(204);

        // on the fourth run through the cache table entry exists, but file is not available so nothing happens
        dataAccessService.downloadThumbnailFromNgas("observations-112-thumbnail-id", response);
        verify(response, times(4)).sendError(204);
        verify(cacheFile, times(3)).isFileAvailableFlag();
        // so we test the counts on the other paths have not triggered
        verify(cachedFileRepository, times(2)).save(any(CachedFile.class));

        // on the fourth run the file is available to be downloaded
        dataAccessService.downloadThumbnailFromNgas("observations-111-thumbnail-id", response);
        verify(cacheFile, times(6)).isFileAvailableFlag();
        verify(cachedFileRepository, times(2)).save(any(CachedFile.class));
        verify(response, times(1)).flushBuffer();
        // Make sure we haven't had any extra 204s sent
        verify(response, times(4)).sendError(204);
    }

    @Test
    public void testDownloadNgasFileNaming() throws Exception
    {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(mock(ServletOutputStream.class));

        dataAccessService.downloadThumbnailFromNgas("observations-111-thumbnail-mom0_1.png", response);
        verify(thumbnailRepository, times(1)).findThumbnail("mom0_1.png", 111);

        dataAccessService.downloadThumbnailFromNgas("observations-121-thumbnail-C007c-mom1_1.png", response);
        verify(thumbnailRepository, times(1)).findThumbnail("C007c-mom1_1.png", 121);

        dataAccessService.downloadThumbnailFromNgas("observations-121-thumbnail-42.png", response);
        verify(thumbnailRepository, times(1)).findOne(42L);

        dataAccessService.downloadThumbnailFromNgas("level7-1741-thumbnail-spec1.png", response);
        verify(thumbnailRepository, times(1)).findLevel7Thumbnail("spec1.png", 1741);

        dataAccessService.downloadThumbnailFromNgas("level7-1741-thumbnail-358.png", response);
        verify(thumbnailRepository, times(1)).findOne(358L);
    }

    @Test
    public void testGetPagingForJobView()
    {
        Map<String, Object> fileCounts = new LinkedHashMap<String, Object>();
        fileCounts.put("IMAGE_CUBE", 76L);
        fileCounts.put("MEASUREMENT_SET", 106L);
        fileCounts.put("SPECTRUM", 7L);
        fileCounts.put("MOMENT_MAP", 2L);
        fileCounts.put("CUBELET", 2L);
        fileCounts.put("CATALOGUE", 29L);
        fileCounts.put("IMAGE_CUTOUT", 1L);
        fileCounts.put("GENERATED_SPECTRUM", 2L);
        fileCounts.put("ENCAPSULATION_FILE", 1L);
        fileCounts.put("EVALUATION_FILE", 1L);
        fileCounts.put("ERROR", 1L);

        when(dataAccessJdbcRepository.countFilesForJob(any())).thenReturn(fileCounts);

        List<Map<FileType, Integer[]>> pagingDetails = dataAccessService.getPaging("12345", true);

        assertEquals(10, pagingDetails.size());
        // test for pages of 25 items each
        // test page 1
        assertEquals(1, pagingDetails.get(0).get(FileType.IMAGE_CUBE)[0].intValue());
        assertEquals(25, pagingDetails.get(0).get(FileType.IMAGE_CUBE)[1].intValue());

        // test page 2
        assertEquals(26, pagingDetails.get(1).get(FileType.IMAGE_CUBE)[0].intValue());
        assertEquals(50, pagingDetails.get(1).get(FileType.IMAGE_CUBE)[1].intValue());

        // test page 3
        assertEquals(51, pagingDetails.get(2).get(FileType.IMAGE_CUBE)[0].intValue());
        assertEquals(75, pagingDetails.get(2).get(FileType.IMAGE_CUBE)[1].intValue());

        // test page 4
        assertEquals(76, pagingDetails.get(3).get(FileType.IMAGE_CUBE)[0].intValue());
        assertEquals(76, pagingDetails.get(3).get(FileType.IMAGE_CUBE)[0].intValue());
        assertEquals(1, pagingDetails.get(3).get(FileType.MEASUREMENT_SET)[0].intValue());
        assertEquals(24, pagingDetails.get(3).get(FileType.MEASUREMENT_SET)[1].intValue());

        // test page 5
        assertEquals(25, pagingDetails.get(4).get(FileType.MEASUREMENT_SET)[0].intValue());
        assertEquals(49, pagingDetails.get(4).get(FileType.MEASUREMENT_SET)[1].intValue());

        // test page 6
        assertEquals(50, pagingDetails.get(5).get(FileType.MEASUREMENT_SET)[0].intValue());
        assertEquals(74, pagingDetails.get(5).get(FileType.MEASUREMENT_SET)[1].intValue());

        // test page 7
        assertEquals(75, pagingDetails.get(6).get(FileType.MEASUREMENT_SET)[0].intValue());
        assertEquals(99, pagingDetails.get(6).get(FileType.MEASUREMENT_SET)[1].intValue());

        // test page 8
        assertEquals(100, pagingDetails.get(7).get(FileType.MEASUREMENT_SET)[0].intValue());
        assertEquals(106, pagingDetails.get(7).get(FileType.MEASUREMENT_SET)[1].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.SPECTRUM)[0].intValue());
        assertEquals(7, pagingDetails.get(7).get(FileType.SPECTRUM)[1].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.MOMENT_MAP)[0].intValue());
        assertEquals(2, pagingDetails.get(7).get(FileType.MOMENT_MAP)[1].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.CUBELET)[0].intValue());
        assertEquals(2, pagingDetails.get(7).get(FileType.CUBELET)[1].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.CATALOGUE)[0].intValue());
        assertEquals(6, pagingDetails.get(7).get(FileType.CATALOGUE)[1].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.ENCAPSULATION_FILE)[0].intValue());
        assertEquals(1, pagingDetails.get(7).get(FileType.ENCAPSULATION_FILE)[1].intValue());

        // test page 9
        assertEquals(7, pagingDetails.get(8).get(FileType.CATALOGUE)[0].intValue());
        assertEquals(29, pagingDetails.get(8).get(FileType.CATALOGUE)[1].intValue());
        assertEquals(1, pagingDetails.get(8).get(FileType.IMAGE_CUTOUT)[0].intValue());
        assertEquals(1, pagingDetails.get(8).get(FileType.IMAGE_CUTOUT)[1].intValue());
        assertEquals(1, pagingDetails.get(8).get(FileType.GENERATED_SPECTRUM)[0].intValue());
        assertEquals(1, pagingDetails.get(8).get(FileType.GENERATED_SPECTRUM)[1].intValue());

        // test page 10
        assertEquals(1, pagingDetails.get(9).get(FileType.ERROR)[0].intValue());
        assertEquals(1, pagingDetails.get(9).get(FileType.ERROR)[1].intValue());
        assertEquals(2, pagingDetails.get(9).get(FileType.GENERATED_SPECTRUM)[0].intValue());
        assertEquals(2, pagingDetails.get(9).get(FileType.GENERATED_SPECTRUM)[1].intValue());
        
        assertEquals(1, pagingDetails.get(9).get(FileType.EVALUATION_FILE)[0].intValue());
        assertEquals(1, pagingDetails.get(9).get(FileType.EVALUATION_FILE)[1].intValue());

    }

    @Test
    public void testFileCount()
    {
        Map<String, Object> fileCounts = new LinkedHashMap<String, Object>();
        fileCounts.put("IMAGE_CUBE", 76L);
        fileCounts.put("MEASUREMENT_SET", 106L);
        fileCounts.put("SPECTRUM", 7L);
        fileCounts.put("MOMENT_MAP", 2L);
        fileCounts.put("CATALOGUE", 29L);
        fileCounts.put("IMAGE_CUTOUT", 3L);
        fileCounts.put("ERROR", 1L);// should not be counted

        when(dataAccessJdbcRepository.countFilesForJob(any(String.class))).thenReturn(fileCounts);

        assertEquals(223L, dataAccessService.getFileCount("abc-123"));
    }

    @Test
    public void testDownloadCutoutPreviewNotExists()
    {
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double len = 0.2;
        String bounds = String.format("%f %f %.6f %.6f", ra, dec, len, len); 
        HttpServletResponse response = mock(HttpServletResponse.class);
        boolean result = dataAccessService.downloadCutoutPreview(imageId, ra, dec, 0.1, response);
        assertThat(result, is(false));
        verify(imageCutoutRepository).findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png");
    }

    @Test
    public void testDownloadCutoutPreviewAvailable() throws IOException
    {
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double len = 0.2;
        String bounds = String.format("%f %f %.6f %.6f", ra, dec, len, len); 
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream stream = mock(ServletOutputStream.class);
        when(response.getOutputStream()).thenReturn(stream);
        List<ImageCutout> cutoutList = new ArrayList<>();
        ImageCutout cutout = createCutout(bounds + " some other stuff", DataAccessJobStatus.READY);
        cutoutList.add(cutout);
        when(imageCutoutRepository.findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png"))
                .thenReturn(cutoutList);
        String fileId = cutout.getFileId();
        File dataFile = createDataFile("aaa", "foo.png", "Imagine a picture");

        CachedFile cachedFile = new CachedFile(fileId, dataFile.getAbsolutePath(), 9L, DateTime.now());
        when(cacheManager.getCachedFile(cutout.getFileId())).thenReturn(cachedFile);
        
        boolean result = dataAccessService.downloadCutoutPreview(imageId, ra, dec, 0.1, response);
        assertThat(result, is(true));
        verify(imageCutoutRepository).findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png");
        verify(response, never()).sendError(anyInt());
    }

    @Test
    public void testDownloadCutoutPreviewSmallRadius() throws IOException
    {
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double radius = Double.parseDouble("0.0048000000000000004");
        double len = radius * 2d;
        String bounds = String.format("%f %f %.6f %.6f", ra, dec, len, len); 
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream stream = mock(ServletOutputStream.class);
        when(response.getOutputStream()).thenReturn(stream);
        List<ImageCutout> cutoutList = new ArrayList<>();
        ImageCutout cutout = createCutout(bounds + " some other stuff", DataAccessJobStatus.READY);
        cutoutList.add(cutout);
        when(imageCutoutRepository.findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png"))
                .thenReturn(cutoutList);
        String fileId = cutout.getFileId();
        File dataFile = createDataFile("aaa", "foo.png", "Imagine a picture");

        CachedFile cachedFile = new CachedFile(fileId, dataFile.getAbsolutePath(), 9L, DateTime.now());
        when(cacheManager.getCachedFile(cutout.getFileId())).thenReturn(cachedFile);
        
        boolean result = dataAccessService.downloadCutoutPreview(imageId, ra, dec, radius, response);
        assertThat(result, is(true));
        verify(imageCutoutRepository).findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png");
        verify(response, never()).sendError(anyInt());
    }

    @Test
    public void testDownloadCutoutPreviewPreparing() throws IOException
    {
        long imageId = 42;
        double ra = 161.264775;
        double dec = -59.684431;
        double len = 0.2;
        String bounds = String.format("%f %f %.6f %.6f", ra, dec, len, len); 
        HttpServletResponse response = mock(HttpServletResponse.class);
        List<ImageCutout> cutoutList = new ArrayList<>();
        ImageCutout cutout = createCutout(bounds + " some other stuff", DataAccessJobStatus.PREPARING);
        cutoutList.add(cutout);
        when(imageCutoutRepository.findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png"))
                .thenReturn(cutoutList);
        
        boolean result = dataAccessService.downloadCutoutPreview(imageId, ra, dec, 0.1, response);
        assertThat(result, is(true));
        verify(imageCutoutRepository).findByImageCubeIdBoundsAndDownloadFormat(imageId, bounds, "png");
        verify(response).sendError(204);
    }

    private ImageCutout createCutout(String bounds, DataAccessJobStatus status)
    {
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setBounds(bounds);
        DataAccessJob job = new DataAccessJob();
        job.setStatus(status);
        imageCutout.setDataAccessJob(job);
        ImageCube imageCube = new ImageCube();
        imageCube.setId(100L);
        imageCutout.setImageCube(imageCube);
        return imageCutout;
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
