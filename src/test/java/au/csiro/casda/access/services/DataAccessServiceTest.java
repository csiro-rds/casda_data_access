package au.csiro.casda.access.services;

import static au.csiro.casda.access.uws.AccessJobManagerTest.createCatalogue;
import static au.csiro.casda.access.uws.AccessJobManagerTest.createImageCube;
import static au.csiro.casda.access.uws.AccessJobManagerTest.createImageCutout;
import static au.csiro.casda.access.uws.AccessJobManagerTest.createMeasurementSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

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
import au.csiro.casda.access.CutoutFileDescriptor;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CatalogueRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.services.NgasService.Status;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
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
    private CachedFileRepository cachedFileRepository;

    @Mock
    private AccessJobManager accessJobManager;

    @Mock
    private NgasService ngasService;

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
                measurementSetRepository, cachedFileRepository, ngasService, cacheDir.getRoot().getAbsolutePath(),
                archiveStatusCommandAndArgs, new JavaProcessJobFactory());
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
    }

    @Test
    public void testIsInCacheInvalid() throws Exception
    {
        exception.expect(ResourceNotFoundException.class);
        exception.expectMessage("cube with id 123 does not exist");

        when(imageCubeRepository.findOne(123L)).thenReturn(null);

        dataAccessService.isFileInCache(new DataAccessDataProduct(DataAccessProductType.cube, 123L));
    }

    @Test
    public void testIsInCacheImageCube() throws Exception
    {
        ImageCube imageCube = createImageCube(123L, "image_cube-123.fits", 12L, "ABC123", 123123);
        CachedFile imageCubeCachedFile = new CachedFile(imageCube.getFileId(), "/path/to/123123-image_cube-123", 12L,
                DateTime.now().plusDays(2));
        imageCubeCachedFile.setFileAvailableFlag(true);
        when(imageCubeRepository.findOne(123L)).thenReturn(imageCube);
        when(cachedFileRepository.findByFileId(imageCube.getFileId())).thenReturn(imageCubeCachedFile);

        boolean result = dataAccessService.isFileInCache(new DataAccessDataProduct(DataAccessProductType.cube, 123L));
        assertTrue(result);
    }

    @Test
    public void testFindFileInCacheVisibility() throws Exception
    {
        MeasurementSet measurementSet = createMeasurementSet(111L, 11L, "ABC111", 111111);
        CachedFile measurementSetCachedFile = new CachedFile(measurementSet.getFileId(),
                "/path/to/111111-measurement_set-111", 11L, DateTime.now().plusDays(1));
        measurementSetCachedFile.setFileAvailableFlag(true);
        when(measurementSetRepository.findOne(111L)).thenReturn(measurementSet);
        when(cachedFileRepository.findByFileId(measurementSet.getFileId())).thenReturn(measurementSetCachedFile);

        boolean result =
                dataAccessService.isFileInCache(new DataAccessDataProduct(DataAccessProductType.visibility, 111L));
        assertTrue(result);
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
                measurementSetRepository, cachedFileRepository, ngasService, cacheDir.getRoot().getAbsolutePath(),
                archiveStatusCommandAndArgs, new JavaProcessJobFactory());

        ImageCube imageCube = createImageCube(125L, "image_cube-125.fits", 12L, "ABC123", 123123);
        when(imageCubeRepository.findOne(125L)).thenReturn(imageCube);
        when(cachedFileRepository.findByFileId(imageCube.getFileId())).thenReturn(null);

        Status ngasStatus = mock(Status.class);
        when(ngasStatus.wasSuccess()).thenReturn(true);
        when(ngasService.getStatus(imageCube.getFileId())).thenReturn(ngasStatus);
        when(ngasStatus.getMountPoint()).thenReturn("/ngas/mount/point");
        when(ngasStatus.getFileName()).thenReturn(imageCube.getFileId());

        Path imageCubePath =
                dataAccessService.findFileInNgasIfOnDisk(new DataAccessDataProduct(DataAccessProductType.cube, 125L));
        assertNull(imageCubePath);
    }

    @Test
    public void testFindFileInNgasWithFileIdNotOnDisk() throws Exception
    {
        String archiveStatusCommandAndArgs = TestUtils.getCommandAndArgsElStringForEchoOutput("OFL");
        dataAccessService = new DataAccessService(dataAccessJobRepository, imageCubeRepository,
                measurementSetRepository, cachedFileRepository, ngasService, cacheDir.getRoot().getAbsolutePath(),
                archiveStatusCommandAndArgs, new JavaProcessJobFactory());

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
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("AAA111");
        job.addImageCutout(createImageCutout(1L, 12L));
        job.addImageCutout(createImageCutout(12L, 13L));
        job.addImageCutout(createImageCutout(15L, 100L));
        job.setSizeKb(125L);

        Collection<DownloadFile> files = new ArrayList<>();
        files.add(new CutoutFileDescriptor(1L, "fileid", 17L, "originalImageFileId", 1000L));
        files.add(new CutoutFileDescriptor(12L, "fileid2", 108L, "originalImageFileId", 1000L));
        files.add(new CutoutFileDescriptor(14L, "fileid3", 102L, "originalImageFileId", 1000L));

        when(dataAccessJobRepository.findByRequestId("AAA111")).thenReturn(job);

        dataAccessService.updateFileSizeForCutouts("AAA111", files);
        verify(dataAccessJobRepository).save(eq(job));
        assertEquals(225L, job.getSizeKb().longValue());

        for (ImageCutout cutout : job.getImageCutouts())
        {
            switch (cutout.getId().intValue())
            {
            case 1:
                assertEquals(17L, cutout.getFilesize().longValue());
                break;
            case 12:
                assertEquals(108L, cutout.getFilesize().longValue());
                break;
            case 15:
                assertEquals(100L, cutout.getFilesize().longValue());
                break;
            default:
                fail("there are no other cutout ids");
                break;
            }
        }
    }

    @Test
    public void testUpdateSizeForCutoutDoesntUpdateOtherFiles() throws Exception
    {
        DataAccessJob job = new DataAccessJob();
        job.setRequestId("AAA111");
        job.addImageCube(createImageCube(1L, "image_cube-1.fits", 12L, "ABC123", 123));
        job.addMeasurementSet(createMeasurementSet(2L, 13L, "ASD111", 132));
        job.addCatalogue(createCatalogue(3L, 14L));
        job.addImageCutout(createImageCutout(15L, 100L));
        job.setSizeKb(139L);

        Collection<DownloadFile> files = new ArrayList<>();
        files.add(new FileDescriptor(job.getImageCubes().get(0).getFileId(), 17L, FileType.IMAGE_CUBE));
        files.add(new FileDescriptor(job.getMeasurementSets().get(0).getFileId(), 18L, FileType.MEASUREMENT_SET));
        files.add(new FileDescriptor(job.getCatalogues().get(0).getFileId(), 19L, FileType.CATALOGUE));
        files.add(new CutoutFileDescriptor(15L, "fileid3", 102L, "originalImageFileId", 1000L));

        when(dataAccessJobRepository.findByRequestId("AAA111")).thenReturn(job);

        dataAccessService.updateFileSizeForCutouts("AAA111", files);
        verify(dataAccessJobRepository).save(eq(job));
        assertEquals(141L, job.getSizeKb().longValue());

        assertEquals(12L, job.getImageCubes().get(0).getFilesize().longValue());
        assertEquals(13L, job.getMeasurementSets().get(0).getFilesize().longValue());
        assertEquals(14L, job.getCatalogues().get(0).getFilesize().longValue());
        assertEquals(102L, job.getImageCutouts().get(0).getFilesize().longValue());
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
