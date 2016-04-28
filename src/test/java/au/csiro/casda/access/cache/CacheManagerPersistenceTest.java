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

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import javax.transaction.Transactional;

import org.apache.commons.io.FileUtils;
import org.hamcrest.core.Is;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import au.csiro.casda.access.BaseTest;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.TestEntityUtils;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.ObservationRepository;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.Observation;

/**
 * Tests the cache manager using an in memory database.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
public class CacheManagerPersistenceTest extends BaseTest
{
    @PersistenceUnit
    private EntityManagerFactory emf;

    private CachedFileRepository cachedFileRepository;

    private DataAccessJobRepository jobRepository;

    private ImageCubeRepository cubeRepository;

    private ObservationRepository observationRepository;

    private EntityManager entityManager;

    private CacheManager cacheManager;

    private final static String TEST_WORKING_DIR = "build/tempTest";

    public CacheManagerPersistenceTest() throws Exception
    {
        super();
    }

    @Before
    @Transactional
    public void setup()
    {
        entityManager = emf.createEntityManager();

        RepositoryFactorySupport rfs = new JpaRepositoryFactory(entityManager);
        cachedFileRepository = rfs.getRepository(CachedFileRepository.class);
        jobRepository = rfs.getRepository(DataAccessJobRepository.class);
        cubeRepository = rfs.getRepository(ImageCubeRepository.class);
        observationRepository = rfs.getRepository(ObservationRepository.class);

        cachedFileRepository.deleteAll();
        jobRepository.deleteAll();
        cubeRepository.deleteAll();
        observationRepository.deleteAll();

        File wkDir = new File(TEST_WORKING_DIR);
        FileUtils.deleteQuietly(wkDir);
        wkDir.mkdirs();

        cacheManager = new CacheManager(400l, 2, TEST_WORKING_DIR, cachedFileRepository, jobRepository);

        entityManager.getTransaction().begin();
    }

    @After
    public void tearDown()
    {
        entityManager.getTransaction().rollback();
    }

    @Test
    @Transactional
    public void testReclaimCachedFile() throws Exception
    {
        Observation obs = new Observation(1);

        ImageCube imageCube = new ImageCube();
        imageCube.setFilename("file1");
        imageCube.setFilesize(200L);
        imageCube.setParent(obs);

        String fileId = imageCube.getFileId();

        // cached file exists, and is unlocked
        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId(fileId);
        cachedFile.setSizeKb(200L);
        // unlock time 10000 ms ago - so it is free to be deleted
        cachedFile.setUnlock(DateTime.now(DateTimeZone.UTC).minusMillis(10000));

        CachedFile cachedFile2 = new CachedFile();
        cachedFile2.setFileId("observations-1-image_cubes-file2");
        cachedFile2.setSizeKb(199L);
        // unlock time is after now, so must remain in the cache
        cachedFile2.setUnlock(DateTime.now(DateTimeZone.UTC).plusMillis(1000000));
        cachedFileRepository.save(cachedFile);
        cachedFileRepository.save(cachedFile2);

        // make sure the cached files were saved correctly
        assertNotNull(cachedFileRepository.findByFileId(fileId));
        assertEquals(399L, cachedFileRepository.sumCachedFileSize().get().longValue());
        // make sure the first cached file is marked for deletion
        assertEquals(200L,
                cachedFileRepository.sumUnlockedCachedFileSize(DateTime.now(DateTimeZone.UTC)).get().longValue());

        // new job requests the file that can be deleted
        DataAccessJob job = new DataAccessJob();
        job.addImageCube(imageCube);

        Collection<DownloadFile> files = new ArrayList<>();
        files.addAll(job.getImageCubes().stream()
                .map(ic -> new FileDescriptor(ic.getFileId(), ic.getFilesize(), FileType.IMAGE_CUBE, ic.getFilename()))
                .collect(Collectors.toList()));

        cacheManager.reserveSpaceAndRegisterFilesForDownload(files, job);

        // make sure the cached file unlock date is significantly older than now
        // - this means it has been reclaimed and not deleted
        assertTrue(cachedFileRepository.findByFileId(fileId).getUnlock()
                .isAfter(new DateTime(System.currentTimeMillis()).plusDays(3)));
    }

    @Test
    public void testLinkJob() throws CacheException, IOException
    {
        Observation obs = TestEntityUtils.createObservation(123, 1000);
        ImageCube imageCube = TestEntityUtils.createImageCube(145L, 123L, obs, 1000);

        observationRepository.save(obs);

        String fileId = imageCube.getFileId();

        // cached file exists, and is unlocked
        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId(fileId);
        cachedFile.setSizeKb(200L);
        // unlock time 10000 ms ago - so it is free to be deleted
        cachedFile.setUnlock(new DateTime(System.currentTimeMillis() - 10000, DateTimeZone.UTC));

        CachedFile cachedFile2 = new CachedFile();
        cachedFile2.setFileId("observations-1-image_cubes-file2");
        cachedFile2.setSizeKb(199L);
        // unlock time is after now, so must remain in the cache
        cachedFile2.setUnlock(new DateTime(System.currentTimeMillis() + 1000000));
        cachedFileRepository.save(cachedFile);
        cachedFileRepository.save(cachedFile2);

        // make sure the cached files were saved correctly
        assertNotNull(cachedFileRepository.findByFileId(fileId));
        assertEquals(399L, cachedFileRepository.sumCachedFileSize().get().longValue());
        // make sure the first cached file is marked for deletion
        assertEquals(200L,
                cachedFileRepository
                        .sumUnlockedCachedFileSize(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC)).get()
                        .longValue());

        // new job requests the file that can be deleted
        DataAccessJob job = new DataAccessJob();
        job.addImageCube(imageCube);
        job.setRequestId("Alpha");
        jobRepository.save(job);

        CachedFile cfo = cacheManager.getCachedFile(imageCube.getFileId());
        assertNotNull(cfo);
        assertThat(cfo.getDataAccessJobs(), Is.is((Collection<DataAccessJob>) null));

        File savedFile = new File(imageCube.getFilename());
        savedFile.createNewFile();
        cacheManager.linkJob(job, cfo, savedFile);

        // Check the result
        CachedFile storedCf = cacheManager.getCachedFile(imageCube.getFileId());
        assertNotNull(storedCf);
        assertThat(storedCf.getDataAccessJobs(), contains(job));
        assertThat(storedCf.getDataAccessJobs().size(), Is.is(1));
    }
}
