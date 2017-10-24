package au.csiro.casda.access.jpa;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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


import static org.hamcrest.CoreMatchers.is;

import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import javax.transaction.Transactional;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import au.csiro.casda.access.BaseTest;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;

/**
 * Tests for the repository using in memory GeoDb instance.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
public class CachedFileRepositoryTest extends BaseTest
{
    private DateTime now;
    private DateTime future;
    private DataAccessJob testJob;

    @PersistenceUnit
    private EntityManagerFactory emf;

    private CachedFileRepository repository;

    private DataAccessJobRepository jobRepository;

    private EntityManager entityManager;

    public CachedFileRepositoryTest() throws Exception
    {
        super();
    }

    @Before
    @Transactional
    public void setup()
    {
        entityManager = emf.createEntityManager();

        RepositoryFactorySupport rfs = new JpaRepositoryFactory(entityManager);
        repository = rfs.getRepository(CachedFileRepository.class);
        jobRepository = rfs.getRepository(DataAccessJobRepository.class);

        repository.deleteAll();
        jobRepository.deleteAll();

        DataAccessJob job = new DataAccessJob();
        job.setRequestId("4477-9999");
        testJob = jobRepository.save(job);

        now = DateTime.now();
        future = now.plusMillis(1000000);

        entityManager.getTransaction().begin();
        this.saveCachedFile(4l, now.minusMillis(100000), "file1", "1", FileType.IMAGE_CUBE, 1, true, null);
        this.saveCachedFile(5l, now.minusMillis(100008), "file2", "2", FileType.MEASUREMENT_SET, 1, true, null, testJob);
        this.saveCachedFile(8l, future, "file3", "1", FileType.IMAGE_CUBE, 1, true, null);
        this.saveCachedFile(10l, future, "file4", "1", FileType.CATALOGUE, 1, false, null);
        this.saveCachedFile(11l, future, "file5", "1", FileType.CATALOGUE, 1, true, null);
        this.saveCachedFile(12l, future, "file6", "1", FileType.CATALOGUE, 2, false, null);
        this.saveCachedFile(13l, future, "file7", "1", FileType.CATALOGUE, 2, true, null);
        this.saveCachedFile(14l, future, "file8", "1", FileType.IMAGE_CUBE, 1, false, null);
        this.saveCachedFile(15l, future, "file9", "1", FileType.IMAGE_CUBE, 1, true, null);
        this.saveCachedFile(16l, future, "file10", "1", FileType.IMAGE_CUBE, 2, false, null);
        this.saveCachedFile(17l, future, "file11", "1", FileType.IMAGE_CUBE, 2, true, null);
        this.saveCachedFile(18l, future, "file12", "1", FileType.MEASUREMENT_SET, 1, false, null);
        this.saveCachedFile(19l, future, "file13", "1", FileType.MEASUREMENT_SET, 1, true, null);
        this.saveCachedFile(20l, future, "file14", "1", FileType.MEASUREMENT_SET, 2, false, null);
        this.saveCachedFile(21l, future, "file15", "1", FileType.MEASUREMENT_SET, 2, true, null);
        this.saveCachedFile(22L, future, "file16", "1", FileType.IMAGE_CUTOUT, 1, false, null);
        this.saveCachedFile(23L, future, "file17", "1", FileType.IMAGE_CUTOUT, 1, false, "some/path17");
        this.saveCachedFile(24L, future, "file18", "2", FileType.IMAGE_CUTOUT, 2, false, "some/path18");
        this.saveCachedFile(25L, future, "file19", "1", FileType.IMAGE_CUTOUT, 1, true, null);
        this.saveCachedFile(26L, future, "file20", "1", FileType.IMAGE_CUTOUT, 1, true, "some/path20");
        
        
        
    }

    @After
    public void tearDown()
    {
        entityManager.getTransaction().rollback();
    }

    @Test
    public void testSumUnlockedCachedFileSize()
    {
        assertThat(repository.sumUnlockedCachedFileSize(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC))
                .get(), is(9l));
    }

    @Test
    public void testSumCachedFileSize()
    {
        assertThat(repository.sumCachedFileSize().get(), is(323L));
        repository.deleteAll();
        assertThat(repository.sumCachedFileSize().isPresent(), is(false));

    }

    @Test
    @Transactional
    // transactional to allow walking object tree to data access jobs
    public void testfindByFileIdSuccess()
    {
        CachedFile cf = repository.findByFileId("file2");
        assertThat(cf.getSizeKb(), is(5l));
        assertThat(cf.getDataAccessJobs().iterator().next().getRequestId(), is("4477-9999"));
    }

    @Test
    public void testfindByFileIdFail()
    {
        assertNull(repository.findByFileId("file0"));
    }

    @Test
    public void testFindCachedFilesToUnlock()
    {
        Pageable page = new PageRequest(0, 1);
        Page<CachedFile> results = repository.findCachedFilesToUnlock(now, page);
        assertTrue(results.hasContent());
        assertThat(repository.findCachedFilesToUnlock(now, page).getContent().get(0).getSizeKb(), is(5l));

        assertThat(repository.findCachedFilesToUnlock(DateTime.now(DateTimeZone.UTC).minusMillis(200000), page)
                .hasContent(), is(false));
    }

    @Test
    public void testFindDownloadingCachedFiles()
    {
        Pageable page = new PageRequest(0, 10);
        Page<CachedFile> results = repository.findDownloadingCachedFiles(1, page);
        assertTrue(results.hasContent());
        assertThat(results.getTotalElements(), is(3L));
        for (CachedFile cachedFile : results.getContent())
        {
            assertThat(cachedFile.getDownloadJobRetryCount(), lessThan(2));
            assertThat(cachedFile.getFileType(), not(FileType.CATALOGUE));
            if (cachedFile.getFileType() == FileType.IMAGE_CUTOUT)
            {
                assertNotNull(cachedFile.getOriginalFilePath());
            }
            assertFalse(cachedFile.isFileAvailableFlag());
        }
    }

    @Transactional
    private void saveCachedFile(Long size, DateTime unlock, String fileId, String version, FileType fileType,
            int downloadJobRetryCount, boolean fileAvailableFlag, String originalFilePath, DataAccessJob... jobs)
    {
        CachedFile cf = new CachedFile();
        cf.setSizeKb(size);
        cf.setUnlock(unlock);
        cf.setFileId(fileId);
        cf.setFileType(fileType);
        cf.setFileAvailableFlag(fileAvailableFlag);
        cf.setDownloadJobRetryCount(downloadJobRetryCount);
        cf.setOriginalFilePath(originalFilePath);
        for (DataAccessJob job : jobs)
        {
            cf.addJob(job);
        }
        repository.save(cf);
    }
}
