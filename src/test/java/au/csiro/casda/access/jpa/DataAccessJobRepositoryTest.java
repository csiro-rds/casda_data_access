package au.csiro.casda.access.jpa;

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import au.csiro.casda.access.TestAppConfig;
import au.csiro.casda.access.TestEntityUtils;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.Observation;

/**
 * Tests the DataAccessJobRepository methods using in memory GeoDb instance.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestAppConfig.class })
public class DataAccessJobRepositoryTest
{

    @Autowired
    private DataAccessJobRepository dataAccessJobRepository;

    @Autowired
    private CachedFileRepository cachedFileRepository;
    
    @Autowired
    private ObservationRepository observationRepository;

    @Before
    public void setup()
    {
        cachedFileRepository.deleteAll();
        dataAccessJobRepository.deleteAll();

        List<DataAccessJob> jobs = new ArrayList<>();
        DateTime now = DateTime.now();
        jobs.add(createDataAccessJob(now.minusDays(2), null, null, CasdaDownloadMode.WEB, "failed1",
                DataAccessJobStatus.ERROR));
        jobs.add(createDataAccessJob(now.minusDays(4), now.minusDays(2), now.minusMillis(1000), CasdaDownloadMode.WEB,
                "request1", DataAccessJobStatus.EXPIRED));
        jobs.add(createDataAccessJob(now.minusDays(4), now.minusDays(2).minusSeconds(4), now.minusMillis(1000),
                CasdaDownloadMode.WEB, "request2", DataAccessJobStatus.READY));
        jobs.add(createDataAccessJob(now, null, null, CasdaDownloadMode.WEB, "request3", null));
        jobs.add(createDataAccessJob(now.minusDays(5), now.minusDays(3), now.minusMillis(10000), CasdaDownloadMode.WEB,
                "request4", DataAccessJobStatus.READY));
        jobs.add(createDataAccessJob(now.minusDays(5), now.minusDays(3), now.minusMillis(10000),
                CasdaDownloadMode.PAWSEY_HTTP, "request5", DataAccessJobStatus.READY));
        jobs.add(createDataAccessJob(now.minusDays(4), now.minusDays(3), now.minusMillis(100),
                CasdaDownloadMode.PAWSEY_HTTP, "request6", DataAccessJobStatus.READY));
        jobs.add(createDataAccessJob(now.minusDays(4), now.minusDays(3), now.minusMillis(102),
                CasdaDownloadMode.PAWSEY_HTTP, "request7", DataAccessJobStatus.READY));
        jobs.add(createDataAccessJob(now, null, null, CasdaDownloadMode.WEB, "request8", null));

        jobs.add(createDataAccessJob(now.minusDays(2), null, null, CasdaDownloadMode.WEB, "request9",
                DataAccessJobStatus.PREPARING));
        jobs.add(createDataAccessJob(now.minusDays(10), null, null, CasdaDownloadMode.WEB, "request10",
                DataAccessJobStatus.PREPARING));
        jobs.add(createDataAccessJob(now.minusDays(2), null, null, CasdaDownloadMode.WEB, "failed2",
                DataAccessJobStatus.ERROR));
        jobs.add(createDataAccessJob(now.minusDays(2), null, null, CasdaDownloadMode.WEB, "paused1",
                DataAccessJobStatus.PAUSED));

        Iterator<DataAccessJob> savedJobs = dataAccessJobRepository.save(jobs).iterator();

        CachedFile cachedFile = createCachedFile();
        while (savedJobs.hasNext())
        {
            cachedFile.addJob(savedJobs.next());
        }
        cachedFileRepository.save(cachedFile);
    }

    @Test
    public void testFindLatestJobExpiryForCachedFile()
    {
        Long cachedFileId = cachedFileRepository.findByFileId("fileId").getId();
        DateTime dateOfLatest = dataAccessJobRepository.findByRequestId("request6").getExpiredTimestamp();

        DateTime latestDate = dataAccessJobRepository.findLatestJobExpiryForCachedFile(cachedFileId);
        assertNotNull(latestDate);
        assertEquals(dateOfLatest, latestDate);
    }

    @Test
    public void testFindJobsToExpire()
    {
        Pageable pageable = new PageRequest(0, 2);

        Page<DataAccessJob> page =
                dataAccessJobRepository.findJobsToExpire(DateTime.now(), CasdaDownloadMode.PAWSEY_HTTP, pageable);
        assertEquals(2, page.getTotalPages());
        assertTrue(page.hasContent());
        assertEquals(2, page.getNumberOfElements());
        List<DataAccessJob> jobs = page.getContent();
        assertEquals("request5", jobs.get(0).getRequestId());
        assertEquals("request7", jobs.get(1).getRequestId());

        pageable = pageable.next();
        page = dataAccessJobRepository.findJobsToExpire(DateTime.now(), CasdaDownloadMode.PAWSEY_HTTP, pageable);
        assertEquals(2, page.getTotalPages());
        assertTrue(page.hasContent());
        assertEquals(1, page.getNumberOfElements());
        jobs = page.getContent();
        assertEquals("request6", jobs.get(0).getRequestId());

        pageable = new PageRequest(0, 2);
        page = dataAccessJobRepository.findJobsToExpire(DateTime.now(), CasdaDownloadMode.WEB, pageable);
        assertEquals(1, page.getTotalPages());
        assertTrue(page.hasContent());
        assertEquals(2, page.getNumberOfElements());
        jobs = page.getContent();
        assertEquals("request4", jobs.get(0).getRequestId());
        assertEquals("request2", jobs.get(1).getRequestId());
    }

    @Test
    public void testFindFailedJobsAfterTime() throws Exception
    {
        List<DataAccessJob> failedJobs = dataAccessJobRepository.findFailedJobsAfterTime(DateTime.now().plus(1));
        assertEquals(0, failedJobs.size());

        failedJobs = dataAccessJobRepository.findFailedJobsAfterTime(DateTime.now().minusDays(2));
        assertEquals(2, failedJobs.size());
        assertThat(new String[] { failedJobs.get(0).getRequestId(), failedJobs.get(1).getRequestId() },
                arrayContainingInAnyOrder("failed1", "failed2"));
        // the last modified date is automatically updated on save, so it's hard to control - we do assert false
        // on isAfter because the last modified date of the second failed job could be before or equal to the
        // last modified date of the first failed job (descending date order)
        assertFalse(failedJobs.get(1).getLastModified().isAfter(failedJobs.get(0).getLastModified()));
    }

    @Test
    public void testFindAvailableJobsAfterTime() throws Exception
    {
        List<DataAccessJob> availableJobs = dataAccessJobRepository.findCompletedJobsAfterTime(DateTime.now());
        assertEquals(0, availableJobs.size());

        availableJobs = dataAccessJobRepository.findCompletedJobsAfterTime(DateTime.now().minusDays(2).minusHours(12));

        assertEquals(2, availableJobs.size());
        assertEquals("request1", availableJobs.get(0).getRequestId());
        assertEquals("request2", availableJobs.get(1).getRequestId());
        // ordering by descending available timestamp
        assertTrue(availableJobs.get(1).getAvailableTimestamp().isBefore(availableJobs.get(0).getAvailableTimestamp()));
    }

    @Test
    public void testFindPreparingJobs() throws Exception
    {
        List<DataAccessJob> availableJobs = dataAccessJobRepository.findPreparingJobs();
        assertEquals(2, availableJobs.size());

        assertThat(availableJobs.stream().map(job -> job.getRequestId()).collect(Collectors.toList()),
                contains("request10", "request9"));
        assertTrue(availableJobs.get(0).getCreatedTimestamp().isBefore(availableJobs.get(1).getCreatedTimestamp()));
    }
    
    @Test
    public void testFindPausedJobs() throws Exception
    {
        List<DataAccessJob> pausedJobs = dataAccessJobRepository.findPausedJobs();
        assertEquals(1, pausedJobs.size());
        assertEquals("paused1", pausedJobs.get(0).getRequestId());
    }
    
    @Test
    public void testSaveDataAccessJobCutouts() throws Exception
    {
        Observation obs = TestEntityUtils.createObservation(1231, 1000);
        ImageCube imageCube = TestEntityUtils.createImageCube(145L, 123L, obs, 1033);

        obs = observationRepository.save(obs);
        imageCube = obs.getImageCubes().get(0);
        
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setImageCube(imageCube);
        
        DataAccessJob job = new DataAccessJob();
        job.addImageCutout(imageCutout);
        
        job = dataAccessJobRepository.save(job);
        assertNotNull(job.getId());
        assertEquals(1, job.getImageCutouts().size());
        assertEquals(0, job.getImageCubes().size());
        assertEquals(0, job.getMeasurementSets().size());
        assertEquals(0, job.getCatalogues().size());
        assertEquals(1, job.getFileCount());
    }

    private DataAccessJob createDataAccessJob(DateTime createdTimestamp, DateTime availableTimestamp,
            DateTime expiredTimestamp, CasdaDownloadMode downloadMode, String requestId, DataAccessJobStatus status)
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setUserIdent("usr001");
        dataAccessJob.setUserLoginSystem("NEXUS");
        dataAccessJob.setUserName("User");
        dataAccessJob.setUserEmail("user@csiro.au");

        dataAccessJob.setDownloadMode(downloadMode);

        DateTime now = new DateTime(System.currentTimeMillis(), DateTimeZone.UTC);
        dataAccessJob.setCreatedTimestamp(createdTimestamp);
        dataAccessJob.setAvailableTimestamp(availableTimestamp);
        dataAccessJob.setExpiredTimestamp(expiredTimestamp);
        dataAccessJob.setLastModified(now);
        dataAccessJob.setRequestId(requestId);
        dataAccessJob.setStatus(status);
        return dataAccessJob;
    }

    private CachedFile createCachedFile()
    {
        CachedFile cachedFile = new CachedFile();
        cachedFile.setFileId("fileId");
        return cachedFile;
    }
}
