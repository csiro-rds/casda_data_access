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


import java.util.List;

import org.joda.time.DateTime;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;

/**
 * JPA Repository declaration. Copyright 2014, CSIRO Australia All rights reserved.
 */
@Repository
public interface DataAccessJobRepository extends CrudRepository<DataAccessJob, Long>
{
    /**
     * Find a data access job by its request id.
     * 
     * @param id
     *            The request id of the job.
     * @return The matching DataAccessJob, or null if not found.
     */
    public DataAccessJob findByRequestId(String id);

    /**
     * Finds the latest expiry date for a data access job that requires the given cached file.
     * 
     * @param cachedFileId
     *            id of the cached file
     * @return the maximum expiry date of existing data access job associated with the cached file
     */
    @Query("SELECT max(daj.expiredTimestamp) FROM CachedFile cf INNER JOIN cf.dataAccessJobs daj "
            + "WHERE cf.id = :cachedFileId")
    public DateTime findLatestJobExpiryForCachedFile(@Param(value = "cachedFileId") long cachedFileId);

    /**
     * Find jobs that 1) haven't had their status updated to 'EXPIRED', 2) match the given download mode and 3) expired
     * before a give time.
     * 
     * @param time
     *            the time before which the jobs expired.
     * @param downloadMode
     *            the download mode requested for the job
     * @param pageable
     *            Specification of how the results should be paged.
     * 
     * @return the list of jobs that match the download mode and have expired, but haven't had their status updated to
     *         EXPIRED
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.expiredTimestamp < :time AND daj.status != 'EXPIRED'"
            + " AND daj.downloadMode = :downloadMode ORDER by daj.expiredTimestamp ASC")
    public Page<DataAccessJob> findJobsToExpire(@Param(value = "time") DateTime time,
            @Param(value = "downloadMode") CasdaDownloadMode downloadMode, Pageable pageable);

    /**
     * Finds requests that failed on or after the given date/time.
     * 
     * @param time
     *            the given date/time
     * @return the list of failed jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status = 'ERROR' AND daj.lastModified >= :time "
            + "ORDER BY daj.lastModified DESC")
    public List<DataAccessJob> findFailedJobsAfterTime(@Param(value = "time") DateTime time);

    /**
     * Finds requests that were made available on or after the given date/time
     * 
     * @param time
     *            the given date/time
     * @return the list of available jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status NOT IN ('ERROR', 'PREPARING') AND "
            + "daj.availableTimestamp >= :time ORDER BY daj.availableTimestamp DESC")
    public List<DataAccessJob> findCompletedJobsAfterTime(@Param(value = "time") DateTime time);

    /**
     * Finds requests that are currently being prepared
     * 
     * @return the list of preparing jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status = 'PREPARING' ORDER BY daj.createdTimestamp")
    public List<DataAccessJob> findPreparingJobs();
    
    /**
     * Finds requests that are currently paused
     * 
     * @return the list of paused jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status = 'PAUSED' ORDER BY daj.createdTimestamp")
    public List<DataAccessJob> findPausedJobs();

}
