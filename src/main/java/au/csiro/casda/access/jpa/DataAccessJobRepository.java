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
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

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
     * Finds requests that have been in the prepared since before a specified time. 
     * 
     * @param time
     *            the given date/time
     * @return the list of old preparing jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status = 'PREPARING' AND daj.createdTimestamp <= :time")
    public List<DataAccessJob> findPreparingJobsOlderThanTime(@Param(value = "time") DateTime time);
    
    /**
     * Finds requests that are currently paused
     * 
     * @return the list of paused jobs
     */
    @Query("SELECT daj FROM DataAccessJob daj WHERE daj.status = 'PAUSED' ORDER BY daj.createdTimestamp")
    public List<DataAccessJob> findPausedJobs();
    
    /**
     * Expire all jobs
     */
    @Modifying
    @Query("UPDATE DataAccessJob daj set "
            + "daj.status = 'CANCELLED', "
            + "daj.expiredTimestamp = current_timestamp, "
            + "daj.errorMessage = 'Cache deleted by the administrator.' "
            + "where daj.expiredTimestamp >= current_timestamp")
    public void expireAllJobs();
    
    /**
     * Finds jobs which will expire in the given time limit
     * 
     * @param maxTime
     *            the given date/time
     * @param minTime
     *            the given date/time minus one day, so notification is only sned out once
     * @return the list of jobs expiring in the given time limit
     */
    @Query("select daj from DataAccessJob daj where daj.status = 'READY' and "
    		+ "daj.expiredTimestamp <= :maxTime and daj.expiredTimestamp > :minTime")
    public List<DataAccessJob> findAllJobsForExpiryNotification(@Param(value = "maxTime") DateTime maxTime, 
    		@Param(value = "minTime") DateTime minTime);
    
    /**
     * find all jobs which have passed the expiredTimestamp date, but have not yet been set to expired
     * @return the list of expired jobs
     */
    @Query("select daj from DataAccessJob daj where daj.status = 'READY' and daj.expiredTimestamp <= current_date()")
    public List<DataAccessJob> findExpiredJobs();
}
