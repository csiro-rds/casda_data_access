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


import java.util.Optional;

import org.joda.time.DateTime;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.dataaccess.CachedFile;

/**
 * JPA Repository declaration. Copyright 2014, CSIRO Australia All rights reserved.
 */
@Repository
public interface CachedFileRepository extends CrudRepository<CachedFile, Long>
{

    /**
     * Returns the space available to be recovered for use by this job. It returns the size of the files where the
     * unlock time has passed. This means that only expired jobs are using these file. Uses optional as this function
     * returns null when there are no files.
     * 
     * @param time
     *            unlocked date/time needs to be before this date to be included in the sum. Note we are using
     *            {@link DateTime} which contains timezone information; the timezone should be set to UTC
     * 
     * @return The size of the unlocked cache files in kb - candidates to be removed.
     */
    @Query("SELECT sum(cf.sizeKb) FROM CachedFile cf WHERE cf.unlock < :time ")
    public Optional<Long> sumUnlockedCachedFileSize(@Param("time") DateTime time);

    /**
     * Uses optional as this function returns null when there are no files.
     * 
     * @return The total size of the cache files in kb.
     */
    @Query("SELECT sum(cf.sizeKb) FROM CachedFile cf")
    public Optional<Long> sumCachedFileSize();

    /**
     * Retrieve a cached file by its fileid.
     * 
     * @param fileId
     *            The id of the CachedFile.
     * @return The matching CachedFile, if any.
     */
    public CachedFile findByFileId(String fileId);

    /**
     * Retrieve the cached files that are ready to unlock (ie unlock time is earlier than the given time), ordered by
     * unlock time from earliest to latest.
     * 
     * @param time
     *            The latest unlock time to match.
     * @param pageable
     *            Specification of how the results should be paged.
     * @return The page of CachedFiles that can be unlocked.
     */
    @Query("SELECT cf FROM CachedFile cf WHERE cf.unlock < :time ORDER BY cf.unlock")
    public Page<CachedFile> findCachedFilesToUnlock(@Param("time") DateTime time, Pageable pageable);

    /**
     * Finds the cached files that are currently downloading. This is indicated by the fileAvailableFlag set to false,
     * the retry count set to a number less than the given maximum retry count, the filetype is not catalogue (these are
     * generated inline from a call to VO Tools)
     * 
     * @param maxRetryCount
     *            the maximum number of retries allowed
     * @param pageable
     *            Specification of how the results should be paged.
     * @return The page of downloading CachedFiles.
     */
    @Query("SELECT cf FROM CachedFile cf WHERE cf.fileAvailableFlag = false AND "
            + "cf.downloadJobRetryCount <= :maxRetryCount AND cf.fileType != 'CATALOGUE'"
            + " AND (cf.fileType != 'IMAGE_CUTOUT' OR (cf.fileType = 'IMAGE_CUTOUT' AND cf.originalFilePath is not null))")
    public Page<CachedFile> findDownloadingCachedFiles(@Param("maxRetryCount") int maxRetryCount, Pageable pageable);

}