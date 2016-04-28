package au.csiro.casda.access.cache;

import java.io.File;
import java.util.Collection;

import org.joda.time.DateTime;

import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.DataAccessJob;

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

/**
 * An interface for the CacheManager's public methods. Provided to assist Spring transaction management via a proxy
 * class.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public interface CacheManagerInterface
{

    /**
     * Checks if all the files are already available in the cache.
     * 
     * @param files
     *            the list of files to check for availability in the cache
     * @return true if all files are available, false otherwise
     */
    public boolean allFilesAvailableInCache(Collection<DownloadFile> files);

    /**
     * Checks if there is enough space available for the given data access job. If required, removes unlocked files from
     * the cache until it has enough space for this job. If there is not enough space and there are no files that can be
     * removed it throws a CacheException. When removing unlocked cached files it removes the file in the data directory
     * and the whole job directory for any jobs using that file - as that job is no longer available.
     * 
     * If there is enough space, it will add all the files to the CachedFile table as placeholders so that subsequent
     * calls to this method can accurately assess what is already in and space that has been reserved in the cache for
     * files to be downloaded. The CachedFiles that are reserving space are identified by the DownloadManager which runs
     * a scheduled task to start and check download of files.
     * 
     * @param files
     *            files to reserve space for, and to register in the cache for downloading
     * @param dataAccessJob
     *            the data access job to reserve space for
     * 
     * @return the total size of the files that are already in the cache
     * @throws CacheFullException
     *             if the cache is full and can't reserve space for this job
     * @throws CacheException
     *             if there is any other problem reserving space for this job
     */
    public long reserveSpaceAndRegisterFilesForDownload(Collection<DownloadFile> files, DataAccessJob dataAccessJob)
            throws CacheFullException, CacheException;

    /**
     * @param job
     *            the data access job
     * @return the directory where the files or symlinks for a given job is stored
     */
    public File getJobDirectory(DataAccessJob job);

    /**
     * Creates a symbolic link for the given request id to the given file, and its checksum file if required. The job's
     * directory will be created if it isn't already present.
     * 
     * @param requestId
     *            the job request id
     * @param file
     *            the file to link to
     * @param checksum
     *            if true, will also create a symlink to the file's checksum file.
     * @throws CacheException
     *             if there is a problem creating the symlink
     */
    public void createSymLink(String requestId, File file, boolean checksum) throws CacheException;

    /**
     * Returns the file if it is in the cache
     * 
     * @param fileId
     *            file id to check
     * @return cached file
     */
    public CachedFile getCachedFile(String fileId);

    /**
     * Record that the job is using the cached file.
     * 
     * @param job
     *            that is using the cached file
     * @param cachedFileParam
     *            cached file that this job is using
     * @param savedFile
     *            Downloaded file to add to the cache
     * @throws CacheException
     *             if there if a problem linking the file to the job
     */
    public void linkJob(DataAccessJob job, CachedFile cachedFileParam, File savedFile) throws CacheException;

    /**
     * Updates the cached file's unlock time to the latest of either the given unlock time OR the latest time required
     * for an existing job that requires this file.
     * 
     * @param files
     *            the files to update
     * @param unlock
     *            the new value for the unlock time (unless another job requires it for longer)
     */
    public void updateUnlockForFiles(Collection<DownloadFile> files, DateTime unlock);

    /**
     * @return The maximum size of the cache, in KB.
     */
    public long getMaxCacheSizeKb();

    /**
     * @return The used size of the cache, in KB.
     */
    public long getUsedCacheSizeKb();

    /**
     * Updates the file size value in the CachedFile table, for a file that matches the given download file. Also
     * updates the file available flag to true if the file exists in the cache
     * 
     * @param job
     *            the data access job
     * @param downloadFile
     *            the details of the file the user requested to download
     * @return file size in kb
     * @throws CacheException
     *             if there is any problem finding or updating the matching record
     */
    public Long updateSizeForCachedFile(DataAccessJob job, DownloadFile downloadFile) throws CacheException;

    /**
     * Updates the original file path value in the CachedFile table, for a file that matches the given download file.
     * 
     * @param downloadFile
     *            the details of the file the user requested to download
     * @param path
     *            the file path value
     * @throws CacheException
     *             if there is any problem finding or updating the matching record
     */
    public void updateOriginalFilePath(DownloadFile downloadFile, String path) throws CacheException;

    /**
     * Checks whether the requested file is available in the cache.
     * 
     * @param requiredFile
     *            the requested file
     * @return true if the file is available in the cache
     * @throws CacheException
     *             if the file is missing from the cache because it failed downloading, or wasn't registered
     */
    public boolean isCachedFileAvailable(DownloadFile requiredFile) throws CacheException;

    /**
     * Creates the directory for the user's data access job
     * 
     * @param job
     *            the user's data access job request
     * @param files
     *            the list of files requested for download, to include (symlinks to) in the directory
     * @throws CacheException
     *             if the file is not present in the cache
     */
    public void createDataAccessJobDirectory(DataAccessJob job, Collection<DownloadFile> files) throws CacheException;
    
    /**
     * Signal to clear a cache entry safely
     * 
     * @param cachedFile
     *            cachedFile entry to be deleted
     */
    public void clearCacheIfPossible(CachedFile cachedFile);

}
