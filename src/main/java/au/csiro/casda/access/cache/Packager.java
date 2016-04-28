package au.csiro.casda.access.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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

import java.util.Collection;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.CatalogueDownloadFile;
import au.csiro.casda.access.CutoutFileDescriptor;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.InlineScriptException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.rest.CatalogueRetrievalException;
import au.csiro.casda.access.rest.CreateChecksumException;
import au.csiro.casda.access.rest.VoToolsCataloguePackager;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.logging.CasdaFormatter;
import au.csiro.casda.logging.DataLocation;

/**
 * Packager prepares packs of files ordered by users for retrieval.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class Packager
{
    /**
     * Result of the packaging process.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class Result
    {
        private final DateTime expiryDate;
        private final long cachedSizeKb;
        private final long totalSizeKb;

        /**
         * Constructor
         * 
         * @param expiryDate
         *            the expiry date for the files
         * @param cachedSizeKb
         *            the total size in KB of files already available in the cache
         * @param totalSizeKb
         *            the total size in KB of the data access job being packaged
         */
        public Result(DateTime expiryDate, long cachedSizeKb, long totalSizeKb)
        {
            this.expiryDate = expiryDate;
            this.cachedSizeKb = cachedSizeKb;
            this.totalSizeKb = totalSizeKb;
        }

        public DateTime getExpiryDate()
        {
            return expiryDate;
        }

        public long getCachedSizeKb()
        {
            return cachedSizeKb;
        }

        public long getTotalSizeKb()
        {
            return totalSizeKb;
        }

    }

    private final CacheManagerInterface cacheManager;

    private VoToolsCataloguePackager voToolsCataloguePackager;

    private DataAccessService dataAccessService;

    /** Time to sleep when waiting for downloading file processes to finish */
    private int downloadSleepIntervalMillis;

    private static Logger logger = LoggerFactory.getLogger(Packager.class);

    private InlineScriptService inlineScriptService;

    private String calculateChecksumScript;

    /**
     * Constructor
     * 
     * @param cacheManager
     *            to use for finding and storing downloaded files.
     * @param voToolsCataloguePackager
     *            service for downloading catalogue files from VO Tools
     * @param dataAccessService
     *            service used to access data
     * @param downloadSleepInterval
     *            time to sleep when waiting for downloading file processes to finish, millis
     * @param inlineScriptService
     *            service for calling shell scripts inline, used here to create checksums
     * @param calculateChecksumScript
     *            the path to the calculate checksum script
     */
    @Autowired
    public Packager(CacheManagerInterface cacheManager, VoToolsCataloguePackager voToolsCataloguePackager,
            DataAccessService dataAccessService, @Value("${download.sleep.interval}") int downloadSleepInterval,
            InlineScriptService inlineScriptService,
            @Value("${calculate.checksum.script}") String calculateChecksumScript)
    {
        this.cacheManager = cacheManager;
        this.voToolsCataloguePackager = voToolsCataloguePackager;
        this.dataAccessService = dataAccessService;
        this.downloadSleepIntervalMillis = downloadSleepInterval;
        this.inlineScriptService = inlineScriptService;
        this.calculateChecksumScript = calculateChecksumScript;
    }

    /**
     * Creates the job directory with files requested. Uses existing files in cache or requests files from archive.
     * 
     * @param job
     *            which is having its files prepared
     * @param hoursToExpiryForJob
     *            the number of hours after which the files associated with this job may be removed
     * @return the time that job will expire (for Web, this is also the time the files are in the cache)
     * @throws CacheFullException
     *             if the cache is full
     * @throws CacheException
     *             likely problems with disks and permissions
     * @throws CreateChecksumException
     *             if there is a problem creating a checksum
     * @throws CatalogueRetrievalException
     *             if there is a problem retrieving the catalogue file
     * @throws InterruptedException
     *             if the packager processing is interrupted during the polling process
     * @throws ResourceNotFoundException
     *             if the job's file type was an IMAGE_CUTOUT and the source file could not be found in NGAS
     */
    public Result pack(DataAccessJob job, int hoursToExpiryForJob) throws CacheFullException, CacheException,
            CatalogueRetrievalException, CreateChecksumException, InterruptedException, ResourceNotFoundException
    {
        Collection<DownloadFile> files = assembleDataAccessJobDownloadFiles(job);

        if (CollectionUtils.isEmpty(files))
        {
            // return now - no need to go further if there are no files to download.
            return new Result(DateTime.now(DateTimeZone.UTC), 0, 0);
        }

        // If it is possible to release enough space, old files will be irreversibly deleted.
        // This method will register the files that need to be downloaded in the CachedFile table as placeholders, so we
        // can make accurate size estimations. These will be identified as running jobs by the DownloadManager which
        // runs a scheduled task to start and check download jobs. This method sets a default expiry time on the files
        // to now + one week.
        Long sizeInCacheKb = cacheManager.reserveSpaceAndRegisterFilesForDownload(files, job);

        pollUntilFileDownloadComplete(job, files, hoursToExpiryForJob);

        cacheManager.createDataAccessJobDirectory(job, files);

        // re-calculate the job expiry to allow for some slow retrieval and update cached files
        DateTime jobExpiryDate = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob);
        cacheManager.updateUnlockForFiles(files, jobExpiryDate);
        dataAccessService.updateFileSizeForCutouts(job.getRequestId(), files);

        long sizeKbOfThisJob = files.stream().mapToLong(file -> file.getSizeKb()).sum();
        return new Result(jobExpiryDate, sizeInCacheKb, sizeKbOfThisJob);
    }

    /**
     * Assembles the download file details for the files requested by this data access job.
     * 
     * @param job
     *            the data access job
     * @return the details of the files requested for download.
     * @throws ResourceNotFoundException
     *             if the job's file type was an IMAGE_CUTOUT and the source file could not be found in NGAS
     */
    Collection<DownloadFile> assembleDataAccessJobDownloadFiles(DataAccessJob job) throws ResourceNotFoundException
    {
        Collection<DownloadFile> files =
                DataAccessUtil.getDataAccessJobDownloadFiles(job, cacheManager.getJobDirectory(job));

        for (DownloadFile downloadFile : files)
        {
            if (downloadFile.getFileType() == FileType.CATALOGUE)
            {
                long sizeKb = voToolsCataloguePackager.estimateFileSizeKb((CatalogueDownloadFile) downloadFile);
                downloadFile.setSizeKb(sizeKb);
            }
            if (downloadFile.getFileType() == FileType.IMAGE_CUTOUT)
            {
                CutoutFileDescriptor cutoutDownloadFile = (CutoutFileDescriptor) downloadFile;
                // if the file is on disk, use it - otherwise we will rely on getting it from the cache
                try
                {
                    Path filePathOnDisk = dataAccessService
                            .findFileInNgasIfOnDisk(cutoutDownloadFile.getOriginalImageDownloadFile().getFileId());
                    if (filePathOnDisk != null)
                    {
                        cutoutDownloadFile.setOriginalImageFilePath(filePathOnDisk.toFile().getAbsolutePath());
                    }
                }
                catch (ServiceCallException e)
                {
                    logger.error("There was a problem accessing ngas, will attempt to download to cache", e);
                }
            }
        }

        return files;
    }

    /**
     * Make sure all required files are in the cache
     * 
     * @param job
     *            job which is having its files prepared
     * @param hoursToExpiryForJob
     *            the number of hours after which the files associated with this job may be removed
     * @param files
     *            files to be available for access
     * @throws CacheException
     *             likely problems with disks and permissions
     * @throws CreateChecksumException
     *             if there is a problem creating a checksum
     * @throws CatalogueRetrievalException
     *             if there is a problem retrieving the catalogue file
     * @throws InterruptedException
     *             if the packager processing is interrupted during the polling process
     */
    void pollUntilFileDownloadComplete(DataAccessJob job, Collection<DownloadFile> files, int hoursToExpiryForJob)
            throws CacheException, CatalogueRetrievalException, CreateChecksumException, InterruptedException
    {
        DateTime jobExpiryDate = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob);

        while (true)
        {
            logger.debug("Polling for downloaded files for job request id {}", job.getRequestId());

            boolean allFilesAvailable = true;
            for (DownloadFile requiredFile : files)
            {
                if (!requiredFile.isComplete())
                {
                    if (requiredFile.getFileType() == FileType.CATALOGUE)
                    {
                        DateTime fileAssemblyStart = DateTime.now(DateTimeZone.UTC);

                        // downloads the catalogue file from vo tools - this is done inline
                        voToolsCataloguePackager.generateCatalogueAndChecksumFile(cacheManager.getJobDirectory(job),
                                (CatalogueDownloadFile) requiredFile, jobExpiryDate);
                        // update the file size- also sets cached file to available
                        long filesizeKb = cacheManager.updateSizeForCachedFile(job, requiredFile);
                        requiredFile.setSizeKb(filesizeKb);
                        requiredFile.setComplete(true);

                        DateTime fileAssemblyEnd = DateTime.now(DateTimeZone.UTC);
                        long duration = fileAssemblyEnd.getMillis() - fileAssemblyStart.getMillis();

                        logger.info(CasdaDataAccessEvents.E134.messageBuilder().addTimeTaken(duration)
                                .add(CasdaFormatter.formatDateTimeForLog(fileAssemblyStart.toDate()))
                                .add(CasdaFormatter.formatDateTimeForLog(fileAssemblyEnd.toDate()))
                                .add(DataLocation.VO_TOOLS).add(DataLocation.DATA_ACCESS).add(requiredFile.getSizeKb())
                                .add(requiredFile.getFileId()).toString());
                    }
                    else if (requiredFile.getFileType() == FileType.IMAGE_CUTOUT)
                    {
                        if (cacheManager.isCachedFileAvailable(requiredFile))
                        {
                            // update the file size- also sets cached file to available
                            long filesizeKb = cacheManager.updateSizeForCachedFile(job, requiredFile);
                            createChecksumFile(new File(cacheManager.getCachedFile(requiredFile.getFileId()).getPath()));
                            requiredFile.setSizeKb(filesizeKb);
                            requiredFile.setComplete(true);
                        }
                        else
                        {
                            allFilesAvailable = false;
                            /* check if the image is available */
                            CutoutFileDescriptor cutoutFileDescriptor = (CutoutFileDescriptor) requiredFile;
                            /*
                             * if the original file is ready to process, update the cached file table so it will be
                             * picked up
                             */
                            if (cutoutFileDescriptor.getOriginalImageFilePath() == null && cacheManager
                                    .isCachedFileAvailable(cutoutFileDescriptor.getOriginalImageDownloadFile()))
                            {
                                CachedFile imageForCutoutCachedFile = cacheManager
                                        .getCachedFile(cutoutFileDescriptor.getOriginalImageDownloadFile().getFileId());
                                cacheManager.updateOriginalFilePath(requiredFile, imageForCutoutCachedFile.getPath());
                                cutoutFileDescriptor.setOriginalImageFilePath(imageForCutoutCachedFile.getPath());
                            }
                        }
                    }
                    else if (cacheManager.isCachedFileAvailable(requiredFile))
                    {
                        requiredFile.setComplete(true);
                    }
                    else
                    {
                        allFilesAvailable = false;
                    }
                }
            }
            if (allFilesAvailable)
            {
                return;
            }
            logger.debug("Starting sleep for job request id {}", job.getRequestId());

            Thread.sleep(this.downloadSleepIntervalMillis);
        }
    }

    /**
     * Creates a checksum file for a given file. The destination will be file.checksum
     * 
     * @param file
     *            the file to calculate the checksum for
     * @throws CreateChecksumException
     *             if there is a problem creating the checksum file
     */
    protected void createChecksumFile(File file) throws CreateChecksumException
    {
        logger.debug("Creating checksum file for: {} exists: {}", file, file.exists());
        try
        {
            String response = inlineScriptService.callScriptInline(calculateChecksumScript, file.getCanonicalPath());
            if (StringUtils.isNotBlank(response))
            {
                FileUtils.writeStringToFile(new File(file.getCanonicalPath() + ".checksum"), response);
            }
            else
            {
                throw new CreateChecksumException(
                        "Script generated an empty checksum response for file: " + file.getCanonicalPath());
            }
        }
        catch (IOException | InlineScriptException e)
        {
            throw new CreateChecksumException(e);
        }
    }

}
