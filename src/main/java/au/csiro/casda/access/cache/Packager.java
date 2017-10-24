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
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.EncapsulatedFileDescriptor;
import au.csiro.casda.access.ErrorFileDescriptor;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.GeneratedFileDescriptor;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.rest.CatalogueRetrievalException;
import au.csiro.casda.access.rest.CreateChecksumException;
import au.csiro.casda.access.rest.VoToolsCataloguePackager;
import au.csiro.casda.access.services.DataAccessService;
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

    private DownloadManager downloadManager;

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
     * @param downloadManager
     *            The service instance to manage retrieving and producing files.
     */
    @Autowired
    public Packager(CacheManagerInterface cacheManager, VoToolsCataloguePackager voToolsCataloguePackager,
            DataAccessService dataAccessService, @Value("${download.sleep.interval}") int downloadSleepInterval,
            DownloadManager downloadManager)
    {
        this.cacheManager = cacheManager;
        this.voToolsCataloguePackager = voToolsCataloguePackager;
        this.dataAccessService = dataAccessService;
        this.downloadSleepIntervalMillis = downloadSleepInterval;
        this.downloadManager = downloadManager;
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
        List<Map<FileType, Integer[]>> paging = dataAccessService.getPaging(job.getRequestId(), false);
        
        if (CollectionUtils.isEmpty(paging))
        {
            // return now - no need to go further if there are no files to download.
            return new Result(DateTime.now(DateTimeZone.UTC), 0, 0);
        }
        
        DateTime jobExpiryDate = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob);
        long sizeKbOfThisJob = 0;
        long sizeInCacheKb = 0;
        
        for(int pageNum = 0; pageNum < paging.size(); pageNum++)
        {
            logger.info("Packing page "+ pageNum + " for request " + job.getRequestId());
        	List<DownloadFile> filesPage = dataAccessService.getPageOfFiles(paging.get(pageNum), job);

            logger.debug("Page "+ pageNum + " retrieved for request " + job.getRequestId());
        	
            Collection<DownloadFile> files = assembleDataAccessJobDownloadFiles(filesPage);

            logger.debug("Files assembled for "+ pageNum + " for request " + job.getRequestId());
            
            // If it is possible to release enough space, old files will be irreversibly deleted.
            // This method will register the files that need to be downloaded in the CachedFile table as placeholders, 
            // so we can make accurate size estimations. These will be identified as running jobs by the DownloadManager
            //which runs a scheduled task to start and check download jobs. This method sets a default expiry time on 
            // the files to now + one week.
            Object[] sizeAndCacheFiles = cacheManager.reserveSpaceAndRegisterFilesForDownload(files, job);
            sizeInCacheKb += (Long) sizeAndCacheFiles[0];
            List<CachedFile> filesToDownload = (List<CachedFile>) sizeAndCacheFiles[1];

            // Notify DMF about the files that we are intending to bring online
            // This will block until the files are retrieved from tape
            if (CollectionUtils.isNotEmpty(filesToDownload))
            {
                String filePaths = buildUniqueFilePaths(filesToDownload);
                if (StringUtils.isNotBlank(filePaths))
                {
                    dataAccessService.signalDownloadFilesToGoOnline(filePaths);
                }
            }   
                   
            pollUntilFileDownloadComplete(job, files, hoursToExpiryForJob);

            cacheManager.createDataAccessJobDirectory(job, files);

            cacheManager.updateUnlockForFiles(files, jobExpiryDate);
            logger.debug("Unlock update completed for "+ pageNum + " for request " + job.getRequestId());
            dataAccessService.updateFileSizeForGeneratedFiles(files);
            logger.debug("File size update completed for "+ pageNum + " for request " + job.getRequestId());
            sizeKbOfThisJob += files.stream().mapToLong(file -> file.getSizeKb()).sum();
            logger.debug("Pack loop completed for "+ pageNum + " for request " + job.getRequestId());
        }
        
        job.setSizeKb(sizeKbOfThisJob);
        dataAccessService.saveJob(job);

        logger.info("Pack completed for request " + job.getRequestId());
        
        return new Result(jobExpiryDate, sizeInCacheKb, sizeKbOfThisJob);
    }

    private String buildUniqueFilePaths(List<CachedFile> filesToDownload)
    {
        Set<String> filePathsSet = new HashSet<>();
        for (CachedFile cachedFile : filesToDownload)
        {
            if (!EnumSet.of(FileType.CATALOGUE, FileType.ERROR).contains(cachedFile.getFileType()))
            {
                filePathsSet.add(cachedFile.getOriginalFilePath());
            }
        }
        String filePaths = StringUtils.join(filePathsSet, " ");
        return filePaths;
    }

    /**
     * Assembles the download file details for the files requested by this data access job.
     * 
     * @param files the list of files to assemble
     * @return the details of the files requested for download.
     * @throws ResourceNotFoundException
     *             if the job's file type was an IMAGE_CUTOUT and the source file could not be found in NGAS
     */
    Collection<DownloadFile> assembleDataAccessJobDownloadFiles(Collection<DownloadFile> files) 
    		throws ResourceNotFoundException
    {
        for (DownloadFile downloadFile : files)
        {
            if (downloadFile.getFileType() == FileType.CATALOGUE)
            {
                long sizeKb = voToolsCataloguePackager.estimateFileSizeKb((CatalogueDownloadFile) downloadFile);
                downloadFile.setSizeKb(sizeKb);
            }
            else if (downloadFile.isGeneratedFileType())
            {
                GeneratedFileDescriptor generatedDownloadFile = (GeneratedFileDescriptor) downloadFile;
                // if the file is on disk, use it - otherwise we will rely on getting it from the cache
                try
                {
                    Path filePathOnDisk = dataAccessService
                            .findFileInNgas(generatedDownloadFile.getOriginalImageDownloadFile().getFileId());
                    if (filePathOnDisk != null)
                    {
                    	generatedDownloadFile.setOriginalImageFilePath(filePathOnDisk.toFile().getAbsolutePath());
                    }
                }
                catch (ServiceCallException e)
                {
                    logger.error("There was a problem accessing ngas, will attempt to download to cache", e);
                }
            }
            //if the encapsulation file is null, then this spectrum/moment map is from before encapsulation and follows
            //the generic file path below. this is not possible for cubelets as they post-date this change
            else if((downloadFile.isEncapsulatedType()) && 
            		((EncapsulatedFileDescriptor)downloadFile).getEncapsulationFile() != null)
            {
            	EncapsulatedFileDescriptor encapsulatedDownloadFile = (EncapsulatedFileDescriptor) downloadFile;
            	
                try
                {
                    Path filePathOnDisk = dataAccessService
                            .findFileInNgas(encapsulatedDownloadFile.getEncapsulationFile().getFileId());
                    if (filePathOnDisk != null)
                    {
                        encapsulatedDownloadFile
                                .setOriginalEncapsulationFilePath(filePathOnDisk.toFile().getAbsolutePath());
                    }
                }
                catch (ServiceCallException e)
                {
                    logger.error("There was a problem accessing ngas, will attempt to download to cache", e);
                }
            }
            else if (downloadFile.getFileType() != FileType.ERROR)
            {
                FileDescriptor dFile = (FileDescriptor) downloadFile;
                try
                {
                    Path filePathOnDisk = dataAccessService.findFileInNgasIfOnDisk(dFile.getFileId());
                    if (filePathOnDisk != null)
                    {
                        dFile.setOriginalFilePath(filePathOnDisk.toFile().getAbsolutePath());
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
        long startTime = (new Date()).getTime();
        int numLoops = 0;
        Map<String, CachedFile> retrievedParentFiles = new HashMap<>();

        while (true)
        {
            logger.info("Polling for downloaded files for job request id {}", job.getRequestId());
            numLoops++;

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
                    else if (requiredFile.getFileType() == FileType.IMAGE_CUTOUT
                            || requiredFile.getFileType() == FileType.GENERATED_SPECTRUM)
                    {
                        GeneratedFileDescriptor generatedFileDescriptor = (GeneratedFileDescriptor) requiredFile;
                        if (generatedFileDescriptor.getOriginalImageFilePath() == null)
                        {
                            String parentFileId = generatedFileDescriptor.getOriginalImageDownloadFile().getFileId();
                            CachedFile parentCachedFile = retrievedParentFiles.containsKey(parentFileId)
                                    ? retrievedParentFiles.get(parentFileId) : cacheManager.getCachedFile(parentFileId);
                            if (!parentCachedFile.isFileAvailableFlag())
                            {
                                downloadManager.pollJobManagerForDownloadJob(parentCachedFile);
                            }
                            if (parentCachedFile.isFileAvailableFlag())
                            {
                                cacheManager.updateOriginalFilePath(requiredFile, parentCachedFile.getPath());
                                generatedFileDescriptor.setOriginalImageFilePath(parentCachedFile.getPath());
                                retrievedParentFiles.put(parentFileId, parentCachedFile);
                            }
                            else
                            {
                                allFilesAvailable = false;
                                continue;
                            }
                        }

                        allFilesAvailable = checkFileAvailable(allFilesAvailable, requiredFile);
                    }
                    else if (requiredFile.isEncapsulatedType() 
                    		&& ((EncapsulatedFileDescriptor) requiredFile).getEncapsulationFile() != null)
                    {
                        EncapsulatedFileDescriptor encapsulatedFileDescriptor =
                                (EncapsulatedFileDescriptor) requiredFile;
                        if (encapsulatedFileDescriptor.getOriginalEncapsulationFilePath() == null)
                        {
                            String parentFileId = encapsulatedFileDescriptor.getEncapsulationFile().getFileId();
                            CachedFile encapsulationCachedFile = retrievedParentFiles.containsKey(parentFileId)
                                    ? retrievedParentFiles.get(parentFileId) : cacheManager.getCachedFile(parentFileId);
                            if (!encapsulationCachedFile.isFileAvailableFlag())
                            {
                                downloadManager.pollJobManagerForDownloadJob(encapsulationCachedFile);
                            }
                            if (encapsulationCachedFile.isFileAvailableFlag())
                            {
                                cacheManager.updateOriginalFilePath(requiredFile, encapsulationCachedFile.getPath());
                                encapsulatedFileDescriptor
                                        .setOriginalEncapsulationFilePath(encapsulationCachedFile.getPath());
                                retrievedParentFiles.put(parentFileId, encapsulationCachedFile);
                            }
                            else
                            {
                                allFilesAvailable = false;
                                continue;
                            }
                        }

                        allFilesAvailable = checkFileAvailable(allFilesAvailable, requiredFile);
                    }
                    else if (requiredFile.getFileType() == FileType.ERROR)
                    {
                        writeErrorFileAndChecksum(cacheManager.getJobDirectory(job),
                                (ErrorFileDescriptor) requiredFile);
                        long filesizeKb = cacheManager.updateSizeForCachedFile(job, requiredFile);
                        requiredFile.setSizeKb(filesizeKb);
                        requiredFile.setComplete(true);
                    }
                    else
                    {
                        allFilesAvailable = checkFileAvailable(allFilesAvailable, requiredFile);
                    }
                }
            }
            if (allFilesAvailable)
            {
                long endTime = (new Date()).getTime();
                logger.info("All files available for page of job request id {} after {} loops taking {} ms",
                        job.getRequestId(), numLoops, (endTime - startTime));
                return;
            }
            logger.debug("Starting sleep for job request id {}", job.getRequestId());

            Thread.sleep(this.downloadSleepIntervalMillis);
        }
    }

    private boolean checkFileAvailable(boolean allFilesAvailable, DownloadFile requiredFile) throws CacheException
    {
        CachedFile cachedFile = cacheManager.getCachedFile(requiredFile.getFileId());
        if (cachedFile == null)
        {
            throw new CacheException("Space hasn't been reserved for file " + requiredFile.getFileId());
        }

        downloadManager.pollJobManagerForDownloadJob(cachedFile);
        if (cachedFile.isFileAvailableFlag())
        {
            requiredFile.setComplete(true);
        }
        else
        {
            allFilesAvailable = false;
        }
        return allFilesAvailable;
    }

    /**
     * Check if the specified cache file has finished processing and is available in the cache. This will prompt 
     * scheduling of the job if it is not already in the queue.
     *  
     * @param cachedFile The file to be retrieved or generated.
     * @return True if the file is in the cache, false if it is still being retrieved.
     * @throws CacheException When the file cannot be retrieved or generated.
     */
    public boolean checkFileAvailable(CachedFile cachedFile) throws CacheException
    {
        downloadManager.pollJobManagerForDownloadJob(cachedFile);
        return cachedFile.isFileAvailableFlag();
    }

    /**
     * Write the error message to a file along with a checksum. The file will be named according to the fileId of the
     * errorFileDesc.
     * 
     * @param jobDir
     *            The folder to write the files to.
     * @param errorFileDesc
     *            The description of the error file and message.
     * @throws CacheException
     *             If the error file cannot be written.
     * @throws CreateChecksumException
     *             If the checksum file cannot be written.
     */
    void writeErrorFileAndChecksum(File jobDir, ErrorFileDescriptor errorFileDesc)
            throws CacheException, CreateChecksumException
    {
        logger.debug("Creating error file for: {}", errorFileDesc.getFilename());
        try
        {
            File file = new File(jobDir, errorFileDesc.getFilename());
            File errorFile = new File(file.getCanonicalPath());
            FileUtils.writeStringToFile(errorFile, errorFileDesc.getErrorMessage());
            downloadManager.createChecksumFile(errorFile);
        }
        catch (IOException e)
        {
            throw new CacheException("Unable to write error file " + errorFileDesc.getFilename(), e);
        }
    }

}
