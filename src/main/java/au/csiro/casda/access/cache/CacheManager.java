package au.csiro.casda.access.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.transaction.Transactional;

import org.apache.commons.collections4.CollectionUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import au.csiro.casda.access.CutoutFileDescriptor;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
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
 * Manages a cache of data files. The cache consists of actual data files saved in date based folders and links to these
 * files saved in job folders. Each data file can be referenced from several jobs. Files are locked in the cache until
 * their unlock time kept in a database. Files are deleted only when space is required for new data. A file with
 * earliest unlock time is deleted first, together with all jobs that refer to it. All other files in these jobs are
 * deleted as well as there is no point to keeping incomplete job sets.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@Service
@Transactional
public class CacheManager implements CacheManagerInterface
{
    /** Location of the cache root directory */
    private final File homeDir;

    /** Jobs subdirectory */
    private final File jobsDir;

    /** Data subdirectory */
    private final File dataDir;

    private final long maxCacheSizeKb;

    /** Max number of attempts to make to download a file */
    private int maxDownloadAttempts;

    /** The size of the page of records to extract from the database */
    private static final int PAGE_SIZE = 5;

    private final CachedFileRepository cachedFileRepository;
    private final DataAccessJobRepository dataAccessJobRepository;

    private static Logger logger = LoggerFactory.getLogger(CacheManager.class);

    /**
     * @param maxCacheSizeKb
     *            maximum size of the cache in KB
     * @param maxDownloadAttempts
     *            max number of attempts to make to download a file
     * @param homeDirConf
     *            the base directory for the cache
     * @param cachedFileRepository
     *            to operate the cachedFile entity
     * @param dataAccessJobRepository
     *            to access info on data access jobs
     * @throws IllegalArgumentException
     *             if can't create cache data directory
     */
    @Autowired
    public CacheManager(@Value("${cache.max.size}") Long maxCacheSizeKb,
            @Value("${max.download.attempts}") int maxDownloadAttempts, @Value("${cache.home.dir}") String homeDirConf,
            CachedFileRepository cachedFileRepository, DataAccessJobRepository dataAccessJobRepository)
                    throws IllegalArgumentException
    {
        this.cachedFileRepository = cachedFileRepository;
        this.dataAccessJobRepository = dataAccessJobRepository;

        this.maxDownloadAttempts = maxDownloadAttempts;
        this.maxCacheSizeKb = maxCacheSizeKb;
        this.homeDir = new File(homeDirConf);

        if (!homeDir.exists() && !homeDir.mkdirs() || !homeDir.isDirectory())
        {
            throw new IllegalArgumentException(String.format("Can't create cache home directory: %s", homeDir));
        }
        jobsDir = new File(homeDir, "jobs");
        if (!jobsDir.exists() && !jobsDir.mkdirs() || !jobsDir.isDirectory())
        {
            throw new IllegalArgumentException(String.format("Can't create cache jobs directory: %s", jobsDir));
        }
        dataDir = new File(homeDir, "data");
        if (!dataDir.exists() && !dataDir.mkdirs() || !dataDir.isDirectory())
        {
            throw new IllegalArgumentException(String.format("Can't create cache data directory: %s", dataDir));
        }
    }

    /**
     * Creates a directory for new job
     * 
     * @param jobName
     *            name of the job to create the directory for
     * @return a new directory to store this jobs files
     * @throws IllegalArgumentException
     *             if there is a problem creating the job directory
     */
    private File createJobFolder(String jobName) throws IllegalArgumentException
    {
        File newDir = new File(jobsDir, jobName);
        if (!newDir.exists() && !newDir.mkdirs() || !newDir.isDirectory())
        {
            throw new IllegalArgumentException("Can't create job directory for job " + jobName);
        }
        logger.debug("Created job directory for job {}", jobName);
        return newDir;
    }

    @Override
    public boolean allFilesAvailableInCache(Collection<DownloadFile> files)
    {
        for (DownloadFile file : files)
        {
            CachedFile cfo = cachedFileRepository.findByFileId(file.getFileId());

            if (cfo == null || !cfo.isFileAvailableFlag())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public synchronized long reserveSpaceAndRegisterFilesForDownload(Collection<DownloadFile> files,
            DataAccessJob dataAccessJob) throws CacheException
    {
        List<CachedFile> filesToDownload = new ArrayList<>();

        long sizeCachedKb = 0L;
        Set<String> imageFileIdsAlreadyChecked = new HashSet<>();

        for (DownloadFile file : files)
        {
            CachedFile cachedFile = cachedFileRepository.findByFileId(file.getFileId());

            if (cachedFile == null)
            {
                CachedFile newFile = createCachedFile(dataAccessJob, file);
                filesToDownload.add(newFile);
            }
            else
            {
                sizeCachedKb += cachedFile.getSizeKb();
                resetCachedFileToDownloadIfFailedAndExtendExpiry(cachedFile);
            }

            /*
             * if it's an image cutout, we need to download the original image to the cache if it isn't available on
             * disk. we only need to check each image file once.
             */
            if (file.getFileType() == FileType.IMAGE_CUTOUT)
            {
                CutoutFileDescriptor cutoutFile = (CutoutFileDescriptor) file;
                if (cutoutFile.getOriginalImageFilePath() == null
                        && !imageFileIdsAlreadyChecked.contains(cutoutFile.getOriginalImageDownloadFile().getFileId()))
                {
                    imageFileIdsAlreadyChecked.add(cutoutFile.getOriginalImageDownloadFile().getFileId());
                    CachedFile imageCachedFile =
                            cachedFileRepository.findByFileId(cutoutFile.getOriginalImageDownloadFile().getFileId());

                    if (imageCachedFile == null)
                    {
                        CachedFile newFile = createCachedFile(dataAccessJob, cutoutFile.getOriginalImageDownloadFile());
                        boolean alreadyListed = false;
                        for (CachedFile downloadFile : filesToDownload)
                        {
                            if (downloadFile.getFileId().equals(newFile.getFileId()))
                            {
                                alreadyListed = true;
                                break;
                            }
                        }
                        if (!alreadyListed)
                        {
                            filesToDownload.add(newFile);
                        }
                    }
                    else
                    {
                        sizeCachedKb += imageCachedFile.getSizeKb();
                        resetCachedFileToDownloadIfFailedAndExtendExpiry(imageCachedFile);
                    }
                }
            }
        }

        if (CollectionUtils.isEmpty(filesToDownload))
        {
            logger.debug("All files are in the cache");
            return sizeCachedKb;
        }

        long freeSpace = maxCacheSizeKb - getUsedCacheSizeKb();
        long canRelease = cachedFileRepository.sumUnlockedCachedFileSize(DateTime.now(DateTimeZone.UTC)).orElse(0l);
        long sizeRequired = filesToDownload.stream().mapToLong(file -> file.getSizeKb()).sum();

        logger.debug("Reserve space, size required={} free space={}", sizeRequired, freeSpace);

        /* if there is no space available throw exception */
        long availableSpace = freeSpace + canRelease;
        if (availableSpace < sizeRequired)
        {
            throw new CacheFullException(
                    String.format("Insufficient space in the cache, needed: %d kb, available: %d kb", sizeRequired,
                            availableSpace >= 0 ? availableSpace : 0));
        }

        /* while we still need space - remove files */
        Pageable pageable = new PageRequest(0, PAGE_SIZE);
        while (freeSpace < sizeRequired)
        {
            Page<CachedFile> cfo =
                    cachedFileRepository.findCachedFilesToUnlock(DateTime.now(DateTimeZone.UTC), pageable);
            if (cfo.getNumberOfElements() > 0)
            {
                for (CachedFile cf : cfo.getContent())
                {
                    try
                    {
                        logger.debug("Removing file {} will release {}KB", cf.getFileId(), cf.getSizeKb());
                        long size = cf.getSizeKb();
                        this.removeCachedFile(cf.getPath());
                        this.removeJobsUsingFile(cf);
                        cachedFileRepository.delete(cf);
                        freeSpace += size;
                    }
                    catch (IOException ioe)
                    {
                        throw new CacheException("Unable to remove file from cache: " + cf.getPath(), ioe);
                    }
                }
            }
            else
            {
                /*
                 * should not have got this far, if there are no more files that are unlocked, but we still need space
                 */
                throw new CacheFullException("Out of space. No more files to remove.");
            }
            pageable.next();
        }

        /* add all the new cached files */
        cachedFileRepository.save(filesToDownload);

        return sizeCachedKb;
    }

    private CachedFile createCachedFile(DataAccessJob dataAccessJob, DownloadFile file) throws CacheException
    {
        CachedFile newFile = new CachedFile();
        newFile.setFileId(file.getFileId());
        newFile.setSizeKb(file.getSizeKb());
        newFile.setFileAvailableFlag(false);
        newFile.setFileType(file.getFileType());
        if (file.getFileType() == FileType.CATALOGUE || file.getFileType() == FileType.IMAGE_CUTOUT)
        {
            String destination = new File(getJobDirectory(dataAccessJob), file.getFilename()).getAbsolutePath();
            newFile.setPath(destination);

            // Make sure the job folder is ready for files.
            createJobFolder(dataAccessJob.getRequestId());
        }
        else
        {
            String destination = new File(this.getCurrentDateDir(), file.getFilename()).getAbsolutePath();
            newFile.setPath(destination);
        }

        if (file.getFileType() == FileType.IMAGE_CUTOUT)
        {
            CutoutFileDescriptor cutoutFileDescriptor = (CutoutFileDescriptor) file;
            if (cutoutFileDescriptor.getOriginalImageFilePath() != null)
            {
                newFile.setOriginalFilePath(cutoutFileDescriptor.getOriginalImageFilePath());
            }
        }

        newFile.setUnlock(DateTime.now(DateTimeZone.UTC).plusWeeks(1));
        return newFile;
    }

    private void resetCachedFileToDownloadIfFailedAndExtendExpiry(CachedFile cachedFile)
    {
        if (!cachedFile.isFileAvailableFlag() && cachedFile.getDownloadJobRetryCount() > maxDownloadAttempts)
        {
            /* if the file failed, set the retry count to 0 so it will retry download */
            cachedFile.setDownloadJobRetryCount(0);
        }
        /* reclaim the file, and update the expiry so it doesn't get deleted later in this method */
        cachedFile.setUnlock(DateTime.now(DateTimeZone.UTC).plusWeeks(1));
        cachedFileRepository.save(cachedFile);
    }

    /**
     * @param fileName
     *            to remove from the cache (also removes its checksum)
     * @throws IOException
     *             if there is a problem removing the file or its checksum
     */
    protected void removeCachedFile(String fileName) throws IOException
    {
        logger.debug("Removing {}", fileName);

        /*
         * if the file doesn't exist (which could be the case with cached files catalogue files if a job is deleted),
         * don't try to delete it
         */
        Files.deleteIfExists(Paths.get(fileName));
        Files.deleteIfExists(Paths.get(fileName + ".checksum"));

        Path parentDirectory = Paths.get(fileName).getParent();
        if (isDirectoryEmpty(parentDirectory))
        {
            Files.deleteIfExists(parentDirectory);
        }
    }

    private boolean isDirectoryEmpty(Path directory)
    {
        if (Files.isDirectory(directory))
        {
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(directory))
            {
                return !dirStream.iterator().hasNext();
            }
            catch (IOException e)
            {
                logger.warn("There was a problem trying to check whether directory is empty", e);
            }
        }
        return false;
    }

    /**
     * Remove the links made to a file in the cache from the directories of the jobs using it. Also remove the job
     * directories using this file - as those jobs directories are no longer complete.
     * 
     * @param cachedFile
     *            the file to be removed
     * @throws IOException
     *             if a problem occurs removing file links
     */
    protected void removeJobsUsingFile(CachedFile cachedFile) throws IOException
    {
        if (cachedFile.getDataAccessJobs() != null)
        {
            for (DataAccessJob job : cachedFile.getDataAccessJobs())
            {
                File jobDir = new File(jobsDir, job.getRequestId());
                if (jobDir.exists())
                {
                    logger.debug("Removing job directory {}", jobDir.getCanonicalPath());
                    File[] links = jobDir.listFiles();
                    for (File link : links)
                    {
                        if (link.exists() && !link.delete())
                        {
                            throw new IOException("Could not delete file" + link.getAbsolutePath());
                        }
                    }
                    jobDir.delete();
                }
            }
        }
    }

    /**
     * @return the current day directory
     * @throws CacheException
     *             if the directory cannot be created
     */
    protected File getCurrentDateDir() throws CacheException
    {
        String dirString = dataDir.getAbsolutePath() + File.separatorChar
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        File dir = new File(dirString);
        if (!dir.exists() && !dir.mkdirs() || !dir.isDirectory())
        {
            throw new CacheException("Can't create output directory " + dirString);
        }
        return dir;
    }

    @Override
    public boolean isCachedFileAvailable(DownloadFile requiredFile) throws CacheException
    {
        CachedFile cachedFile = getCachedFile(requiredFile.getFileId());
        if (cachedFile == null)
        {
            throw new CacheException("Space hasn't been reserved for file " + requiredFile.getFileId());
        }

        if (cachedFile.getDownloadJobRetryCount() > maxDownloadAttempts)
        {
            throw new CacheException(
                    "Maximum number of download attempts failed for file: " + requiredFile.getFileId());
        }
        return cachedFile.isFileAvailableFlag();
    }

    @Override
    public void createSymLink(String requestId, File file, boolean checksum) throws CacheException
    {
        this.createJobDirectoryFileLink(requestId, file);

        if (checksum)
        {
            File checksumFile = new File(file.getAbsolutePath() + ".checksum");
            this.createJobDirectoryFileLink(requestId, checksumFile);
        }
    }

    @Override
    public CachedFile getCachedFile(String fileId)
    {
        return cachedFileRepository.findByFileId(fileId);
    }

    @Override
    public void linkJob(DataAccessJob job, CachedFile cachedFileParam, File savedFile)
            throws CacheException
    {
        /* get fresh copy of the cachedFile */
        CachedFile cachedFile = cachedFileRepository.findOne(cachedFileParam.getId());
        cachedFile.addJob(job);
        logger.debug("Added job {} to {}", job, cachedFile);
        cachedFileRepository.save(cachedFile);
    }
    
    @Override
    public void clearCacheIfPossible(CachedFile cachedFile)
    {
        cachedFileRepository.delete(cachedFile);
    }

    @Override
    public void updateUnlockForFiles(Collection<DownloadFile> files, DateTime newUnlock)
    {
        for (DownloadFile file : files)
        {
            CachedFile cachedFile = this.getCachedFile(file.getFileId());
            if (cachedFile != null)
            {
                /*
                 * Update the unlock time to whichever is latest of the given unlock time or the latest time required
                 * for an existing job
                 */
                DateTime latestJobExpiry = dataAccessJobRepository.findLatestJobExpiryForCachedFile(cachedFile.getId());

                if (latestJobExpiry != null && latestJobExpiry.isAfter(newUnlock))
                {
                    newUnlock = latestJobExpiry;
                }

                cachedFile.setUnlock(newUnlock);

                cachedFileRepository.save(cachedFile);
            }
        }

    }

    /**
     * Creates the linked file between the cache data directory and the job directory
     * 
     * @param requestId
     *            the request id for this job, used for results directory
     * @param savedFile
     *            the file stored in the cache data directory
     * @throws CacheException
     *             if there is a problem creating the linked files
     */
    private void createJobDirectoryFileLink(String requestId, File savedFile) throws CacheException
    {
        File jobDir = this.createJobFolder(requestId);
        Path from = Paths.get(jobDir.getAbsolutePath(), savedFile.getName());
        Path target = Paths.get(savedFile.getAbsolutePath());
        try
        {
            logger.debug("Linking {} to {}", from, target);
            Files.createLink(from, target);
        }
        catch (IOException ioe)
        {
            throw new CacheException(
                    String.format("Unable to create link from: %s, to %s.", from.toString(), target.toString()), ioe);
        }

    }

    @Override
    public long getMaxCacheSizeKb()
    {
        return maxCacheSizeKb;
    }

    @Override
    public long getUsedCacheSizeKb()
    {
        return cachedFileRepository.sumCachedFileSize().orElse(0l);
    }

    @Override
    public File getJobDirectory(DataAccessJob dataAccessJob)
    {
        return new File(jobsDir, dataAccessJob.getRequestId());
    }

    @Override
    public Long updateSizeForCachedFile(DataAccessJob job, DownloadFile downloadFile) throws CacheException
    {
        CachedFile cachedFile = cachedFileRepository.findByFileId(downloadFile.getFileId());
        File file = new File(getJobDirectory(job), downloadFile.getFilename());

        if (cachedFile == null)
        {
            throw new CacheException("Cannot update size for cached file id " + downloadFile.getFileId()
                    + ", CachedFile record does not exist");
        }
        if (!file.exists())
        {
            throw new CacheException("Cannot update size for cached file id " + downloadFile.getFileId()
                    + ", file does not exist at " + file.getAbsolutePath());
        }
        Long sizeKb = DataAccessUtil.convertBytesToKb(file.length());
        cachedFile.setSizeKb(sizeKb);
        cachedFile.setFileAvailableFlag(true);
        cachedFileRepository.save(cachedFile);
        return sizeKb;
    }

    @Override
    public void updateOriginalFilePath(DownloadFile downloadFile, String path) throws CacheException
    {
        CachedFile cachedFile = cachedFileRepository.findByFileId(downloadFile.getFileId());

        if (cachedFile == null)
        {
            throw new CacheException("Cannot update size for cached file id " + downloadFile.getFileId()
                    + ", CachedFile record does not exist");
        }
        if (!new File(path).exists())
        {
            throw new CacheException("Cannot update size for cached file id " + downloadFile.getFileId()
                    + ", file does not exist at " + path);
        }
        cachedFile.setOriginalFilePath(path);
        cachedFileRepository.save(cachedFile);
    }

    @Override
    public void createDataAccessJobDirectory(DataAccessJob job, Collection<DownloadFile> files)
            throws CacheException
    {
        EnumSet<FileType> generatedFileTypes = EnumSet.of(FileType.CATALOGUE, FileType.IMAGE_CUTOUT);

        for (DownloadFile requiredFile : files)
        {
            CachedFile cachedFile = getCachedFile(requiredFile.getFileId());
            if (cachedFile == null)
            {
                throw new CacheException("File was not saved to the cache: " + requiredFile.getFileId());
            }

            logger.debug("File {} is in the cache.", requiredFile.getFileId());
            File savedFile = new File(cachedFile.getPath());
            /* link to the file and checksum file, and record the job and the new expiry against the CachedFile */
            linkJob(job, cachedFile, savedFile);
            if (!generatedFileTypes.contains(requiredFile.getFileType()))
            {
                createSymLink(job.getRequestId(), savedFile, true);
            }
        }
    }

}
