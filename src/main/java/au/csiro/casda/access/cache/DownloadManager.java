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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import au.csiro.casda.Utils;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.siap2.CutoutBounds;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.jobmanager.CasdaToolProcessJobBuilder;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.jobmanager.ProcessJob;
import au.csiro.casda.jobmanager.ProcessJobBuilder;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import au.csiro.casda.jobmanager.SimpleToolProcessJobBuilder;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;

/**
 * Manages download of files from NGAS, including starting jobs and updating the CachedFile records.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@Component
@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
public class DownloadManager
{
    /**
     * Types of process jobs.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static enum ProcessJobType
    {
        /**
         * Represents a simple process job
         */
        SIMPLE,

        /**
         * Represents a CASDA tool process job
         */
        CASDA_TOOL
    };

    private static final Logger logger = LoggerFactory.getLogger(DownloadManager.class);

    /** The size of the page of records to extract from the database */
    private static final int PAGE_SIZE = 5;

    /** The command name of the NGAS downloader tool. */
    public static final String NGAS_DOWNLOADER_TOOL_NAME = "ngas_download";

    private CachedFileRepository cachedFileRepository;

    private ImageCutoutRepository imageCutoutRepository;

    private JobManager jobManager;

    private final int maxDownloadAttempts;

    private String[] cutoutCommand;

    private ProcessJobFactory processJobFactory;

    private String depositToolsWorkingDirectory;

    private String downloadCommand;

    private ProcessJobType downloadCommandType;

    private String downloadCommandArgs;

    private CasdaToolProcessJobBuilderFactory casdaToolBuilderFactory;

    /**
     * Constructor
     * 
     * @param cachedFileRepository
     *            the cached file repository
     * @param imageCutoutRepository
     *            the image cutout repository
     * @param jobManager
     *            the job manager
     * @param depositToolsWorkingDirectory
     *            the location for the data deposit working directory (for starting ngas download jobs)
     * @param maxDownloadAttempts
     *            the maximum number of attempts allowed to download a file
     * @param casdaToolProcessJobBuilderFactory
     *            a CasdaToolProcessJobBuilderFactory instance used to build a ProcessJobBuilder configured with Casda
     *            Tool settings
     * @param downloadCommandType
     *            whether the download command is "SIMPLE" or a "CASDA_TOOL"
     * @param downloadCommand
     *            the download command to be used to download a data product from the archive if it is not available in
     *            the cache - if downloadCommandType is SIMPLE then this should include both command and args as a list
     *            in Spring EL format, otherwise it should be a simple string representing the name of a CASDA tool
     * @param downloadCommandArgs
     *            the download command args to be used to download a data product from the archive if it is not - if
     *            downloadCommandType is SIMPLE then this should be null, otherwise it should be a list in Spring EL
     *            format representing the args to the CASDA tool
     * @param cutoutCommand
     *            the command and args to be used to produce a cutout (as a list in Spring EL format)
     * @param processJobFactory
     *            the factory to be used to create job processes.
     */
    @Autowired
    public DownloadManager(CachedFileRepository cachedFileRepository, ImageCutoutRepository imageCutoutRepository,
            JobManager jobManager, @Value("${deposit.tools.working.directory}") String depositToolsWorkingDirectory,
            @Value("${max.download.attempts}") int maxDownloadAttempts,
            CasdaToolProcessJobBuilderFactory casdaToolProcessJobBuilderFactory,
            @Value("${download.command.type}") String downloadCommandType,
            @Value("${download.command}") String downloadCommand, @Value("${download.args}") String downloadCommandArgs,
            @Value("${cutout.command}") String cutoutCommand, ProcessJobFactory processJobFactory)
    {
        this.cachedFileRepository = cachedFileRepository;
        this.imageCutoutRepository = imageCutoutRepository;
        this.jobManager = jobManager;
        this.depositToolsWorkingDirectory = depositToolsWorkingDirectory;
        this.downloadCommand = downloadCommand;
        this.downloadCommandType = ProcessJobType.valueOf(downloadCommandType);
        this.downloadCommandArgs = downloadCommandArgs;
        this.maxDownloadAttempts = maxDownloadAttempts;
        this.cutoutCommand = Utils.elStringToArray(cutoutCommand);
        this.processJobFactory = processJobFactory;
        this.casdaToolBuilderFactory = casdaToolProcessJobBuilderFactory;
        /*
         * Perform a sanity check on the download command args by trying to create a ProcessJobBuilder
         */
        createProcessJobBuilderForProcessJobType(this.downloadCommandType, this.downloadCommand,
                this.downloadCommandArgs);
    }

    /**
     * Polls the CachedFile table for downloading files, starts/restarts downloading if required, checks the status of
     * running and completed jobs and updates the CachedFile table.
     */
    @Scheduled(fixedDelayString = "${download.sleep.interval}")
    public void pollJobManagerForDownloadJobs()
    {
        logger.debug("Polling for download jobs");

        Pageable pageable = new PageRequest(0, PAGE_SIZE);

        while (true)
        {
            Page<CachedFile> downloadingFiles =
                    cachedFileRepository.findDownloadingCachedFiles(maxDownloadAttempts, pageable);

            if (!downloadingFiles.hasContent())
            {
                break;
            }

            for (CachedFile downloadingFile : downloadingFiles.getContent())
            {
                pollJobManagerForDownloadJob(downloadingFile);

            }

            if (downloadingFiles.hasNext())
            {
                pageable.next();
            }
            else
            {
                break;
            }

        }
    }

    /**
     * Starts/restarts downloading a file if required, or checks the status if a download job is running or completed,
     * and updates the CachedFile table.
     * 
     * @param downloadingFile
     *            the file to download
     */
    protected void pollJobManagerForDownloadJob(CachedFile downloadingFile)
    {
        JobManager.JobStatus status = downloadingFile.getDownloadJobId() == null ? null
                : jobManager.getJobStatus(downloadingFile.getDownloadJobId());

        if (status == null)
        {
            // Job hasn't been started for this retry (e.g. throttled), so try kicking it off
            maybeStartDownloadJob(downloadingFile, status);
        }
        else if (status.isFailed())
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Job {} failed during with output: {}, and failure cause: {}");
            logger.error(builder.toString(), downloadingFile.getDownloadJobId(), status.getJobOutput(),
                    status.getFailureCause());
            retryDownloading(downloadingFile, status);
        }
        else if (status.isFinished())
        {
            File file = new File(downloadingFile.getPath());
            if (!file.exists())
            {
                // this shouldn't happen, but log it in case it does, and then treat it as if the download
                // failed
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add(String.format("Download job %s finished, but file %s isn't in the cache",
                        downloadingFile.getDownloadJobId(), downloadingFile.getFileId()));
                logger.warn(builder.toString());
                retryDownloading(downloadingFile, status);
            }
            else
            {
                logger.debug("Downloaded successfully: {}", downloadingFile.getFileId());
                downloadingFile.setFileAvailableFlag(true);
                this.cachedFileRepository.save(downloadingFile);
            }

        }
    }

    private void retryDownloading(CachedFile downloadingFile, JobManager.JobStatus status)
    {
        incrementRetryCount(downloadingFile);

        maybeStartDownloadJob(downloadingFile, status);
        // make sure the new file id is saved, regardless of the job start outcome.
        this.cachedFileRepository.save(downloadingFile);
    }

    private void incrementRetryCount(CachedFile downloadingFile)
    {
        int retryCount = downloadingFile.getDownloadJobRetryCount();
        retryCount += 1;
        downloadingFile.setDownloadJobRetryCount(retryCount);
    }

    /**
     * Attempt to start a download job for the file. This may be rejected if too many retries of failures have occurred
     * before.
     * <p>
     * As a throttling job manager is used, the attempt to start may also be ignored if the system has too many files
     * already being downloaded. In that case, the job status will remain null and it is assumed that callers will not
     * increment the retry count.
     * <p>
     * If the job manager tries to start the job but fails, the retry count will be incremented.
     * 
     * @param downloadingFile
     *            The CachedFile to be retrieved.
     * @param status
     *            The job status of the previous attempt to start the job.
     */
    private void maybeStartDownloadJob(CachedFile downloadingFile, JobManager.JobStatus status)
    {
        if (downloadingFile.getDownloadJobRetryCount() > maxDownloadAttempts)
        {
            // log a warning, and update the retry count. This won't be picked up as downloading again
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add(String.format("All download attempts failed for file %s", downloadingFile.getFileId()));
            logger.warn(builder.toString());
            cachedFileRepository.save(downloadingFile);
            return;
        }

        String downloadJobId = buildDownloadJobId(downloadingFile.getFileId(), System.currentTimeMillis(),
                downloadingFile.getDownloadJobRetryCount());
        downloadingFile.setDownloadJobId(downloadJobId);

        ProcessJob downloadingJob;
        if (FileType.IMAGE_CUTOUT == downloadingFile.getFileType())
        {
            downloadingJob = buildCutoutJob(downloadJobId, downloadingFile);
        }
        else
        {
            downloadingJob = buildDownloadJob(downloadJobId, downloadingFile.getFileId(), downloadingFile.getPath(),
                    downloadingFile.getFileType());
        }

        try
        {
            // When the job manager is throttled it ignores attempts to start jobs past the configured maximum
            jobManager.startJob(downloadingJob);
            if (jobManager.getJobStatus(downloadJobId) != null)
            {
                // Job was started so log the event.
                if (status != null)
                {
                    logger.info(
                            "Retried download of file {} to {} with job id {}, "
                                    + "after job returned with status of {}.",
                            downloadingFile.getFileId(), downloadingFile.getPath(), downloadJobId, status);
                }
                else
                {
                    logger.info("Started download of file {} to {} with job id {}", downloadingFile.getFileId(),
                            downloadingFile.getPath(), downloadJobId);
                }
                this.cachedFileRepository.save(downloadingFile);
            }
        }
        catch (RuntimeException e)
        {
            logger.warn("Couldn't start download job for cached file id {} with job id {}, will retry",
                    downloadingFile.getFileId(), downloadJobId, e);
            // Bump up the retry count so we don't retry in an endless loop
            incrementRetryCount(downloadingFile);
            this.cachedFileRepository.save(downloadingFile);
        }
    }

    /**
     * Builds the download job id for slurm - this uses the time it's started for uniqueness, because jobs with the same
     * file id and retry count may occur if a download is restarted.
     * 
     * @param fileId
     *            the NGAS file id for the file to download
     * @param timeMillis
     *            the time the process was created, in millis
     * @param retryCount
     *            the number of times we have tried to download this file
     * @return String the job id, formatted as jobRequestId-fileId-timeMillis-retryCount
     */
    private String buildDownloadJobId(String fileId, long timeMillis, int retryCount)
    {
        return String.format("DataAccess-%s-%d-%d", fileId, timeMillis, retryCount);
    }

    /**
     * Build the job to download a file and its checksum from NGAS.
     * 
     * @param jobId
     *            the id for the job (running in slurm)
     * @param fileId
     *            the ngas id of the file to download
     * @param destination
     *            the path to the destination file
     * @param fileType
     *            the type of file to process
     * @return ProcessJob the job details
     */
    private ProcessJob buildDownloadJob(String jobId, String fileId, String destination, FileType fileType)
    {
        ProcessJobBuilder downloadProcessJobBuilder = createProcessJobBuilderForProcessJobType(this.downloadCommandType,
                this.downloadCommand, this.downloadCommandArgs);

        downloadProcessJobBuilder.setProcessParameter("fileId", fileId);
        downloadProcessJobBuilder.setProcessParameter("destination", destination);

        return downloadProcessJobBuilder.createJob(jobId, NGAS_DOWNLOADER_TOOL_NAME);
    }

    /**
     * Build up a ProcessJob which will produce a cutout.
     * 
     * @param jobId
     *            the id for the job (running in slurm)
     * @param downloadingFile
     *            The CachedFile object defining the cutout.
     * @return The job details.
     */
    ProcessJob buildCutoutJob(String jobId, CachedFile downloadingFile)
    {
        String dimKeys[] = new String[] {"-D3", "-D4"};
        List<String> commandParts = new ArrayList<>();
        commandParts.addAll(Arrays.asList(cutoutCommand));

        String sourcePath = downloadingFile.getOriginalFilePath();
        String[] fileIdDetails = downloadingFile.getFileId().split("-");
        ImageCutout cutout = imageCutoutRepository.findOne(Long.parseLong(fileIdDetails[1]));
        CutoutBounds cutoutBounds = new CutoutBounds(cutout.getBounds());

        for (int i = 0; i < dimKeys.length; i++)
        {
            // If there is no value for the parameter we need to remove the flag and the value from the command
            if (StringUtils.isEmpty(cutoutBounds.getDimBounds(i)))
            {
                int index = commandParts.indexOf(dimKeys[i]);
                commandParts.remove(index);
                commandParts.remove(index);
            }
        }
        SimpleToolProcessJobBuilder processBuilder =
                new SimpleToolProcessJobBuilder(this.processJobFactory, commandParts.toArray(new String[]{}));
        
        processBuilder.setProcessParameter("source_file", sourcePath);
        processBuilder.setProcessParameter("dest_file", downloadingFile.getPath());

        if (StringUtils.isNotEmpty(cutoutBounds.getDimBounds(0)))
        {
            processBuilder.setProcessParameter("dim3_range", cutoutBounds.getDimBounds(0));
        }
        if (StringUtils.isNotEmpty(cutoutBounds.getDimBounds(1)))
        {
            processBuilder.setProcessParameter("dim4_range", cutoutBounds.getDimBounds(1));
        }
        
        processBuilder.setProcessParameter("maxplane", String.valueOf(cutoutBounds.getMaxPlane()));
        processBuilder.setProcessParameter("ra", cutoutBounds.getRa());
        processBuilder.setProcessParameter("dec", cutoutBounds.getDec());
        processBuilder.setProcessParameter("xsize", cutoutBounds.getXSize());
        processBuilder.setProcessParameter("ysize", StringUtils.trimToEmpty(cutoutBounds.getYSize()));
        
        processBuilder.setWorkingDirectory(depositToolsWorkingDirectory);

        return processBuilder.createJob(jobId, FileType.IMAGE_CUTOUT.name());
    }

    /**
     * Creates a ProcessJobBuilder for a given job type and command.
     * 
     * @param commandType
     *            used to determine which type of builder to create - SIMPLE creates a SimpleToolProcessJobBuilder,
     *            CASDA_TOOL creates a CasdaToolProcessJobBuilder
     * @param command
     *            the command string
     * @param commandArgs
     *            the command and args
     * @return the ProcessJobBuilder
     */
    private ProcessJobBuilder createProcessJobBuilderForProcessJobType(ProcessJobType commandType, String command,
            String commandArgs)
    {
        ProcessJobBuilder processJobBuilder = null;
        switch (commandType)
        {
        case SIMPLE:
            SimpleToolProcessJobBuilder simpleToolProcessJobBuilder =
                    new SimpleToolProcessJobBuilder(this.processJobFactory, Utils.elStringToArray(command));
            processJobBuilder = simpleToolProcessJobBuilder;
            break;
        case CASDA_TOOL:
            CasdaToolProcessJobBuilder casdaToolProcessJobBuilder = casdaToolBuilderFactory.createBuilder();
            casdaToolProcessJobBuilder.setCommand(command);
            casdaToolProcessJobBuilder.addCommandArguments(Utils.elStringToArray(commandArgs));
            processJobBuilder = casdaToolProcessJobBuilder;
            break;
        default:
            throw new RuntimeException(String.format("Unknown command type: '%s' (expected one of: %s", commandType,
                    StringUtils.join(ProcessJobType.values(), ", ")));
        }
        return processJobBuilder;
    }

}
