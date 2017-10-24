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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import au.csiro.casda.Utils;
import au.csiro.casda.access.InlineScriptException;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.GeneratedSpectrumRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.jpa.ThumbnailRepository;
import au.csiro.casda.access.rest.CreateChecksumException;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.access.soda.GeneratedFileBounds;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.GeneratedSpectrum;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.MomentMap;
import au.csiro.casda.entity.observation.Cubelet;
import au.csiro.casda.entity.observation.Spectrum;
import au.csiro.casda.entity.observation.Thumbnail;
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

    /** The command name of the NGAS downloader tool. */
    public static final String NGAS_DOWNLOADER_TOOL_NAME = "ngas_download";

    private CachedFileRepository cachedFileRepository;

    private ImageCutoutRepository imageCutoutRepository;

    private GeneratedSpectrumRepository generatedSpectrumRepository;

    private JobManager jobManager;

    private final int maxDownloadAttempts;

    private String[] cutoutCommand;

    private String[] pngCutoutCommand;
    
    private String[] generateSpectrumCommand;
    
    private String[] extractEncapsulatedFileCommand;

    private ProcessJobFactory processJobFactory;

    private String depositToolsWorkingDirectory;

    private String downloadCommand;

    private ProcessJobType downloadCommandType;

    private String downloadCommandArgs;

    private CasdaToolProcessJobBuilderFactory casdaToolBuilderFactory;

    private SpectrumRepository spectrumRepository;

    private MomentMapRepository momentMapRepository;

    private CubeletRepository cubeletRepository;

    private ThumbnailRepository thumbnailRepository;

    private InlineScriptService inlineScriptService;

    private String calculateChecksumScript;

    /**
     * Constructor
     * 
     * @param cachedFileRepository
     *            the cached file repository
     * @param imageCutoutRepository
     *            the image cutout repository
     * @param generatedSpectrumRepository
     *            the generated spectrum repository
     * @param spectrumRepository
     *            the spectrum repository
     * @param momentMapRepository
     *            the moment map repository
     * @param cubeletRepository
     *            the cubelet repository
     * @param thumbnailRepository
     *            the thumbnail repository
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
     * @param extractEncapsulatedFileCommand
     * 			  the command to extract a single file from and encapsulation file
     * @param downloadCommandArgs
     *            the download command args to be used to download a data product from the archive if it is not - if
     *            downloadCommandType is SIMPLE then this should be null, otherwise it should be a list in Spring EL
     *            format representing the args to the CASDA tool
     * @param cutoutCommand
     *            the command and args to be used to produce a cutout (as a list in Spring EL format)
     * @param pngCutoutCommand
     *            the command and args to be used to produce a png cutout (as a list in Spring EL format)
     * @param generateSpectrumCommand
     *            the command and args to be used to produce a spectrum (as a list in Spring EL format)
     * @param processJobFactory
     *            the factory to be used to create job processes.
     * @param inlineScriptService
     *            service for calling shell scripts inline, used here to create checksums
     * @param calculateChecksumScript
     *            the path to the calculate checksum script
     */
    @Autowired
    public DownloadManager(CachedFileRepository cachedFileRepository, ImageCutoutRepository imageCutoutRepository, 
    		GeneratedSpectrumRepository generatedSpectrumRepository, SpectrumRepository spectrumRepository,
    		MomentMapRepository momentMapRepository, CubeletRepository cubeletRepository, 
    		ThumbnailRepository thumbnailRepository, JobManager jobManager, 
    		@Value("${deposit.tools.working.directory}") String depositToolsWorkingDirectory,
            @Value("${max.download.attempts}") int maxDownloadAttempts,
            CasdaToolProcessJobBuilderFactory casdaToolProcessJobBuilderFactory,
            @Value("${download.command.type}") String downloadCommandType,
            @Value("${download.command}") String downloadCommand, @Value("${download.args}") String downloadCommandArgs,
            @Value("${cutout.command}") String cutoutCommand, 
            @Value("${png.cutout.command}") String pngCutoutCommand, 
            @Value("${generate.spectrum.command}") String generateSpectrumCommand, 
            @Value("${extract.encapsulated.file.command}") String extractEncapsulatedFileCommand, 
            ProcessJobFactory processJobFactory, InlineScriptService inlineScriptService,
            @Value("${calculate.checksum.script}") String calculateChecksumScript)
    {
        this.cachedFileRepository = cachedFileRepository;
        this.imageCutoutRepository = imageCutoutRepository;
        this.generatedSpectrumRepository = generatedSpectrumRepository;
        this.spectrumRepository = spectrumRepository;
        this.momentMapRepository = momentMapRepository;
        this.cubeletRepository = cubeletRepository;
        this.jobManager = jobManager;
        this.depositToolsWorkingDirectory = depositToolsWorkingDirectory;
        this.downloadCommand = downloadCommand;
        this.inlineScriptService = inlineScriptService;
        this.calculateChecksumScript = calculateChecksumScript;
        this.downloadCommandType = ProcessJobType.valueOf(downloadCommandType);
        this.downloadCommandArgs = downloadCommandArgs;
        this.maxDownloadAttempts = maxDownloadAttempts;
        this.cutoutCommand = Utils.elStringToArray(cutoutCommand);
        this.pngCutoutCommand = Utils.elStringToArray(pngCutoutCommand);
        this.generateSpectrumCommand = Utils.elStringToArray(generateSpectrumCommand);
        this.extractEncapsulatedFileCommand = Utils.elStringToArray(extractEncapsulatedFileCommand);
        this.processJobFactory = processJobFactory;
        this.casdaToolBuilderFactory = casdaToolProcessJobBuilderFactory;
        this.thumbnailRepository = thumbnailRepository;
        /*
         * Perform a sanity check on the download command args by trying to create a ProcessJobBuilder
         */
        createProcessJobBuilderForProcessJobType(this.downloadCommandType, this.downloadCommand,
                this.downloadCommandArgs);
    }

    /**
     * Starts/restarts downloading a file if required, or checks the status if a download job is running or completed,
     * and updates the CachedFile table.
     * 
     * @param downloadingFile
     *            the file to download
     * @throws CacheException When the file cannot be retrieved or generated.
     */
    public void pollJobManagerForDownloadJob(CachedFile downloadingFile) throws CacheException
    {
        JobManager.JobStatus status = downloadingFile.getDownloadJobId() == null ? null
                : jobManager.getJobStatus(downloadingFile.getDownloadJobId());

        if (status == null)
        {
            // Job hasn't been started for this retry (e.g. throttled), so try kicking it off
            if (!maybeStartDownloadJob(downloadingFile, status))
            {
                throw new CacheException(String.format("File %s could not be retrieved.", downloadingFile.getFileId()));
            }
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
                if (EnumSet.of(FileType.GENERATED_SPECTRUM, FileType.IMAGE_CUTOUT)
                        .contains(downloadingFile.getFileType()))
                {
                    try
                    {
                        createChecksumFile(new File(downloadingFile.getPath()));
                    }
                    catch (CreateChecksumException e)
                    {
                        throw new CacheException("Unable to chreate checksum file for " + downloadingFile.getPath(), e);
                    }
                }
                        
            }

        }
    }

    private void retryDownloading(CachedFile downloadingFile, JobManager.JobStatus status) throws CacheException
    {
        incrementRetryCount(downloadingFile);

        if (!maybeStartDownloadJob(downloadingFile, status))
        {
            throw new CacheException(String.format("File %s could not be retrieved.", downloadingFile.getFileId()));
        }

        // make sure the new file id is saved, 
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
     * @return true if the job could be started, false if the job has already expended all retry attempts
     */
    private boolean maybeStartDownloadJob(CachedFile downloadingFile, JobManager.JobStatus status)
    {
        if (downloadingFile.getDownloadJobRetryCount() > maxDownloadAttempts)
        {
            // log a warning, and update the retry count. This won't be picked up as downloading again
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add(String.format("All download attempts failed for file %s", downloadingFile.getFileId()));
            logger.warn(builder.toString());
            cachedFileRepository.save(downloadingFile);
            return false;
        }

        String downloadJobId = buildDownloadJobId(downloadingFile.getFileId(), System.currentTimeMillis(),
                downloadingFile.getDownloadJobRetryCount());
        downloadingFile.setDownloadJobId(downloadJobId);

        ProcessJob downloadingJob;
        if (FileType.IMAGE_CUTOUT == downloadingFile.getFileType())
        {
        	downloadingJob = buildImageCutoutJob(downloadJobId, downloadingFile);
        }
        else if (FileType.GENERATED_SPECTRUM == downloadingFile.getFileType())
        {
        	downloadingJob = this.buildGenerateSpectrumJob(downloadJobId, downloadingFile);
        }
        else if(downloadingFile.isEncapsulatedFileType() && isEncapsulationFile(downloadingFile.getOriginalFilePath()))
        {
        	downloadingJob = buildEncapsulationJob(downloadJobId, downloadingFile);
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
        
        return true;
    }

    private boolean isEncapsulationFile(String originalFilePath)
    {
        if (StringUtils.isBlank(originalFilePath))
        {
            return false;
        }
        
        String lowerCasePath = originalFilePath.toLowerCase();
        return lowerCasePath.endsWith(".tar") || lowerCasePath.endsWith(".tar.gz")
                || lowerCasePath.endsWith(".tgz") || lowerCasePath.endsWith(".zip");
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
     * Build up a ProcessJob which will produce a image_cutout.
     * 
     * @param jobId
     *            the id for the job (running in slurm)
     * @param downloadingFile
     *            The CachedFile object defining the generated file.
     * @return The job details.
     */
    ProcessJob buildImageCutoutJob(String jobId, CachedFile downloadingFile)
    {
        String dimKeys[] = new String[] {"-D3", "-D4"};
        List<String> commandParts = new ArrayList<>();
        
        if (downloadingFile.getPath().endsWith("png"))
        {
            commandParts.addAll(Arrays.asList(pngCutoutCommand));
        }
        else
        {
            commandParts.addAll(Arrays.asList(cutoutCommand));
        }

        String sourcePath = downloadingFile.getOriginalFilePath();
        String[] fileIdDetails = downloadingFile.getFileId().split("-");
        ImageCutout cutout = imageCutoutRepository.findOne(Long.parseLong(fileIdDetails[1]));
        GeneratedFileBounds bounds = new GeneratedFileBounds(cutout.getBounds());

        for (int i = 0; i < dimKeys.length; i++)
        {
            // If there is no value for the parameter we need to remove the flag and the value from the command
            if (StringUtils.isEmpty(bounds.getDimBounds(i)))
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

        if (StringUtils.isNotEmpty(bounds.getDimBounds(0)))
        {
            processBuilder.setProcessParameter("dim3_range", bounds.getDimBounds(0));
        }
        if (StringUtils.isNotEmpty(bounds.getDimBounds(1)))
        {
            processBuilder.setProcessParameter("dim4_range", bounds.getDimBounds(1));
        }
        
        processBuilder.setProcessParameter("maxplane", String.valueOf(bounds.getMaxPlane()));
        processBuilder.setProcessParameter("ra", bounds.getRa());
        processBuilder.setProcessParameter("dec", bounds.getDec());
        processBuilder.setProcessParameter("xsize", bounds.getXSize());
        processBuilder.setProcessParameter("ysize", StringUtils.trimToEmpty(bounds.getYSize()));
        
        processBuilder.setWorkingDirectory(depositToolsWorkingDirectory);

        return processBuilder.createJob(jobId, FileType.IMAGE_CUTOUT.name());
    }
    
    /**
     * Build up a ProcessJob which will produce generated spectrum
     * 
     * @param jobId
     *            the id for the job (running in slurm)
     * @param downloadingFile
     *            The CachedFile object defining the generated file.
     * @return The job details.
     */
    ProcessJob buildGenerateSpectrumJob(String jobId, CachedFile downloadingFile)
    {
        String dimKeys[] = new String[] {"-D3", "-D4"};
        List<String> commandParts = new ArrayList<>();
        commandParts.addAll(Arrays.asList(generateSpectrumCommand));

        String sourcePath = downloadingFile.getOriginalFilePath();
        String[] fileIdDetails = downloadingFile.getFileId().split("-");
        GeneratedSpectrum spectrum = generatedSpectrumRepository.findOne(Long.parseLong(fileIdDetails[1]));
        GeneratedFileBounds bounds = new GeneratedFileBounds(spectrum.getBounds());

        for (int i = 0; i < dimKeys.length; i++)
        {
            // If there is no value for the parameter we need to remove the flag and the value from the command
            if (StringUtils.isEmpty(bounds.getDimBounds(i)))
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

        if (StringUtils.isNotEmpty(bounds.getDimBounds(0)))
        {
            processBuilder.setProcessParameter("dim3_range", bounds.getDimBounds(0));
        }
        if (StringUtils.isNotEmpty(bounds.getDimBounds(1)))
        {
            processBuilder.setProcessParameter("dim4_range", bounds.getDimBounds(1));
        }
        
        processBuilder.setProcessParameter("maxplane", String.valueOf(bounds.getMaxPlane()));
        processBuilder.setProcessParameter("ra", bounds.getRa());
        processBuilder.setProcessParameter("dec", bounds.getDec());
        processBuilder.setProcessParameter("xsize", bounds.getXSize());
        processBuilder.setProcessParameter("ysize", StringUtils.trimToEmpty(bounds.getYSize()));
        
        processBuilder.setWorkingDirectory(depositToolsWorkingDirectory);

        return processBuilder.createJob(jobId, FileType.GENERATED_SPECTRUM.name());
    }

    /**
     * Build up a ProcessJob which will extract a spectrum or moment map from an encapsulation file.
     * The file command is expected to handle extraction of both the file and its checksum and renaming it to match the 
     * NGAS file id.
     * 
     * @param jobId
     *            the id for the job (running in slurm)
     * @param downloadingFile
     *            The CachedFile object defining the encapsulated file (spectrum, moment map or cubelet)
     * @return The job details.
     */
    ProcessJob buildEncapsulationJob(String jobId, CachedFile downloadingFile)
    {
    	List<String> commandParts = new ArrayList<>();
    	commandParts.addAll(Arrays.asList(extractEncapsulatedFileCommand));
        String fileNameInArchive = getFileNameInArchive(downloadingFile);
    	commandParts.add(fileNameInArchive);
        commandParts.add(downloadingFile.getFileId());
    	
    	String sourcePath = downloadingFile.getOriginalFilePath();
    	String folder = new File(downloadingFile.getPath()).getParent();
    	
        SimpleToolProcessJobBuilder processBuilder =
                new SimpleToolProcessJobBuilder(this.processJobFactory, commandParts.toArray(new String[]{}));
 
        processBuilder.setProcessParameter("tarFileName", sourcePath);
    	
        processBuilder.setWorkingDirectory(folder);
        
        return processBuilder.createJob(jobId, downloadingFile.getFileType().name());
    }
    
    /**
     * Retrieve the original file name of the file to be extracted from an encapsulation 
     * @param downloadingFile 
     *            The CachedFile object defining the encapsulated file spectrum, moment map or cubelet)
     * @return The name of the file in the encapsulation 
     */
    String getFileNameInArchive(CachedFile downloadingFile)
    {
        // File id may include either the file name or the id of the record, along with the observation
        String fileIdPattern = "(observations|level7)-[0-9]+-[A-Za-z_]+-(.+)"; 
        Pattern pattern = Pattern.compile(fileIdPattern);
        Matcher matcher = pattern.matcher(downloadingFile.getFileId());
        String filename = matcher.find() ? matcher.group(2) : "";
        if (StringUtils.isEmpty(filename))
        {
            return "";
        }
        
        if (!filename.matches("^[0-9]+\\.[A-Za-z]+$"))
        {
            // This is the actual file name we should use - no more work to do
            return filename;
        }        
        
        // What we have is a numeric code plus the file type - we will need to lookup the code in the database
        long id = Long.parseLong(filename.split("\\.")[0]);

        switch (downloadingFile.getFileType())
        {
        case SPECTRUM:
            Spectrum spectrum = spectrumRepository.findOne(id);
            filename = spectrum != null ? spectrum.getFilename() : ""; 
            break;

        case MOMENT_MAP:
            MomentMap momentMap = momentMapRepository.findOne(id);
            filename = momentMap != null ? momentMap.getFilename() : ""; 
            break;

        case CUBELET:
            Cubelet cubelet = cubeletRepository.findOne(id);
            filename = cubelet != null ? cubelet.getFilename() : ""; 
            break;
              
        case THUMBNAIL:
            Thumbnail thumbnail = thumbnailRepository.findOne(id);
            filename = thumbnail != null ? thumbnail.getFilename() : ""; 
            break;

        default:
            filename = "";
            break;
        }

        return filename;
    }

    /**
     * Creates a checksum file for a given file. The destination will be file.checksum
     * 
     * @param file
     *            the file to calculate the checksum for
     * @throws CreateChecksumException
     *             if there is a problem creating the checksum file
     */
    void createChecksumFile(File file) throws CreateChecksumException
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
