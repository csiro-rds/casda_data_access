package au.csiro.casda.access.services;

import java.io.BufferedWriter;

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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import au.csiro.casda.Utils;
import au.csiro.casda.access.BadRequestException;
import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.EncapsulatedFileDescriptor;
import au.csiro.casda.access.FileDescriptor;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.cache.CacheException;
import au.csiro.casda.access.cache.CacheManagerInterface;
import au.csiro.casda.access.cache.DownloadManager;
import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.CubeletRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.EncapsulationFileRepository;
import au.csiro.casda.access.jpa.EvaluationFileRepository;
import au.csiro.casda.access.jpa.GeneratedSpectrumRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.ImageCutoutRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.jpa.MomentMapRepository;
import au.csiro.casda.access.jpa.SpectrumRepository;
import au.csiro.casda.access.jpa.ThumbnailRepository;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.services.NgasService.Status;
import au.csiro.casda.entity.CasdaDepositableArtefactEntity;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.GeneratedSpectrum;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.Thumbnail;
import au.csiro.casda.jobmanager.ProcessJob;
import au.csiro.casda.jobmanager.ProcessJobBuilder;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import au.csiro.casda.jobmanager.SimpleToolProcessJobBuilder;
import au.csiro.casda.jobmanager.SingleJobMonitor;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;

/**
 * Service for accessing the data for download.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 */
@Service
public class DataAccessService
{
    private static Logger logger = LoggerFactory.getLogger(DataAccessService.class);
    
    private static final List<FileType> DATA_ACCESS_JOB_FILE_TYPES = Arrays.asList(FileType.IMAGE_CUBE,
            FileType.MEASUREMENT_SET, FileType.ENCAPSULATION_FILE, FileType.SPECTRUM, FileType.MOMENT_MAP,
            FileType.CUBELET, FileType.CATALOGUE, FileType.IMAGE_CUTOUT, FileType.GENERATED_SPECTRUM, FileType.ERROR,
            FileType.EVALUATION_FILE);

    private final DataAccessJobRepository dataAccessJobRepository;

    private final ImageCubeRepository imageCubeRepository;

    private final MeasurementSetRepository measurementSetRepository;
    
    private final SpectrumRepository spectrumRepository;
    
    private final MomentMapRepository momentMapRepository;
    
    private final CubeletRepository cubeletRepository;
    
    private final EncapsulationFileRepository encapsulationFileRepository;
    
    private final EvaluationFileRepository evaluationFileRepository;
    
    private final ThumbnailRepository thumbnailRepository;

    private final CachedFileRepository cachedFileRepository;
    
    private final ImageCutoutRepository imageCutoutRepository;
    
    private final GeneratedSpectrumRepository generatedSpectrumRepository;

    private final NgasService ngasService;

    private final String cacheDir;
    
    private final int viewPageSize;
    
    private final int processPageSize;

    private static final String JOBS_DIR = "jobs";

    private static final String DMF_FILE_LOCATION_PARAMETER = "dmf_file_location";

    private final String archiveStatusCommandAndArgs;
    
    private static final String DMF_FILE_LOCATIONS_PARAMETER = "dmf_file_locations";

    private final String archiveGetCommandAndArgs;

    private ProcessJobFactory processJobFactory;
    
    private CacheManagerInterface cacheManager;
    
    private String undepositedLevel7CollectionsDir;
    
    private final DataAccessJdbcRepository dataAccessJdbcRepository;
    private CasdaMailService casdaMailService;


    private DownloadManager downloadManager;

    /**
     * Create a new DataAccessJobService instance.
     * 
     * @param dataAccessJobRepository
     *            The JPA repository for the DataAccessJob table.
     * @param imageCubeRepository
     *            The JPA repository for the ImageCube table.
     * @param measurementSetRepository
     *            The JPA repository for the MeasurementSet table.
     * @param spectrumRepository
     *            The JPA repository for the Spectrum table.
     * @param momentMapRepository
     *            The JPA repository for the MomentMap table.
     * @param cubeletRepository
     *            The JPA repository for the Cubelet table.
     * @param encapsulationFileRepository
     *            The JPA repository for the encapsulationFile table.
     * @param evaluationFileRepository
     *            The JPA repository for the evaluation file table.
     * @param thumbnailRepository
     *            The JPA repository for the Thumbnail table.
     * @param cachedFileRepository
     *            The JPA repository for the CachedFile table.
     * @param ngasService
     *            The service to interact with NGAS
     * @param cacheDir
     *            The path to the directory used for storing retrieved files.
     * @param viewPageSize
     *            the amount of files to be displayed on each page
     * @param processPageSize
     *            the amount of files to be processed each lot
     * @param archiveStatusCommandAndArgs
     *            The command and args EL string used to find the file status in ngas
     * @param archiveGetCommandAndArgs
     *            The command and args EL string used to Signal the DMF to bring files online for ngas
     * @param undepositedLevel7CollectionsDir
     *            The directory where the level 7 image cubes are copied to prior to deposit
     * @param processJobFactory
     *            the factory to be used to create job processes.
     * @param cacheManager 
     * 			  the class for managing the cache.
     * @param dataAccessJdbcRepository 
     * 			  the dataAccessJdbcRepository for straight SQl queries.
     * @param imageCutoutRepository 
     * 			  the imageCutoutRepository
     * @param generatedSpectrumRepository 
     * 			  the generatedSpectrumRepository
     * @param casdaMailService
     * 			  the email service for sending user notifications
     * @param downloadManager
     *            The downloadManager instance which will be doing the work.
     */
    @Autowired
    public DataAccessService(DataAccessJobRepository dataAccessJobRepository, 
    		ImageCubeRepository imageCubeRepository, MeasurementSetRepository measurementSetRepository, 
    		SpectrumRepository spectrumRepository, MomentMapRepository momentMapRepository, 
    		CubeletRepository cubeletRepository, EncapsulationFileRepository encapsulationFileRepository, 
    		EvaluationFileRepository evaluationFileRepository, 
    		ThumbnailRepository thumbnailRepository, CachedFileRepository cachedFileRepository, NgasService ngasService,
            @Value("${cache.home.dir}") String cacheDir,
            @Value("${view.page.size:25}") int viewPageSize,
            @Value("${process.page.size:25}") int processPageSize,
            @Value("${artefact.archive.status.command.and.args}") String archiveStatusCommandAndArgs,
            @Value("${artefact.archive.get.command.and.args}") String archiveGetCommandAndArgs, 
            @Value("${undeposited.level7.collections.dir}") String undepositedLevel7CollectionsDir, 
            ProcessJobFactory processJobFactory, CacheManagerInterface cacheManager, 
            DataAccessJdbcRepository dataAccessJdbcRepository, ImageCutoutRepository imageCutoutRepository, 
            GeneratedSpectrumRepository generatedSpectrumRepository,
            CasdaMailService casdaMailService,
            DownloadManager downloadManager)
    {
    	this.cacheManager = cacheManager;
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.imageCubeRepository = imageCubeRepository;
        this.measurementSetRepository = measurementSetRepository;
        this.spectrumRepository = spectrumRepository;
        this.momentMapRepository = momentMapRepository;
        this.cubeletRepository = cubeletRepository;
        this.cachedFileRepository = cachedFileRepository;
        this.ngasService = ngasService;
        this.cacheDir = cacheDir;
        this.viewPageSize = viewPageSize;
        this.processPageSize = processPageSize;
        this.archiveStatusCommandAndArgs = archiveStatusCommandAndArgs;
        this.archiveGetCommandAndArgs = archiveGetCommandAndArgs;
        this.processJobFactory = processJobFactory;
        this.thumbnailRepository = thumbnailRepository;
        this.undepositedLevel7CollectionsDir = undepositedLevel7CollectionsDir;
        this.dataAccessJdbcRepository = dataAccessJdbcRepository;
        this.imageCutoutRepository = imageCutoutRepository;
        this.generatedSpectrumRepository = generatedSpectrumRepository;
        this.encapsulationFileRepository = encapsulationFileRepository;
        this.evaluationFileRepository = evaluationFileRepository;
        this.casdaMailService = casdaMailService;
        this.downloadManager = downloadManager;
    }

	/**
     * Gets the existing data access job
     * 
     * @param requestId
     *            unique uuid identifier of the download job
     * @return the dataAccessJob or null if not found
     */
    public DataAccessJob getExistingJob(String requestId)
    {
        DataAccessJob job = dataAccessJobRepository.findByRequestId(requestId);
        return job;
    }

    /**
     * Saves a data access job.
     * 
     * @param job
     *            the dataAccessJob to be saved.
     * @return the saved dataAccessJob
     */
    public DataAccessJob saveJob(DataAccessJob job)
    {
        job = dataAccessJobRepository.save(job);
        return job;
    }

    /**
     * Locates the file, that has been requested for download, on the file system
     * 
     * @param dataAccessJob
     *            the data access job to download the file for
     * @param filename
     *            the filename
     * @return File the file
     * @throws IOException
     *             if the file doesn't exist, or can't be read
     */
    public File findFile(DataAccessJob dataAccessJob, String filename) throws IOException
    {
        String requestId = dataAccessJob.getRequestId();

        File downloadFile = getFileLocation(requestId, filename);

        if (!downloadFile.exists() || !Files.isReadable(downloadFile.toPath()))
        {
            throw new IOException("File doesn't exist at " + downloadFile.getAbsolutePath() + " for request id "
                    + requestId + " and filename " + filename);
        }
        return downloadFile;
    }

    /**
     * Finds the file type of the given filename.
     * 
     * @param filename
     *            the filename, corresponds with the file id in the cached table
     * @return the file type
     */
    public FileType findRecordType(String filename)
    {
        CachedFile cachedFile = cachedFileRepository.findByFileId(filename);
        return (cachedFile != null) ? cachedFile.getFileType() : null;
    }

    /**
     * Gets the file location on the file system.
     * 
     * @param requestId
     *            the request id
     * @param filename
     *            the filename
     * @return the absolute file path for the file to download
     */
    public File getFileLocation(String requestId, String filename)
    {
        return new File(getJobDirectory(requestId), filename);
    }

    /**
     * Gets the location of the job directory on the file system.
     * 
     * @param requestId
     *            the job request id
     * @return the absolute file path for the job directory
     */
    public File getJobDirectory(String requestId)
    {
        StringBuilder jobRequestDir = new StringBuilder();
        jobRequestDir.append(cacheDir);
        jobRequestDir.append(File.separator);
        jobRequestDir.append(JOBS_DIR);
        jobRequestDir.append(File.separator);
        jobRequestDir.append(requestId);
        return new File(jobRequestDir.toString());
    }

    /**
     * Updates job as completed.
     * 
     * @param requestId
     *            of the job that is completed
     * @param expiryTime
     *            of the job that is completed - this is when the files will expire from the cache.
     */
    public void markRequestCompleted(String requestId, DateTime expiryTime)
    {
        DataAccessJob dataAccessJob = getExistingJob(requestId);

        dataAccessJob.setStatus(DataAccessJobStatus.READY);
        dataAccessJob.setAvailableTimestamp(DateTime.now(DateTimeZone.UTC));
        dataAccessJob.setExpiredTimestamp(expiryTime);
        casdaMailService.sendEmail(dataAccessJob, CasdaMailService.READY_EMAIL, CasdaMailService.READY_EMAIL_SUBJECT);
        dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Updates job expiry date.
     * 
     * @param requestId
     *            of the job
     * @param expiryTime
     *            the time to set for expiry
     */
    public void updateExpiryDate(String requestId, DateTime expiryTime)
    {
        DataAccessJob dataAccessJob = getExistingJob(requestId);

        dataAccessJob.setExpiredTimestamp(expiryTime);

        dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Updates the file size for the cutouts for a given data access request. Also updates the total size of the data
     * access job record.
     * 
     * @param files
     *            download files, with file size details to use to update the data access job record
     */
    public void updateFileSizeForGeneratedFiles(Collection<DownloadFile> files)
    {
        for (DownloadFile file : files)
        {
            switch (file.getFileType())
            {
            case IMAGE_CUTOUT:
            	ImageCutout cutout = imageCutoutRepository.findOne(file.getId());
            	if(cutout != null)
            	{
                    cutout.setFilesize(file.getSizeKb());
                    imageCutoutRepository.save(cutout);
            	}
                break;
            case GENERATED_SPECTRUM:

            	GeneratedSpectrum spectrum = generatedSpectrumRepository.findOne(file.getId());
            	if(spectrum != null)
            	{
                    spectrum.setFilesize(file.getSizeKb());
                    generatedSpectrumRepository.save(spectrum);
            	}
                break;
            default:
                // skip anything that isn't a generated file
                break;
            }
        }
    }

    /**
     * Updates the job as in error with the given expiryTime.
     * 
     * @param requestId
     *            the request id of the job that failed.
     * @param expiryTime
     *            the expiry time of the job.
     */
    public void markRequestError(String requestId, DateTime expiryTime)
    {
        DataAccessJob dataAccessJob = getExistingJob(requestId);
        dataAccessJob.setExpiredTimestamp(expiryTime);
        dataAccessJob.setStatus(DataAccessJobStatus.ERROR);
        casdaMailService.sendEmail(dataAccessJob, CasdaMailService.FAILED_EMAIL, CasdaMailService.FAILED_EMAIL_SUBJECT);   
        dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Finds the file path in NGAS corresponding with the given data product type and id.
     * 
     * @param dataAccessProduct
     *            the requested data product
     * @return the file path if it is available on disk, null otherwise.
     * @throws ResourceNotFoundException
     *             if there is no corresponding database record for this data product type and id
     * @throws ServiceCallException
     *             if there is a problem calling NGAS about the status or location of the file.
     */
    @Transactional
    public Path findFileInNgasIfOnDisk(DataAccessDataProduct dataAccessProduct)
            throws ResourceNotFoundException, ServiceCallException
    {
    	CasdaDepositableArtefactEntity artefact = findDataProduct(dataAccessProduct);
        if (artefact == null)
        {
            throw new ResourceNotFoundException(dataAccessProduct.getDataAccessProductType() + " with id "
                    + dataAccessProduct.getId() + " does not exist");
        }

        String fileId = artefact.getFileId();
        return findFileInNgasIfOnDisk(fileId);
    }

    /**
     * Finds the file path in NGAS corresponding with the given file id.
     * 
     * @param fileId
     *            the data product's file id in ngas
     * @return the file path if it is available on disk, null otherwise.
     * @throws ResourceNotFoundException
     *             if there is no corresponding NGAS record for this file id
     * @throws ServiceCallException
     *             if there is a problem calling NGAS about the status or location of the file.
     */
    public Path findFileInNgasIfOnDisk(String fileId) throws ResourceNotFoundException, ServiceCallException
    {
        Path filepath = findFileInNgas(fileId);
        if (isAvailableOnDiskInNgas(filepath, fileId))
        {
            return filepath;
        }
        else
        {
            // if it's not on disk in ngas, return null
            return null;
        }
    }


    /**
     * Finds the file path in NGAS corresponding with the given file id.
     * 
     * @param fileId
     *            the data product's file id in ngas
     * @return the file path.
     * @throws ResourceNotFoundException
     *             if there is no corresponding NGAS record for this file id
     * @throws ServiceCallException
     *             if there is a problem calling NGAS about the status or location of the file.
     */
    public Path findFileInNgas(String fileId) throws ServiceCallException, ResourceNotFoundException
    {
        Status ngasStatus = ngasService.getStatus(fileId);
        if (!ngasStatus.wasSuccess())
        {
            throw new ServiceCallException("Request to get status failed from NGAS for file id " + fileId);
        }
        if (StringUtils.isBlank(ngasStatus.getMountPoint()) || StringUtils.isBlank(ngasStatus.getFileName()))
        {
            throw new ResourceNotFoundException(fileId + " does not exist in NGAS");
        }
        Path filepath = Paths.get(ngasStatus.getMountPoint(), ngasStatus.getFileName());
        return filepath;
    }

    /**
     * Finds the data product entity corresponding to the type and id.
     * 
     * @param dataAccessProduct
     *            the requested data product
     * @return the data product (or null if no matching data product could be found)
     */
    public CasdaDepositableArtefactEntity findDataProduct(DataAccessDataProduct dataAccessProduct)
    {
        switch (dataAccessProduct.getDataAccessProductType())
        {
        case cube:
            return imageCubeRepository.findOne(dataAccessProduct.getId());
        case visibility:
            return measurementSetRepository.findOne(dataAccessProduct.getId());
        case spectrum:
            return spectrumRepository.findOne(dataAccessProduct.getId());
        case moment_map:
            return momentMapRepository.findOne(dataAccessProduct.getId());
        case cubelet:
            return cubeletRepository.findOne(dataAccessProduct.getId());
        case encap:
            return encapsulationFileRepository.findOne(dataAccessProduct.getId());
        case evaluation:
            return evaluationFileRepository.findOne(dataAccessProduct.getId());
        default:
            throw new IllegalArgumentException(
                    "Data access product type " + dataAccessProduct.getDataAccessProductType() + " not supported");
        }
    }

    /**
     * Checks whether the given file is available on disk in ngas
     * 
     * @param filepath
     *            the path to the file
     * @param fileId
     *            the ngas file id
     * @return true if the file in ngas is available on disk, false otherwise.
     * @throws ServiceCallException
     *             if there is a problem getting the state of the file from ngas
     */
    private boolean isAvailableOnDiskInNgas(Path filepath, String fileId) throws ServiceCallException
    {
        // create new process job to get the DMF state for the artifact
        ProcessJobBuilder archiveStatusBuilder = new SimpleToolProcessJobBuilder(processJobFactory,
                Utils.elStringToArray(this.archiveStatusCommandAndArgs));
        archiveStatusBuilder.setProcessParameter(DMF_FILE_LOCATION_PARAMETER, filepath.toString());
        ProcessJob job = archiveStatusBuilder.createJob(null, null);

        SingleJobMonitor monitor = createSingleJobMonitor();
        // runs inline
        job.run(monitor);
        if (monitor.isJobFailed())
        {
            logger.error("Checking status of artefact on the DMF using command {} failed, reason: {}",
                    job.getDescription(),
                    StringUtils.isBlank(monitor.getJobOutput()) ? "<NO OUTPUT FROM PROCESS>" : monitor.getJobOutput());
            throw new ServiceCallException("Couldn't retrieve file status for " + fileId);
        }

        return monitor.isJobFinished() && "DUL".equals(monitor.getJobOutput());
    }

    /**
     * Creates a single job monitor
     * 
     * @return a new SingleJobMonitor
     */
    protected SingleJobMonitor createSingleJobMonitor()
    {
        return new SingleJobMonitor();
    }

    /**
     * Downloads the file from the given job.
     * 
     * @param dataAccessJob
     *            a DataAccessJob
     * @param filename
     *            the name of the file
     * @param response
     *            the response to stream the file data to
     * @param skipCacheCheck
     *            if the file cache should be skipped when looking for the file
     * @param headersOnly
     *            True if the file content should not be sent such as in response to a HEAD request.
     * @throws ResourceNotFoundException
     *             if the file could not be found
     */
    public void downloadFile(DataAccessJob dataAccessJob, String filename, HttpServletResponse response,
            boolean skipCacheCheck, boolean headersOnly) throws ResourceNotFoundException
    {
        long start = System.currentTimeMillis();

        File filepath = null;
        try
        {
            if (!skipCacheCheck)
            {
                filepath = findFile(dataAccessJob, filename);
                logger.debug("filepath: {}", filepath);
            }
        }
        catch (IOException e)
        {
            /*
             * SODA_SYNC_* jobs may load data from NGAS so we try to read them directly from NGAS - see below. For
             * non-SODA_SYNC jobs, failing to find the file in the cache is an error.
             */
            if (!au.csiro.casda.access.util.Utils.SYNC_DOWNLOADS.contains(dataAccessJob.getDownloadMode()))
            {
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add("Could not stream the file " + dataAccessJob.getRequestId() + " filename " + filename);
                logger.error(builder.toString(), e);
                throw new BadRequestException(e);
            }
        }
        if (filepath == null)
        {
            try
            {
                /*
                 * The filename is the fileId
                 */
                filepath = findFileInNgasIfOnDisk(filename).toFile();
            }
            catch (ServiceCallException e)
            {
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add("Could not stream the file " + dataAccessJob.getRequestId() + " filepath " + filepath);
                logger.error(builder.toString(), e);
                throw new BadRequestException(e);
            }
        }

        MediaType contentType;
        switch (FilenameUtils.getExtension(filepath.getName()))
        {
        case "fits":
            contentType = new MediaType("image", "x-fits");
            break;
        case "checksum":
            contentType = MediaType.TEXT_PLAIN;
            break;
        default:
            contentType = MediaType.APPLICATION_OCTET_STREAM;
            break;
        }

        try (ServletOutputStream servletOutput = response.getOutputStream();
                FileInputStream file = new FileInputStream(filepath))
        {
            response.addHeader("Content-Disposition", "attachment; filename=" + filename);
            response.addHeader("Content-Length", Long.toString(filepath.length()));
            response.addHeader("Content-Type", contentType.toString());

            if (!headersOnly)
            {
                IOUtils.copyLarge(file, servletOutput);
            }
            response.flushBuffer();
        }
        catch (Exception e)
        {
            if(e.getClass().getSimpleName().equals("ClientAbortException"))
            {
                logger.info("Client cancelled download for file {} for request {} ", filepath,
                        dataAccessJob.getRequestId());
                return;
            }
            
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Could not stream the file " + dataAccessJob.getRequestId() + " filepath " + filepath);
            logger.error(builder.toString(), e);
            throw new BadRequestException(e);
        }

        long duration = System.currentTimeMillis() - start;

        FileType downloadFileType = findRecordType(filename);
        String fileType = downloadFileType == null ? "unknown" : downloadFileType.name();

        logger.info(CasdaDataAccessEvents.E041.messageBuilder().addTimeTaken(duration).add(dataAccessJob.getRequestId())
                .add(filename).add(DataAccessUtil.convertBytesToKb(filepath.length())).add(fileType)
                .add(dataAccessJob.getDownloadMode().name()).toString());
    }
    
    /**
     * Downloads the file from the given NGAS fileId.
     * 
     * @param fileId
     *            the NGAS fileId
     * @param response
     *            the response to stream the file data to
     * @throws ResourceNotFoundException
     *             if the file could not be found
     */
    public void downloadThumbnailFromNgas(String fileId, HttpServletResponse response) throws ResourceNotFoundException
    {
        Thumbnail thumb = getThumbnailRecord(fileId);

        try
        {
        	if(thumb != null && thumb.getEncapsulationFile() != null)
        	{
        		EncapsulatedFileDescriptor downloadFile = DataAccessUtil.getThumbnailFile(thumb);
                CachedFile existingCachedFile = cacheManager.getCachedFile(downloadFile.getFileId());
                if (existingCachedFile != null && !existingCachedFile.isFileAvailableFlag())
                {
                    // Encapsulation cache record exists and is downloading, so poll for how it is going
                    logger.debug("Waiting for encapsulation to be available "
                            + downloadFile.getEncapsulationFile().getFileId());
                    checkFileAvailable(existingCachedFile);
                }

                if (existingCachedFile != null && existingCachedFile.isFileAvailableFlag())
                {
        			//if file is available return it to user
                    logger.debug("Returning available thumbnail " + downloadFile.getFileId());
            		CachedFile cachedFile = cacheManager.getCachedFile(downloadFile.getFileId());
        			returnFile(cachedFile, response);
        			return;
        		}
                else if (existingCachedFile == null)
        		{
                    CachedFile encapsedCachedFile =
                            cacheManager.getCachedFile(downloadFile.getEncapsulationFile().getFileId());
                    if (encapsedCachedFile != null && !encapsedCachedFile.isFileAvailableFlag())
                    {
                        // Encapsulation cache record exists and is downloading, so poll for how it is going
                        logger.debug("Waiting for encapsulation to be available "
                                + downloadFile.getEncapsulationFile().getFileId());
                        checkFileAvailable(encapsedCachedFile);
                    }

                    if (encapsedCachedFile != null && encapsedCachedFile.isFileAvailableFlag())
                    {
                        // Encapsulation is available in the cache so create cachedFile for thumbnail
                        String dirString = cacheDir + File.separatorChar + "data" + File.separatorChar
                                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                        File dir = new File(dirString);
                        if (!dir.exists() && !dir.mkdirs() || !dir.isDirectory())
                        {
                            throw new CacheException("Can't create output directory " + dirString);
                        }
                        String cacheDestination = dir.getAbsolutePath() + File.separatorChar + downloadFile.getFileId();
                        CachedFile cachedThumbFile =
                                createCachedFile(downloadFile, cacheDestination, encapsedCachedFile.getPath());

                        logger.info("Thumbnail retrieval request started for " + fileId + " from "
                                + downloadFile.getEncapsulationFile().getFileId() + ". Created " + cachedThumbFile);
                        
                        // save cached file entry
                        cachedFileRepository.save(cachedThumbFile);
                        
                        //now trigger the download 
                        checkFileAvailable(cachedThumbFile);
                    }
                    else if (encapsedCachedFile == null)
                    {
                        // If encapsulation does not exist, create cache record
                        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                        FileDescriptor encapsulationFile = downloadFile.getEncapsulationFile();
                        String cacheDestination = cacheDir + "/data/" + dateFormatter.format(new Date()) + "/"
                                + encapsulationFile.getFileId();
                        CachedFile cachedEncapsFile = createCachedFile(encapsulationFile, cacheDestination,
                                downloadFile.getOriginalEncapsulationFilePath());

                        logger.info("Encapsulation retrieval request started for " + encapsulationFile.getFileId()
                                + ". Created " + cachedEncapsFile);
                        
                        // save cached file entry
                        cachedFileRepository.save(cachedEncapsFile);
                        
                        //now trigger the download 
                        checkFileAvailable(cachedEncapsFile);
                    }
        		}

                response.sendError(HttpServletResponse.SC_NO_CONTENT);

        	}
        	else
        	{
        		//for thumbnails which pre-exist the encapsulation of small files.
        		ngasService.retrieveFile(fileId, response);
        	} 
        }
        catch (ServiceCallException e)
        {
            try
            {
                logger.info("Requested fileId {} was not found", fileId, e);
                response.sendError(HttpStatus.NOT_FOUND.value());
            }
            catch (IOException e1)
            {
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add("Could not send error status (HttpStatus 404)");
                logger.error(builder.toString(), e);
            }
        }
        catch (IOException e) 
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Could not send no content (HttpStatus 204)");
            logger.error(builder.toString(), e);
		}
        catch (CacheException e)
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Problem accessing cache");
            logger.error(builder.toString(), e);
        } 
    }
    
    /**
     * Retrieves thumbnails directly from the staging area, this is only used for undeposited Level 7 image collections
     * which haven't been deposited yet, but still need the previews for approval
     * @param dapCollectionId the dap collection id
     * @param filename the filename including path to file from this particular L7 collection's base directory
     * @return the byte array of the thumbnail
     * @throws IOException if the file cannot be read
     */
    public byte[] retrieveUndepositedThumbnail(String dapCollectionId, String filename) 
    		throws IOException
    {
        Path path = Paths.get(undepositedLevel7CollectionsDir, dapCollectionId, filename);
        return Files.readAllBytes(path);
    }

    private Thumbnail getThumbnailRecord(String fileId)
    {
        // File id may include either the file name or the id of the record, along with the observation
        String fileIdPattern = "(observations|level7)-([0-9]+)-[A-Za-z_]+-(.+)";
        Pattern pattern = Pattern.compile(fileIdPattern);
        Matcher matcher = pattern.matcher(fileId);

        if (!matcher.find())
        {
            return null;
        }

        String parentType = matcher.group(1);
        String sbid = matcher.group(2);
        String filename = matcher.group(3);

        if (!filename.matches("^[0-9]+\\.[A-Za-z]+$"))
        {
            // This is the actual file name so we lookup by filename
            if (parentType.equals("level7"))
            {
                return thumbnailRepository.findLevel7Thumbnail(filename, Long.parseLong(sbid));
            }

            return thumbnailRepository.findThumbnail(filename, Integer.parseInt(sbid));
        }

        // We have a numeric code plus the file type so we lookup by id
        long id = Long.parseLong(filename.split("\\.")[0]);
        return thumbnailRepository.findOne(id);
    }

    private CachedFile createCachedFile(FileDescriptor downloadFile, String cacheDestination,
            String originalPath)
    {
        CachedFile cachedFile= new CachedFile(downloadFile.getFileId(), cacheDestination, 
        		downloadFile.getSizeKb(), DateTime.now(DateTimeZone.UTC).plusWeeks(1));
        cachedFile.setLastModified(DateTime.now());
        cachedFile.setDownloadJobRetryCount(0);
        cachedFile.setFileAvailableFlag(false);
        cachedFile.setFileType(downloadFile.getFileType());
        cachedFile.setOriginalFilePath(originalPath);
        return cachedFile;
    }
    
    private void returnFile(CachedFile cachedFile, HttpServletResponse response)
    {
    	try
    	{
    		File file = new File(cachedFile.getPath());

        	FileInputStream fis = new FileInputStream(file);
        	
        	response.addHeader("Content-disposition", "attachment;filename=" + file.getName());
        	response.setContentType("image/png");
        	
        	IOUtils.copy(fis, response.getOutputStream());
        	
        	response.flushBuffer();
    	}
    	catch(IOException e)
    	{
            try
            {
                logger.info("Requested fileId {} could not be processed", cachedFile.getFileId(), e);
                response.sendError(HttpStatus.UNPROCESSABLE_ENTITY.value());
            }
            catch (IOException e1)
            {
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add("Could not send error status (HttpStatus 422)");
                logger.error(builder.toString(), e);
            }
    	}
    }
    
    /**
     * Signal DMF to start moving files online
     * 
     * @param filePaths
     *            List of file paths separated by white spaces in string
     */
    public void signalDownloadFilesToGoOnline(String filePaths)
    {
        logger.info(String.format("Bringing paths %s online", filePaths));
        long startTime = (new Date()).getTime();
        
        // create new process job to get the DMF state for the artifact
        ProcessJobBuilder archiveStatusBuilder = new SimpleToolProcessJobBuilder(processJobFactory,
                Utils.elStringToArray(this.archiveGetCommandAndArgs));
        archiveStatusBuilder.setProcessParameter(DMF_FILE_LOCATIONS_PARAMETER, filePaths);
        ProcessJob job = archiveStatusBuilder.createJob(null, null);

        SingleJobMonitor monitor = createSingleJobMonitor();
        // runs inline
        job.run(monitor);
        if (monitor.isJobFailed())
        {
            logger.error("Signaling artefacts on the DMF to go Online using command {} failed, reason: {}",
                    job.getDescription(),
                    StringUtils.isBlank(monitor.getJobOutput()) ? "<NO OUTPUT FROM PROCESS>" : monitor.getJobOutput());
        }
        else
        {
            long endTime = (new Date()).getTime();
            logger.info("Paths are online after " + (endTime - startTime) + " ms.");
        }
    }

    /**
     * Return a error file to the user with the error outlined within
     * @param message the error message for the user
     * @param response the http response object
     * @param showParams true if the parameters should be shown in the error. this is used when the 
     *          given combination of params return no results
     * @param dataAccessJob the data access job
     */
    public void returnErrorFile
                    (String message, HttpServletResponse response, DataAccessJob dataAccessJob, boolean showParams)
    {
        try (ServletOutputStream servletOutput = response.getOutputStream();)
        {
            File file = File.createTempFile("casda_data_access_exception", ".txt");
            BufferedWriter bw = new BufferedWriter
                    (new OutputStreamWriter(new FileOutputStream(file), Charset.defaultCharset()));
            bw.write(message);
            bw.newLine();
            bw.write("Request ID: " + dataAccessJob.getRequestId());
            if(showParams)
            {
                bw.newLine();
                bw.write("Parameters: ");
                bw.newLine();
                for(String key : dataAccessJob.getParamMap().keySet())
                {
                    String[] paramValues = dataAccessJob.getParamMap().get(key);
                    String paramString = "";
                    for(String param : paramValues)
                    {
                        paramString += " " + param;
                    }
                    
                    bw.write(key + " : " + paramString);
                    bw.newLine();
                }
            }
            bw.close();
            
            response.addHeader("Content-Disposition", "attachment; filename=casda_data_access_exception.txt");
            response.addHeader("Content-Type", MediaType.TEXT_PLAIN.toString());
            IOUtils.copyLarge(new FileInputStream(file), servletOutput);
            response.flushBuffer();
        }
        catch (Exception e)
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Could not return error file for failure of data access job: " + dataAccessJob.getRequestId());
            logger.error(builder.toString(), e);
            throw new BadRequestException(e);
        }
        
    }    
    
    /**
     * @param requestId the data access job request id
     * @param viewable true if this is for screen display, false for processing (higher page limit)
     * @return the page mapping for all the files to be displayed for this job.
     */
    public List<Map<FileType, Integer[]>> getPaging(String requestId, boolean viewable)
    {
    	List<Map<FileType, Integer[]>> paging = new ArrayList<Map<FileType, Integer[]>>();
    	
    	Map<String, Object> fileCounts = dataAccessJdbcRepository.countFilesForJob(requestId);
    	
    	Map<FileType, Integer[]> page = new LinkedHashMap<FileType, Integer[]>();
    	
    	paging.add(page);//first page/latest page
    	
    	for(FileType key : DATA_ACCESS_JOB_FILE_TYPES)
    	{
        	if((Long)fileCounts.get(key.name()) > 0)
        	{
        		int total = ((Long)fileCounts.get(key.name())).intValue();
        		int remainder = total;
        		while(remainder > 0)
        		{
        			int spaceAvailable = calculateSpaceInPage(page, viewable ? viewPageSize : processPageSize);
        			int min = total-remainder + 1;
        			int max;
        			if(remainder <= spaceAvailable)
        			{
        				max = total;
        				remainder = 0;
            			page.put(key, new Integer[]{min, max});
        			}
        			else
        			{
        				remainder -= spaceAvailable;
        				max = total- remainder;
        				
            			page.put(key, new Integer[]{min, max});
            			page = new LinkedHashMap<FileType, Integer[]>();
            			paging.add(page);
        			}
        		}   		
        	}
    	}

    	return paging;
    }
    
    private int calculateSpaceInPage(Map<FileType, Integer[]> page, int pageSize)
    {
    	int space = pageSize;
    	for(FileType key : page.keySet())
    	{
    		space -= (page.get(key)[1]-page.get(key)[0]+1);
    	}
    	
		return space;
    }
    
    /**
     * @param pageDetails the details of this page (file types included and the bounds for each type)
     * @param dataAccessJob the data access job
     * @return the files to be displayed on the current page
     */
    @SuppressWarnings("unchecked")
	public List<DownloadFile> getPageOfFiles(Map<FileType, Integer[]> pageDetails, DataAccessJob dataAccessJob)
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
        Integer[] page;

    	for(FileType fileType : pageDetails.keySet())
    	{
    		switch(fileType)
    		{
    		case CATALOGUE:
            	if(pageDetails.keySet().contains(fileType))
            	{
            		page = pageDetails.get(fileType);
            		List<Map<String, Object>> artefactDetails = dataAccessJdbcRepository.getPageOfCatalogues(
            				 dataAccessJob.getRequestId(), fileType.name(), page);
                	DataAccessUtil.getCatalogueDownloadFiles(artefactDetails, fileList, 
                			getJobDirectory(dataAccessJob.getRequestId()), dataAccessJob);
            	}
    			break;
    		case MEASUREMENT_SET:
    		case IMAGE_CUBE:
    		case ENCAPSULATION_FILE:
            	if(pageDetails.keySet().contains(fileType))
            	{
            		page = pageDetails.get(fileType);
            		List<Map<String, Object>> artefactDetails = dataAccessJdbcRepository.getPageOfDownloadFiles(
            				fileType.name(), dataAccessJob.getRequestId(), page, false);
                	DataAccessUtil.getFileDescriptors(artefactDetails, fileList, fileType);
            	}
            	break;
            case EVALUATION_FILE:
    		case SPECTRUM:
    		case MOMENT_MAP:
    		case CUBELET:
            	if(pageDetails.keySet().contains(fileType))
            	{
            		page = pageDetails.get(fileType);
            		List<Map<String, Object>> artefactDetails = dataAccessJdbcRepository.getPageOfDownloadFiles(
            				fileType.name(),  dataAccessJob.getRequestId(), page, true);
                	DataAccessUtil.getEncapsulatedFileDescriptor(artefactDetails, fileList, fileType);
            	}
            	break;
    		case GENERATED_SPECTRUM:
    		case IMAGE_CUTOUT:
            	if(pageDetails.keySet().contains(fileType))
            	{
               		page = pageDetails.get(fileType);
            		List<Map<String, Object>> artefactDetails = dataAccessJdbcRepository.getPageOfGeneratedFiles(
            				dataAccessJob.getRequestId(), fileType.name(), page);
            		DataAccessUtil.getGeneratedImageFiles(artefactDetails, fileList, fileType);
            	}
    			break;
    		case ERROR:
               	if(pageDetails.keySet().contains(fileType))
            	{
               		page = pageDetails.get(fileType);
            		List<Map<String, Object>> artefactDetails = dataAccessJdbcRepository.getPageOfErrorFiles(
            				dataAccessJob.getRequestId(), fileType.name(), page);
            		DataAccessUtil.getErrorFiles(artefactDetails, fileList);
            	}
    			break;
    		default:
    			break;
    		}
    	}
        return fileList;
    }

    /**
     * @param id the id of the data access job to check
     * @return true if image cubes or measurement sets exist for this data access job
     */
	public boolean isImageCubeOrMeasurementSetJob(Long id)
	{
		return dataAccessJdbcRepository.isImageCubesAndMeasurementSetsExist(id);
	}

	/**
	 * @param requestId the data access job id
	 * @return the total number of files attached to this data access job
	 */
	public long getFileCount(String requestId)
	{
		long total = 0;
		Map<String, Object> counts = dataAccessJdbcRepository.countFilesForJob(requestId);
		
		for(String key : counts.keySet())
		{
			if(!key.equals(FileType.ERROR.name()))
			{
				total += (Long)counts.get(key);
			}
		}
		return total;
	}

	/**
	 * @param projectCode the project opal code to check against the database
	 * @return true if this project exists in the database
	 */
	public boolean isProjectExist(String projectCode)
	{
		return dataAccessJdbcRepository.projectExists(projectCode);
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
     * Download a preview image of a cutout. This will start or poll a data access job to produce a cutout in PNG 
     * format from an image cube containing the requested sky region.
     *    
     * @param imageCubeId The id of the image cube
     * @param ra The right ascension of the central point of the cutout in decimal degrees.
     * @param dec The declination of the central point of the cutout in decimal degrees.
     * @param radius The size of the cutout, measured as a radius in decimal degrees. 
     * @param response
     *            the response to stream the file data to
     * @return true if an exiting cutout is available or being prepared, false if none exists yet 
     */
    public boolean downloadCutoutPreview(long imageCubeId, double ra, double dec, double radius,
            HttpServletResponse response)
    {
        // Calculate bounds 
        double len = radius *  2d;
        String bounds = String.format("%f %f %.6f %.6f", ra, dec, len, len);
        // Find generated file 
        List<ImageCutout> cutoutList =
                imageCutoutRepository.findByImageCubeIdBoundsAndDownloadFormat(imageCubeId, bounds, "png");
        
        try
        {
            if (CollectionUtils.isEmpty(cutoutList))
            {
                // If not started, let the caller know it doesn't exist yet
                return false;
            }
            
            ImageCutout cutout = cutoutList.get(0);
            if (cutout.getDataAccessJob().isReady())
            {
                // If finished, return the result
                CachedFile cachedFile = cacheManager.getCachedFile(cutout.getFileId());
                returnFile(cachedFile, response);
            }
            else
            {
                // Otherwise return that it is not ready
                response.sendError(HttpServletResponse.SC_NO_CONTENT);
            }
        }
        catch (IOException e) 
        {
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Could not send no content (HttpStatus 204)");
            logger.error(builder.toString(), e);
        }
        return true;
    }
}
