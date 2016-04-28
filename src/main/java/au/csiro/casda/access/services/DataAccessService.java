package au.csiro.casda.access.services;

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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.Transactional;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;

import au.csiro.casda.Utils;
import au.csiro.casda.access.BadRequestException;
import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.CutoutFileDescriptor;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.DownloadFile;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.jpa.ImageCubeRepository;
import au.csiro.casda.access.jpa.MeasurementSetRepository;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.services.NgasService.Status;
import au.csiro.casda.entity.CasdaDataProductEntity;
import au.csiro.casda.entity.dataaccess.CachedFile;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
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

    private final DataAccessJobRepository dataAccessJobRepository;

    private final ImageCubeRepository imageCubeRepository;

    private final MeasurementSetRepository measurementSetRepository;

    private final CachedFileRepository cachedFileRepository;

    private final NgasService ngasService;

    private final String cacheDir;

    private static final String JOBS_DIR = "jobs";

    private static final String DMF_FILE_LOCATION_PARAMETER = "dmf_file_location";

    private final String archiveStatusCommandAndArgs;

    private ProcessJobFactory processJobFactory;

    /**
     * Create a new DataAccessJobService instance.
     * 
     * @param dataAccessJobRepository
     *            The JPA repository for the DataAccessJob table.
     * @param imageCubeRepository
     *            The JPA repository for the ImageCube table.
     * @param measurementSetRepository
     *            The JPA repository for the MeasurementSet table.
     * @param cachedFileRepository
     *            The JPA repository for the CachedFile table.
     * @param ngasService
     *            The service to interact with NGAS
     * @param cacheDir
     *            The path to the directory used for storing retrieved files.
     * @param archiveStatusCommandAndArgs
     *            The command and args EL string used to find the file status in ngas
     * @param processJobFactory
     *            the factory to be used to create job processes.
     */
    @Autowired
    public DataAccessService(DataAccessJobRepository dataAccessJobRepository, ImageCubeRepository imageCubeRepository,
            MeasurementSetRepository measurementSetRepository, CachedFileRepository cachedFileRepository,
            NgasService ngasService, @Value("${cache.home.dir}") String cacheDir,
            @Value("${artefact.archive.status.command.and.args}") String archiveStatusCommandAndArgs,
            ProcessJobFactory processJobFactory)
    {
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.imageCubeRepository = imageCubeRepository;
        this.measurementSetRepository = measurementSetRepository;
        this.cachedFileRepository = cachedFileRepository;
        this.ngasService = ngasService;
        this.cacheDir = cacheDir;
        this.archiveStatusCommandAndArgs = archiveStatusCommandAndArgs;
        this.processJobFactory = processJobFactory;
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
     * @param requestId
     *            the data access job request id
     * @param files
     *            download files, with file size details to use to update the data access job record
     */
    public void updateFileSizeForCutouts(String requestId, Collection<DownloadFile> files)
    {
        DataAccessJob dataAccessJob = getExistingJob(requestId);
        Long originalJobSizeKb = dataAccessJob.getSizeKb();
        Long originalCutoutSize = 0L;
        Long updatedSizeKb = 0L;
        for (DownloadFile file : files)
        {
            switch (file.getFileType())
            {
            case IMAGE_CUTOUT:
                CutoutFileDescriptor cutoutFile = (CutoutFileDescriptor) file;
                for (ImageCutout imageCutout : dataAccessJob.getImageCutouts())
                {
                    if (imageCutout.getId().equals(cutoutFile.getCutoutId()))
                    {
                        originalCutoutSize += imageCutout.getFilesize();
                        imageCutout.setFilesize(file.getSizeKb());
                        updatedSizeKb += file.getSizeKb();
                    }
                }
                break;
            default:
                // skip anything that isn't a cutout
                break;
            }
        }

        dataAccessJob.setSizeKb(originalJobSizeKb - originalCutoutSize + updatedSizeKb);
        dataAccessJobRepository.save(dataAccessJob);
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
        dataAccessJobRepository.save(dataAccessJob);
    }

    /**
     * Checks if the data product file is in the cache.
     * 
     * @param dataAccessProduct
     *            the requested data product
     * @return true if the file is available in the cache, false otherwise.
     * @throws ResourceNotFoundException
     *             if the data product does not exist
     */
    public boolean isFileInCache(DataAccessDataProduct dataAccessProduct) throws ResourceNotFoundException
    {
        CasdaDataProductEntity artefact = findDataProduct(dataAccessProduct);
        if (artefact == null)
        {
            throw new ResourceNotFoundException(dataAccessProduct.getDataAccessProductType() + " with id "
                    + dataAccessProduct.getId() + " does not exist");
        }

        // Try to access in the data access cache
        String fileId = artefact.getFileId();
        CachedFile cachedFile = cachedFileRepository.findByFileId(fileId);
        return cachedFile != null && cachedFile.isFileAvailableFlag();
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
        CasdaDataProductEntity artefact = findDataProduct(dataAccessProduct);
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
     *             if there is no corresponding database record for this data product type and id
     * @throws ServiceCallException
     *             if there is a problem calling NGAS about the status or location of the file.
     */
    public Path findFileInNgasIfOnDisk(String fileId) throws ResourceNotFoundException, ServiceCallException
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
     * Finds the data product entity corresponding to the type and id.
     * 
     * @param dataAccessProduct
     *            the requested data product
     * @return the data product (or null if no matching data product could be found)
     */
    public CasdaDataProductEntity findDataProduct(DataAccessDataProduct dataAccessProduct)
    {
        switch (dataAccessProduct.getDataAccessProductType())
        {
        case cube:
            return imageCubeRepository.findOne(dataAccessProduct.getId());
        case visibility:
            return measurementSetRepository.findOne(dataAccessProduct.getId());
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
     * @throws ResourceNotFoundException
     *             if the file could not be found
     */
    public void downloadFile(DataAccessJob dataAccessJob, String filename, HttpServletResponse response,
            boolean skipCacheCheck) throws ResourceNotFoundException
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
             * SIAP_SYNC jobs may load data from NGAS so we try to read them directly from NGAS - see below. For
             * non-SIAP_SYNC jobs, failing to find the file in the cache is an error.
             */
            if (CasdaDownloadMode.SIAP_SYNC != dataAccessJob.getDownloadMode())
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

            IOUtils.copyLarge(file, servletOutput);
            response.flushBuffer();
        }
        catch (ClientAbortException e)
        {
            logger.info("Client cancelled download for file {} for request {} ", filepath,
                    dataAccessJob.getRequestId());
            return;
        }
        catch (Exception e)
        {
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

}
