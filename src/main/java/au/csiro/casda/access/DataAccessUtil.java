package au.csiro.casda.access;

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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.Level7Collection;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;

/**
 * Utility methods for data access
 *
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class DataAccessUtil
{

    private static final Logger logger = LoggerFactory.getLogger(DataAccessUtil.class);

    /**
     * Generate the relative url to download a file.
     * 
     * @param downloadMode
     *            the data access job download mode
     * @param requestId
     *            the request id
     * @param filename
     *            the filename
     * @return job/{requestId}/{filename} this maps to the {@link DataAccessDownloadController} download file method
     */
    public static String getRelativeLinkForFile(CasdaDownloadMode downloadMode, String requestId, String filename)
    {
        StringBuilder relativeLink = new StringBuilder();
        if (downloadMode == CasdaDownloadMode.PAWSEY_HTTP)
        {
            relativeLink.append("pawsey/");
        }
        relativeLink.append("requests/").append(requestId).append("/").append(filename);
        return relativeLink.toString();
    }

    /**
     * Generate the relative url to download a file's checksum file.
     * 
     * @param downloadMode
     *            the data access job download mode
     * @param requestId
     *            the request id
     * @param filename
     *            the name of the file whose checksum we want
     * @return job/{requestId}/{filename}.checksum this maps to the {@link DataAccessDownloadController} download file
     *         method
     */
    public static String getRelativeLinkForFileChecksum(CasdaDownloadMode downloadMode, String requestId,
            String filename)
    {
        return getRelativeLinkForFile(downloadMode, requestId, filename) + ".checksum";
    }

    /**
     * @param start
     *            the start DateTime
     * @param end
     *            the end DateTime
     * @return the number of hours between the two DateTimes as a human readable string
     */
    public static String getTimeDifferenceInHoursDisplayString(DateTime start, DateTime end)
    {
        StringBuilder remainingTimeText = new StringBuilder();
        long hoursRemaining = Duration
                .between(Instant.ofEpochMilli(start.getMillis()), Instant.ofEpochMilli(end.getMillis())).toHours();
        if (hoursRemaining < 1)
        {
            remainingTimeText.append("less than 1 hour");
        }
        else
        {
            remainingTimeText.append("approximately ");
            remainingTimeText.append(hoursRemaining);
            remainingTimeText.append(" hour");
            if (hoursRemaining > 1)
            {
                remainingTimeText.append("s");
            }
        }
        return remainingTimeText.toString();
    }

    /**
     * Formats a {@link DateTime} into our display format for UTC time.
     * 
     * @param dateTime
     *            the date/ time (with timezone) to format
     * @return date as a string with format: dd/MM/yyyy HH:mm:ss UTC
     */
    public static String formatDateTimeToUTC(DateTime dateTime)
    {
        StringBuilder formattedDate = new StringBuilder();
        if (dateTime != null)
        {
            formattedDate.append(DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").withZoneUTC().print(dateTime));
            formattedDate.append(" UTC");
        }
        return formattedDate.toString();
    }

    /**
     * Generates the list of catalogue download files for a given job.
     * 
     * @param job
     *            the data access job
     * @param jobDirectory
     *            the job filesystem directory location
     * @return the list of catalogue download files
     */
    public static List<DownloadFile> getCatalogueDownloadFiles(DataAccessJob job, File jobDirectory)
    {
        logger.debug("getting catalogue download files for job: {}", job.getRequestId());
        List<DownloadFile> downloadFiles = new ArrayList<>();
        if (CollectionUtils.isEmpty(job.getCatalogues()))
        {
            return downloadFiles;
        }
        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.valueOf(job.getDownloadFormat().toUpperCase());
        if (downloadFormat.isIndividual())
        {
            for (Catalogue catalogue : job.getCatalogues())
            {
                String filename = null;
                String qualifiedTablename = null;
                if (catalogue.getCatalogueType() == CatalogueType.LEVEL7)
                {
                    filename = getLevel7CatalogueFilename(catalogue, downloadFormat);
                    qualifiedTablename = catalogue.getEntriesTableName();
                }
                else
                {
                    filename = getIndividualCatalogueFilename(catalogue, downloadFormat);
                }

                CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
                downloadFile.setFilename(filename);
                downloadFile.setDisplayName(catalogue.getFilename());
                downloadFile.getCatalogueIds().add(catalogue.getId());
                downloadFile.setCatalogueType(catalogue.getCatalogueType());
                downloadFile.setQualifiedTablename(qualifiedTablename);
                downloadFiles.add(downloadFile);
            }
        }
        else if (downloadFormat.isGrouped())
        {
            List<CatalogueType> distinctCatalogueTypes = job.getCatalogues().stream()
                    .map(catalogue -> catalogue.getCatalogueType()).distinct().collect(Collectors.toList());
            for (CatalogueType catalogueType : distinctCatalogueTypes)
            {
                CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
                downloadFile.setFilename(getGroupedCatalogueFilename(catalogueType, downloadFormat));
                downloadFile.setDisplayName(downloadFile.getFilename());
                for (Catalogue catalogue : job.getCatalogues())
                {
                    if (catalogue.getCatalogueType() == catalogueType)
                    {
                        downloadFile.getCatalogueIds().add(catalogue.getId());
                    }
                }
                downloadFile.setCatalogueType(catalogueType);
                downloadFiles.add(downloadFile);
            }
        }

        for (DownloadFile downloadFile : downloadFiles)
        {
            CatalogueDownloadFile catalogueDownloadFile = (CatalogueDownloadFile) downloadFile;
            String fileId = job.getRequestId() + "-" + catalogueDownloadFile.getFilename();
            File file = new File(jobDirectory, catalogueDownloadFile.getFilename());

            if (job.isReady() && file.exists())
            {
                catalogueDownloadFile.setSizeKb(DataAccessUtil.convertBytesToKb(file.length()));
            }

            catalogueDownloadFile.setFileId(fileId);
            catalogueDownloadFile.setDownloadFormat(downloadFormat);
        }

        return downloadFiles;
    }

    /**
     * The filename of the catalogue download file when a user requests to download each catalogue individually.
     * 
     * @param catalogue
     *            the catalogue database record
     * @param downloadFormat
     *            the requested download format, eg CSV or VOTABLE
     * @return filename, eg AS007_Continuum_Component_Catalogue_1234.csv
     */
    public static String getIndividualCatalogueFilename(Catalogue catalogue, CatalogueDownloadFormat downloadFormat)
    {
        if (catalogue.getProject() == null || StringUtils.isBlank(catalogue.getProject().getOpalCode()))
        {
            throw new IllegalArgumentException("Catalogue must be associated with a project with a valid project code");
        }
        if (catalogue.getCatalogueType() == null)
        {
            throw new IllegalArgumentException("Catalogue must have a catalogue type set");
        }
        if (catalogue.getParent() == null || !(catalogue.getParent() instanceof Observation)
                || ((Observation) catalogue.getParent()).getSbid() == null)
        {
            throw new IllegalArgumentException("Catalogue must be associated with an observation with a valid sbid");
        }
        if (!downloadFormat.isIndividual())
        {
            throw new IllegalArgumentException("Download format need to be an individual type");
        }

        StringBuilder filename = new StringBuilder();
        filename.append(catalogue.getProject().getOpalCode());
        filename.append("_");
        String[] catalogueTypeNames = catalogue.getCatalogueType().name().split("_");
        for (String catalogueTypeName : catalogueTypeNames)
        {
            filename.append(StringUtils.capitalize(catalogueTypeName.toLowerCase()));
            filename.append("_");
        }
        filename.append("Catalogue");
        filename.append("_");
        filename.append(((Observation) catalogue.getParent()).getSbid());
        filename.append("_");
        filename.append(catalogue.getId());
        filename.append(".");
        filename.append(downloadFormat.getFileExtension());
        return filename.toString();
    }

    /**
     * The filename of the catalogue download file when a user requests to download a level 7 catalogue.
     * 
     * @param catalogue
     *            the catalogue database record
     * @param downloadFormat
     *            the requested download format, eg CSV or VOTABLE
     * @return filename, eg AS007_Level7_Catalogue_1234.csv
     */
    public static String getLevel7CatalogueFilename(Catalogue catalogue, CatalogueDownloadFormat downloadFormat)
    {
        if (catalogue.getProject() == null || StringUtils.isBlank(catalogue.getProject().getOpalCode()))
        {
            throw new IllegalArgumentException("Catalogue must be associated with a project with a valid project code");
        }
        if (catalogue.getCatalogueType() != CatalogueType.LEVEL7)
        {
            throw new IllegalArgumentException("Catalogue must be of type Level 7");
        }
        if (catalogue.getParent() == null || !(catalogue.getParent() instanceof Level7Collection)
                || catalogue.getParent().getUniqueId() == null)
        {
            throw new IllegalArgumentException(
                    "Catalogue must be associated with a level 7 collection with a valid collection id");
        }
        if (!downloadFormat.isIndividual())
        {
            throw new IllegalArgumentException("Download format needs to be an individual type");
        }

        StringBuilder filename = new StringBuilder();
        filename.append(catalogue.getProject().getOpalCode());
        filename.append("_");
        String[] catalogueTypeNames = catalogue.getCatalogueType().name().split("_");
        for (String catalogueTypeName : catalogueTypeNames)
        {
            filename.append(StringUtils.capitalize(catalogueTypeName.toLowerCase()));
            filename.append("_");
        }
        filename.append("Catalogue");
        filename.append("_");
        filename.append(catalogue.getEntriesTableName().replace("casda.", ""));
        filename.append("_");
        filename.append(catalogue.getId());
        filename.append(".");
        filename.append(downloadFormat.getFileExtension());
        return filename.toString();
    }

    /**
     * The filename of the catalogue download file when a user requests to download groups by catalogue type.
     * 
     * @param catalogueType
     *            the type of catalogue records to include (eg continuum_component, continuum_island)
     * @param downloadFormat
     *            the requested download format, eg CSV or VOTABLE
     * @return filename, eg Continuum_Component_Catalogue.csv
     */
    public static String getGroupedCatalogueFilename(CatalogueType catalogueType,
            CatalogueDownloadFormat downloadFormat)
    {
        if (catalogueType == null)
        {
            throw new IllegalArgumentException("Catalogue type cannot be null");
        }
        if (downloadFormat == null)
        {
            throw new IllegalArgumentException("Download format cannot be null");
        }
        if (!downloadFormat.isGrouped())
        {
            throw new IllegalArgumentException("Download format need to be a grouped type");
        }

        StringBuilder filename = new StringBuilder();
        String[] catalogueTypeNames = catalogueType.name().split("_");
        for (String catalogueTypeName : catalogueTypeNames)
        {
            filename.append(StringUtils.capitalize(catalogueTypeName.toLowerCase()));
            filename.append("_");
        }
        filename.append("Catalogue");
        filename.append(".");
        filename.append(downloadFormat.getFileExtension());
        return filename.toString();
    }

    /**
     * The list of measurement set files requested for download, these hold file id, name and size information
     * 
     * @param measurementSets
     *            the list of measurement sets in the request
     * @return the list of measurement set file details
     */
    public static List<DownloadFile> getMeasurementSetFiles(List<MeasurementSet> measurementSets)
    {
        logger.debug("Getting measurement set files");
        return measurementSets.stream().map(
                ms -> new FileDescriptor(ms.getFileId(), ms.getFilesize(), FileType.MEASUREMENT_SET, ms.getFilename()))
                .collect(Collectors.toList());
    }

    /**
     * The list of image cube files requested for download, these hold the file id, filename and size information
     * 
     * @param imageCubes
     *            the list of image cubes in the request
     * @return the list of image cube file details
     */
    public static List<DownloadFile> getImageCubeFiles(List<ImageCube> imageCubes)
    {
        logger.debug("Getting image cube files");
        return imageCubes.stream()
                .map(ic -> new FileDescriptor(ic.getFileId(), ic.getFilesize(), FileType.IMAGE_CUBE, ic.getFilename()))
                .collect(Collectors.toList());
    }

    /**
     * The list of image cutout files requested for download, these hold the file id, filename and size information
     * 
     * @param imageCutout
     *            the list of image cutouts in the request
     * @return the list of image cutout file details
     */
    public static List<DownloadFile> getImageCutoutFiles(List<ImageCutout> imageCutout)
    {
        logger.debug("Getting image cutout files");
        return imageCutout.stream()
                .map(cutout -> new CutoutFileDescriptor(cutout.getId(),
                        "cutout-" + cutout.getId() + "-imagecube-" + cutout.getImageCube().getId() + ".fits",
                        cutout.getFilesize(), cutout.getImageCube().getFileId(), cutout.getImageCube().getFilesize()))
                .collect(Collectors.toList());
    }

    /**
     * The list of all download files for a given job, including image cubes, measurement sets and catalogues
     * 
     * @param job
     *            the data access job
     * @param jobDir
     *            the location of the directory for the download job
     * @return the list of download file information
     */
    public static Collection<DownloadFile> getDataAccessJobDownloadFiles(DataAccessJob job, File jobDir)
    {
        Collection<DownloadFile> files = new ArrayList<>();
        files.addAll(DataAccessUtil.getImageCubeFiles(job.getImageCubes()));
        files.addAll(DataAccessUtil.getMeasurementSetFiles(job.getMeasurementSets()));
        files.addAll(DataAccessUtil.getCatalogueDownloadFiles(job, jobDir));
        files.addAll(DataAccessUtil.getImageCutoutFiles(job.getImageCutouts()));
        return files;
    }

    /**
     * Converts a value in bytes to kilobytes.
     * 
     * @param sizeBytes
     *            the size in bytes to convert
     * @return null if the input is null, otherwise the ceiling of the byte to kb converted value
     */
    public static Long convertBytesToKb(Long sizeBytes)
    {
        if (sizeBytes == null)
        {
            return null;
        }
        return (long) Math.ceil((double) sizeBytes / FileUtils.ONE_KB);
    }

    /**
     * Converts a value in bytes to gigabytes.
     * 
     * @param sizeBytes
     *            the size in bytes to convert
     * @return null if the input is null, otherwise the ceiling of the byte to GB converted value
     */
    public static Long convertBytesToGb(Long sizeBytes)
    {
        if (sizeBytes == null)
        {
            return null;
        }
        return (long) Math.ceil((double) sizeBytes / FileUtils.ONE_GB);
    }

    /**
     * Creates a new map from the input map, but ensuring that all keys are lowercase.
     * 
     * @param requestParams
     *            The map of parameters from the web request.
     * @return The map of parameters with lower case keys.
     */
    public static Map<String, String[]> buildParamsMap(Map<String, String[]> requestParams)
    {
        Map<String, String[]> paramsMap = new HashMap<>();
        for (Entry<String, String[]> entry : requestParams.entrySet())
        {
            String key = entry.getKey().trim().toLowerCase();
            if (paramsMap.containsKey(key))
            {
                paramsMap.put(key, ArrayUtils.addAll(paramsMap.get(key), entry.getValue()));
            }
            else
            {
                paramsMap.put(key, entry.getValue());
            }
        }
        return paramsMap;
    }

    /**
     * Checks whether image cutouts should be created for the data access job.
     * 
     * @param dataAccessJob
     *            the data access job
     * @return whether image cutouts should be created for the data access job
     */
    public static boolean imageCutoutsShouldBeCreated(DataAccessJob dataAccessJob)
    {
        return dataAccessJob.getDownloadMode() == CasdaDownloadMode.SIAP_ASYNC
                && !dataAccessJob.getParamMap().keySet().isEmpty()
                && ArrayUtils.isNotEmpty(dataAccessJob.getParamMap().get(ParamKeyWhitelist.ID.name()))
                && dataAccessJob.getParamMap().keySet().size() > 1;
    }
}
