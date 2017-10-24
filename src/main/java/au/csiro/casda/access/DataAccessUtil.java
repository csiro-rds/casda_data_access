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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.csiro.casda.access.soda.RequestToken;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.Thumbnail;

/**
 * Utility methods for data access
 *
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class DataAccessUtil
{

    private static final Logger logger = LoggerFactory.getLogger(DataAccessUtil.class);
    /** Constant for the id param */
    public static final String ID = "id";
    /** Constant for the filesize param */
    public static final String FILE_SIZE = "filesize";
    /** Constant for the imagecubeid param */
    public static final String IMAGE_CUBE_ID = "imagecubeid";
    /** Constant for the imagecubesize param */
    public static final String IMAGE_CUBE_SIZE = "imagecubesize";
    /** Constant for the filename param */
    public static final String FILENAME = "filename";
    /** Constant for the obsid param */
    public static final String OBSERVATION_ID = "obsid";
    /** Constant for the l7id param */
    public static final String LEVEL7_ID = "l7id";
    /** Constant for the message param */
    public static final String MESSAGE = "message";
    /** Constant for the encapsulationfilename param */
    public static final String ENCAPSULATION_FILENAME = "encapsulationfilename";
    /** Constant for the encapsulationid param */
    public static final String ENCAPSULATION_ID = "encapsulationid";
    /** Constant for the encapsulationfilesize param */
    public static final String ENCAPSULATION_FILE_SIZE = "encapsulationfilesize";
    /** Constant for the projectid param */
    public static final String PROJECT_ID = "projectid";
    /** Constant for the cataloguetype param */
    public static final String CATALOGUE_TYPE = "cataloguetype";
    /** Constant for the tablename param */
    public static final String TABLE_NAME = "tablename";

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
        relativeLink.append(Utils.PAWSEY_DOWNLOADS.contains(downloadMode) ? "pawsey/" : "web/");
        relativeLink.append(requestId).append("/").append(filename);
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
     * @param catalogues the catalogue data to convert to download files
     * @param fileList the file list to add the catalogues to
     * @param job
     *            the data access job
     * @param jobDirectory
     *            the job filesystem directory location
     */
    public static void getCatalogueDownloadFiles(List<Map<String, Object>> catalogues, List<DownloadFile> fileList, 
    		File jobDirectory, DataAccessJob job)
    {
    	
        logger.debug("getting catalogue download files for job: {}", job.getRequestId());
        if (CollectionUtils.isEmpty(catalogues))
        {
            return;
        }
        List<DownloadFile> catFileList = new ArrayList<DownloadFile>();
        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.valueOf(job.getDownloadFormat().toUpperCase());
        if (downloadFormat.isIndividual())
        {
            for (Map<String, Object> catalogue : catalogues)
            {
                String filename = null;
                String qualifiedTablename = null;
                if (CatalogueType.valueOf((String) catalogue.get(CATALOGUE_TYPE)) == CatalogueType.DERIVED_CATALOGUE)
                {
                    filename = getLevel7CatalogueFilename(catalogue, downloadFormat);
                    qualifiedTablename = (String) catalogue.get(TABLE_NAME);
                }
                else
                {
                    filename = getIndividualCatalogueFilename(catalogue, downloadFormat);
                }

                CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
                downloadFile.setFilename(filename);
                downloadFile.setDisplayName((String)catalogue.get(FILENAME));
                downloadFile.getCatalogueIds().add((Long)catalogue.get(ID));
                downloadFile.setCatalogueType(CatalogueType.valueOf((String) catalogue.get(CATALOGUE_TYPE)));
                downloadFile.setQualifiedTablename(qualifiedTablename);
                catFileList.add(downloadFile);
            }
        }
        else if (downloadFormat.isGrouped())
        {
        	Set<CatalogueType> distinctCatalogueTypes = new TreeSet<CatalogueType>();
        	for(Map<String, Object> singleArtefact : catalogues)
        	{
        		distinctCatalogueTypes.add(CatalogueType.valueOf((String)singleArtefact.get(CATALOGUE_TYPE)));
        	}
            
            for (CatalogueType catalogueType : distinctCatalogueTypes)
            {
                CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
                downloadFile.setFilename(getGroupedCatalogueFilename(catalogueType, downloadFormat));
                downloadFile.setDisplayName(downloadFile.getFilename());
                for (Map<String, Object> catalogue : catalogues)
                {
                    if (CatalogueType.valueOf((String) catalogue.get(CATALOGUE_TYPE)) == catalogueType)
                    {
                        downloadFile.getCatalogueIds().add((Long) catalogue.get(ID));
                    }
                }
                downloadFile.setCatalogueType(catalogueType);
                catFileList.add(downloadFile);
            }
        }

        for (DownloadFile downloadFile : catFileList)
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
        fileList.addAll(catFileList);
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
    public static String getIndividualCatalogueFilename(Map<String, Object> catalogue, 
    		CatalogueDownloadFormat downloadFormat)
    {
        if (StringUtils.isBlank((String)catalogue.get(PROJECT_ID)))
        {
            throw new IllegalArgumentException("Catalogue must be associated with a project with a valid project code");
        }
        if (StringUtils.isBlank((String)catalogue.get(CATALOGUE_TYPE)))
        {
            throw new IllegalArgumentException("Catalogue must have a catalogue type set");
        }
        if ((Integer)catalogue.get(OBSERVATION_ID) == null)
        {
            throw new IllegalArgumentException("Catalogue must be associated with an observation with a valid sbid");
        }
        if (!downloadFormat.isIndividual())
        {
            throw new IllegalArgumentException("Download format need to be an individual type");
        }

        StringBuilder filename = new StringBuilder();
        filename.append((String)catalogue.get(PROJECT_ID));
        filename.append("_");
        String[] catalogueTypeNames = ((String)catalogue.get(CATALOGUE_TYPE)).split("_");
        for (String catalogueTypeName : catalogueTypeNames)
        {
            filename.append(StringUtils.capitalize(catalogueTypeName.toLowerCase()));
            filename.append("_");
        }
        filename.append("Catalogue");
        filename.append("_");
        if((Integer)catalogue.get(OBSERVATION_ID) != null)
        {
        	filename.append((Integer)catalogue.get(OBSERVATION_ID));
        }
        else
        {
        	filename.append((Long)catalogue.get(LEVEL7_ID));
        }
        filename.append("_");
        filename.append((Long)catalogue.get(ID));
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
    public static String getLevel7CatalogueFilename(Map<String, Object> catalogue, 
    		CatalogueDownloadFormat downloadFormat)
    {
        if (StringUtils.isBlank((String)catalogue.get(PROJECT_ID)))
        {
            throw new IllegalArgumentException("Catalogue must be associated with a project with a valid project code");
        }
        if (CatalogueType.valueOf((String)catalogue.get(CATALOGUE_TYPE)) != CatalogueType.DERIVED_CATALOGUE)
        {
            throw new IllegalArgumentException("Catalogue must be of type Level 7");
        }
        if ((Long)catalogue.get(LEVEL7_ID) == null)
        {
            throw new IllegalArgumentException(
                    "Catalogue must be associated with a level 7 collection with a valid collection id");
        }
        if (!downloadFormat.isIndividual())
        {
            throw new IllegalArgumentException("Download format needs to be an individual type");
        }

        StringBuilder filename = new StringBuilder();
        filename.append((String)catalogue.get(PROJECT_ID));
        filename.append("_");
        filename.append(CatalogueType.valueOf((String)catalogue.get(CATALOGUE_TYPE)).getDescription().replace(" ", "_"));
        filename.append("_");
        filename.append("Catalogue");
        filename.append("_");
        filename.append(((String)catalogue.get(TABLE_NAME)).replace("casda.", ""));
        filename.append("_");
        filename.append((Long)catalogue.get(ID));
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
     * The thumbnail file requested for download, this will hold file id, name and size information
     * 
     * @param thumbnail
     *            the thumbnail requested
     * @return the thumbnail file details
     */
	public static EncapsulatedFileDescriptor getThumbnailFile(Thumbnail thumbnail)
    {
        logger.debug("Getting thumbnail files");
        
        return new EncapsulatedFileDescriptor(thumbnail.getFileId(), thumbnail.getFilesize(), 
        		FileType.THUMBNAIL, thumbnail.getFilename(), thumbnail.getEncapsulationFile());
    }

    /**
     * The list of files requested for download, these hold the file id, filename and size information as well as 
     * encapsulation file
     * 
     * @param artefactDetails
     *            the list of encapsulated file details in the request
     * @param fileList the file list to add these files to      
     * @param fileType the type of file         
     */
	public static void getEncapsulatedFileDescriptor
    (List<Map<String, Object>> artefactDetails, List<DownloadFile> fileList, FileType fileType)
    {
    	logger.debug("Getting " + fileType.getCollectionName() + " files");
    	
    	for(Map<String, Object> singleArtefact : artefactDetails)
    	{
    		
    		String uniqueId = DataAccessUtil.compileUniqueFileId(singleArtefact.get(OBSERVATION_ID), 
    				singleArtefact.get(LEVEL7_ID), fileType.getCollectionName(),
    				(String)singleArtefact.get(FILENAME), (Long)singleArtefact.get(ID));
    		
    		String encapUniqueId= null;
    		if(StringUtils.isNotBlank((String)singleArtefact.get(ENCAPSULATION_FILENAME)))
    		{
        		encapUniqueId = DataAccessUtil.compileUniqueFileId(singleArtefact.get(OBSERVATION_ID), 
        				singleArtefact.get(LEVEL7_ID), FileType.ENCAPSULATION_FILE
        				.getCollectionName(), (String)singleArtefact.get(ENCAPSULATION_FILENAME)
        				, (Long)singleArtefact.get(ENCAPSULATION_ID));
    		}

    		//since a project team member is able to download l7 encapsulated files pre-approval, the undeposited encap
    		//file will have a null filesize.
    		Long fileSize = singleArtefact.get(ENCAPSULATION_FILE_SIZE) != null ? 
    				(Long)singleArtefact.get(ENCAPSULATION_FILE_SIZE) : 0;
    		
    		DownloadFile file = new EncapsulatedFileDescriptor(uniqueId, (Long)singleArtefact.get(FILE_SIZE), 
    				fileType, (String)singleArtefact.get(FILENAME), encapUniqueId, fileSize, 
    				(String)singleArtefact.get(ENCAPSULATION_FILENAME));
    		file.setId((Long)singleArtefact.get(ID));
    		fileList.add(file);
    	}
    }
	
    /**
     * The list of files requested for download, these hold the file id, filename and size information
     * 
     * @param artefactDetails
     *            the list of file details in the request
     * @param fileList the file list to add these files to      
     * @param fileType the type of file         
     */
	public static void getFileDescriptors
    (List<Map<String, Object>> artefactDetails, List<DownloadFile> fileList, FileType fileType)
    {
    	logger.debug("Getting " + fileType.getCollectionName() + " files");
    	
    	for(Map<String, Object> singleArtefact : artefactDetails)
    	{									
    		String uniqueId = DataAccessUtil.compileUniqueFileId(singleArtefact.get(OBSERVATION_ID), 
    				singleArtefact.get(LEVEL7_ID), fileType.getCollectionName(),
    				(String)singleArtefact.get(FILENAME), (Long)singleArtefact.get(ID));
    		
    		DownloadFile file = new FileDescriptor(uniqueId, 
    				(Long)singleArtefact.get(FILE_SIZE), fileType, (String)singleArtefact.get(FILENAME));
    		file.setId((Long)singleArtefact.get(ID));
    		fileList.add(file);
    	}
    }

	/**
     * @param artefactDetails
     *            the list of file details in the request
     * @param fileList the file list to add these files to      
     * @param fileType the type of file  
	 */
    public static void getGeneratedImageFiles(
    		List<Map<String, Object>> artefactDetails, List<DownloadFile> fileList, FileType fileType)
    {
    	logger.debug("Getting " + fileType.getCollectionName() + " files");
    	
    	for(Map<String, Object> singleArtefact : artefactDetails)
    	{
    	    String format = (String) singleArtefact.get("format");
    	    String fileExt = "png".equals(format) ?  "png" : "fits";
    		String fileId = (fileType == FileType.IMAGE_CUTOUT ? "cutout-" : "spectrum-") + 
    				(Long)singleArtefact.get(ID)+ "-imagecube-"+ (Long)singleArtefact.get(IMAGE_CUBE_ID) + "." + fileExt;
    		
    		GeneratedFileDescriptor fileDesc = new GeneratedFileDescriptor((Long)singleArtefact.get(ID),
    				fileId, (Long)singleArtefact.get(FILE_SIZE), compileUniqueFileId(singleArtefact.get(OBSERVATION_ID), 
    	    				singleArtefact.get(LEVEL7_ID), 
    				FileType.IMAGE_CUBE.getCollectionName(), (String)singleArtefact.get(FILENAME),
    				(Long)singleArtefact.get(IMAGE_CUBE_ID)), (Long)singleArtefact.get(IMAGE_CUBE_SIZE), fileType);
    		fileDesc.setId((Long)singleArtefact.get(ID));
    		fileDesc.setImageCubeId((Long)singleArtefact.get(IMAGE_CUBE_ID));
    		fileList.add(fileDesc);
    	}
    }

    /**
     * @param artefactDetails
     *            the list of file details in the request
     * @param fileList the file list to add these files to    
     */
	public static void getErrorFiles(List<Map<String, Object>> artefactDetails, List<DownloadFile> fileList)
	{
		for(Map<String, Object> error : artefactDetails)
		{
			ErrorFileDescriptor errorDesc = new ErrorFileDescriptor(String.format("error-%02d.txt", error.get("id")), 
					(String)error.get(MESSAGE));	
			errorDesc.setId((Long)error.get(ID));
			fileList.add(errorDesc);
		}
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
     * @param obsId the main SBID for an observation
     * @param l7Id the dap collection id for L7 collections
     * @param collectionName the collection name
     * @param filename the file name
     * @param id the id of the artefact
     * @return a unique identifier for this artefact
     */
    public static String compileUniqueFileId(Object obsId, Object l7Id, String collectionName, String filename, Long id)
    {
        final int maxNgasFileIdLen = 64;
        try
        {
    		String parentId = obsId == null ? "level7/" + l7Id : "observations/" + obsId;
    				
            String obsPrefix = obsId == null && l7Id == null ? "None" : parentId;
            
            String identifier = obsPrefix + "/" + collectionName + "/"
                    + URLEncoder.encode(filename, "UTF-8");
            if (identifier.length() > maxNgasFileIdLen || !filename.matches("^[A-Za-z0-9._-]+$"))
            {
                String extension = FilenameUtils.getExtension(filename);
                identifier = obsPrefix + "/" + collectionName + "/" + id
                		+ (extension.length() > 0 ? "." + extension : "");
            }
            return identifier.replace("/", "-");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param dataAccessJob the data access job
     * @param dataLinkAccessSecretKey the secret key needed to unencrypt the id param
     * @return true if cutouts need to be created for this data access job
     */
	public static boolean imageCutoutsShouldBeCreated(DataAccessJob dataAccessJob, String dataLinkAccessSecretKey) 
	{
		if(generatedFilesShouldBeCreated(dataAccessJob))
		{
			RequestToken token = new RequestToken(dataAccessJob.getParamMap().get(ParamKeyWhitelist.ID.name())[0], 
					dataLinkAccessSecretKey);
			return RequestToken.CUTOUT.equals(token.getDownloadMode());
		}
		return false;
	}
	
    /**
     * @param dataAccessJob the data access job
     * @param dataLinkAccessSecretKey the secret key needed to unencrypt the id param
     * @return true if spectra need to be created for this data access job
     */
	public static boolean spectrumShouldBeCreated(DataAccessJob dataAccessJob, String dataLinkAccessSecretKey) 
	{
		if(generatedFilesShouldBeCreated(dataAccessJob))
		{
            RequestToken token = new RequestToken(dataAccessJob.getParamMap().get(ParamKeyWhitelist.ID.name())[0], 
					dataLinkAccessSecretKey);
			return RequestToken.GENERATED_SPECTRUM.equals(token.getDownloadMode());
		}
		return false;
	}

    /**
     * Checks whether image cutouts should be created for the data access job.
     * 
     * @param dataAccessJob
     *            the data access job
     * @return whether image cutouts should be created for the data access job
     */
    public static boolean generatedFilesShouldBeCreated(DataAccessJob dataAccessJob)
    {		
        return !dataAccessJob.getParamMap().keySet().isEmpty()
                && ArrayUtils.isNotEmpty(dataAccessJob.getParamMap().get(ParamKeyWhitelist.ID.name()))
                && dataAccessJob.getParamMap().keySet().size() > 1;
    }
}
