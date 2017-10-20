package au.csiro.casda.access;

import org.apache.commons.lang3.StringUtils;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.observation.EncapsulationFile;

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
 * Represents one of the files we have generated in response to a user request to download moment map, cubelet 
 * or spectrum information.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
public class EncapsulatedFileDescriptor extends FileDescriptor 
{
	private FileDescriptor encapsulationFile;
	private String originalEncapsulationFilePath;

    /**
     * Create a new FileDescriptor instance for a file.
     * 
     * @param fileId
     *            The id of the file (e.g. as used by NGAS)
     * @param sizeKb
     *            The size of the file in kilobytes.
     * @param fileType
     *            the type of the file (eg CATALOGUE, MEASUREMENT_SET, IMAGE_CUBE)
     * @param displayName
     *            the file name (e.g. as originally in the observation)
     * @param encapsulationFile the encapsulationFile
     */
    public EncapsulatedFileDescriptor
    (String fileId, long sizeKb, FileType fileType, String displayName, EncapsulationFile encapsulationFile)
    {
        super(fileId, sizeKb, fileType, displayName);
        if(encapsulationFile != null)
        {
        	this.encapsulationFile = new FileDescriptor(encapsulationFile.getFileId(), encapsulationFile.getFilesize(), 
            		FileType.ENCAPSULATION_FILE, encapsulationFile.getFilename());
        }

    }
    
    /**
     * @param fileId the file id
     * @param sizeKb the file size in kb
     * @param fileType the file type
     * @param displayName the display name
     * @param encapsulationFileId the encapsulation file id
     * @param encapsulationFileSize the encapsulation file size
     * @param encapsulationFilename the ecnapsulation file name
     */
    public EncapsulatedFileDescriptor(String fileId, long sizeKb, FileType fileType, String displayName, 
    		String encapsulationFileId, Long encapsulationFileSize, String encapsulationFilename)
    {
        super(fileId, sizeKb, fileType, displayName);
        if(StringUtils.isNotBlank(encapsulationFileId))
        {
        	this.encapsulationFile = new FileDescriptor(encapsulationFileId, encapsulationFileSize, 
            		FileType.ENCAPSULATION_FILE, encapsulationFilename);
        }

    }

	public FileDescriptor getEncapsulationFile() 
	{
		return encapsulationFile;
	}

    public String getOriginalEncapsulationFilePath()
    {
        return originalEncapsulationFilePath;
    }

    public void setOriginalEncapsulationFilePath(String originalEncapsulationFilePath)
    {
        this.originalEncapsulationFilePath = originalEncapsulationFilePath;
    }
	
}
