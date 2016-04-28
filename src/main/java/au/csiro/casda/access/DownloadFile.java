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


import au.csiro.casda.entity.dataaccess.CachedFile.FileType;

/**
 * Interface for describing expected properties of a file that a user requested for download.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
public interface DownloadFile
{
    /**
     * The file id corresponds to the file id in the CachedFile table, this needs to be a unique filename in the cache.
     * 
     * @return the file id
     */
    public String getFileId();

    /**
     * @param fileId
     *            the file id
     */
    public void setFileId(String fileId);
    
    /**
     * @return the filename that corresponds to the filename on the filesystem.
     */
    public String getFilename();
    
    /**
     * @return the original filename for display in the UI
     */
    public String getDisplayName();

    /**
     * @return the size of the file in KB.
     */
    public long getSizeKb();

    /**
     * @param sizeKb
     *            the size of the file in KB.
     */
    public void setSizeKb(long sizeKb);

    /**
     * @return the file type
     */
    public FileType getFileType();

    /**
     * @param fileType
     *            the file type
     */
    public void setFileType(FileType fileType);

    /**
     * @return whether the download of the file has completed
     */
    public boolean isComplete();

    /**
     * @param complete
     *            whether the download of the file has completed
     */
    public void setComplete(boolean complete);
}
