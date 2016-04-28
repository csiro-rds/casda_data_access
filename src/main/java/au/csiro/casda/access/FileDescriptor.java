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
 * Describes the properties of a file required for retrieval from archive.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class FileDescriptor implements DownloadFile
{

    /** Unique archive file id */
    private String fileId;

    /** The original file name for display **/
    private String displayName;
    
    /** File size */
    private long sizeKb;

    private FileType fileType;

    private boolean complete;

    /**
     * Simple constructor
     */
    public FileDescriptor()
    {
    }

    /**
     * Create a new FileDescriptor instance for a file.
     * 
     * @param fileId
     *            The id of the file (e.g. as used by NGAS)
     * @param sizeKb
     *            The size of the file in kilobytes.
     * @param fileType
     *            the type of the file (eg CATALOGUE, MEASUREMENT_SET, IMAGE_CUBE)
     */
    public FileDescriptor(String fileId, long sizeKb, FileType fileType)
    {
        this.fileId = fileId;
        this.sizeKb = sizeKb;
        this.fileType = fileType;
        this.complete = false;
    }

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
     */
    public FileDescriptor(String fileId, long sizeKb, FileType fileType, String displayName)
    {
        this(fileId, sizeKb, fileType);
        this.displayName = displayName;
    }

    public String getFileId()
    {
        return fileId;
    }

    public long getSizeKb()
    {
        return sizeKb;
    }

    public void setFileId(String fileId)
    {
        this.fileId = fileId;
    }
    
    public String getFilename()
    {
        return this.fileId;
    }
    
    public String getDisplayName()
    {
        return this.displayName;
    }
    
    public void setDisplayName(String displayName)
    {
        this.displayName = displayName;
    }

    public void setSizeKb(long sizeKb)
    {
        this.sizeKb = sizeKb;
    }

    public FileType getFileType()
    {
        return fileType;
    }

    public void setFileType(FileType fileType)
    {
        this.fileType = fileType;
    }

    public boolean isComplete()
    {
        return complete;
    }

    public void setComplete(boolean complete)
    {
        this.complete = complete;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (complete ? 1231 : 1237);
        result = prime * result + ((displayName == null) ? 0 : displayName.hashCode());
        result = prime * result + ((fileId == null) ? 0 : fileId.hashCode());
        result = prime * result + ((fileType == null) ? 0 : fileType.hashCode());
        result = prime * result + (int) (sizeKb ^ (sizeKb >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        FileDescriptor other = (FileDescriptor) obj;
        if (complete != other.complete)
        {
            return false;
        }
        if (displayName == null)
        {
            if (other.displayName != null)
            {
                return false;
            }
        }
        else if (!displayName.equals(other.displayName))
        {
            return false;
        }
        if (fileId == null)
        {
            if (other.fileId != null)
            {
                return false;
            }
        }
        else if (!fileId.equals(other.fileId))
        {
            return false;
        }
        if (fileType != other.fileType)
        {
            return false;
        }
        if (sizeKb != other.sizeKb)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("FileDescriptor [fileId=");
        builder.append(fileId);
        builder.append(", displayName=");
        builder.append(displayName);
        builder.append(", sizeKb=");
        builder.append(sizeKb);
        builder.append(", fileType=");
        builder.append(fileType);
        builder.append(", complete=");
        builder.append(complete);
        builder.append("]");
        return builder.toString();
    }
}