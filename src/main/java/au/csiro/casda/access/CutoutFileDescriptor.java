package au.csiro.casda.access;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;

/**
 * File descriptor for image cutouts, contains reference to original image file.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class CutoutFileDescriptor extends FileDescriptor
{
    private Long cutoutId;
    private DownloadFile originalImageDownloadFile;
    private String originalImageFilePath;

    /**
     * Create a new CutoutFileDescriptor instance for a cutout and its original image file.
     * 
     * @param cutoutId
     *            The id of the cutout in the image cutout database table
     * @param fileId
     *            The file id of the cutout file
     * @param sizeKb
     *            The size of the cutout file in kilobytes.
     * @param originalImageFileId
     *            the original image file id (eg used in ngas)
     * @param originalImageFileSize
     *            the size in kilobytes of the original image file
     */
    public CutoutFileDescriptor(Long cutoutId, String fileId, long sizeKb, String originalImageFileId,
            long originalImageFileSize)
    {
        super(fileId, sizeKb, FileType.IMAGE_CUTOUT);
        this.cutoutId = cutoutId;
        this.originalImageDownloadFile =
                new FileDescriptor(originalImageFileId, originalImageFileSize, FileType.IMAGE_CUBE);
    }

    public Long getCutoutId()
    {
        return cutoutId;
    }

    public String getOriginalImageFilePath()
    {
        return originalImageFilePath;
    }

    public void setOriginalImageFilePath(String originalImageFilePath)
    {
        this.originalImageFilePath = originalImageFilePath;
    }

    public DownloadFile getOriginalImageDownloadFile()
    {
        return originalImageDownloadFile;
    }

    @Override
    public String getDisplayName()
    {
        return this.getFileId();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((cutoutId == null) ? 0 : cutoutId.hashCode());
        result = prime * result + ((originalImageDownloadFile == null) ? 0 : originalImageDownloadFile.hashCode());
        result = prime * result + ((originalImageFilePath == null) ? 0 : originalImageFilePath.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!super.equals(obj))
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        CutoutFileDescriptor other = (CutoutFileDescriptor) obj;
        if (cutoutId == null)
        {
            if (other.cutoutId != null)
            {
                return false;
            }
        }
        else if (!cutoutId.equals(other.cutoutId))
        {
            return false;
        }
        if (originalImageDownloadFile == null)
        {
            if (other.originalImageDownloadFile != null)
            {
                return false;
            }
        }
        else if (!originalImageDownloadFile.equals(other.originalImageDownloadFile))
        {
            return false;
        }
        if (originalImageFilePath == null)
        {
            if (other.originalImageFilePath != null)
            {
                return false;
            }
        }
        else if (!originalImageFilePath.equals(other.originalImageFilePath))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CutoutFileDescriptor [fileId=");
        builder.append(getFileId());
        builder.append(", displayName=");
        builder.append(getDisplayName());
        builder.append(", sizeKb=");
        builder.append(getSizeKb());
        builder.append(", fileType=");
        builder.append(getFileType());
        builder.append(", complete=");
        builder.append(isComplete());
        builder.append(", cutoutId=");
        builder.append(cutoutId);
        builder.append(", originalImageDownloadFile=");
        builder.append(originalImageDownloadFile);
        builder.append(", originalImageFilePath=");
        builder.append(originalImageFilePath);
        builder.append("]");
        return builder.toString();
    }

    
}
