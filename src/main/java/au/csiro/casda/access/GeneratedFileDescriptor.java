package au.csiro.casda.access;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;

/**
 * File descriptor for generated files such as cutouts and custom spectra, contains reference to original image file.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class GeneratedFileDescriptor extends FileDescriptor
{
	private Long imageCubeId;
    private Long generatedFileId;
    private DownloadFile originalImageDownloadFile;
    private String originalImageFilePath;

    /**
     * Create a new GeneratedFileDescriptor instance for a generated file and its original image file.
     * 
     * @param generatedFileId
     *            The id of the generated file  in the database table
     * @param fileId
     *            The file id of the generated file
     * @param sizeKb
     *            The size of the generated file file in kilobytes.
     * @param originalImageFileId
     *            the original image file id (eg used in ngas)
     * @param originalImageFileSize
     *            the size in kilobytes of the original image file
     * @param fileType
     *            the type of artefact this descriptor represents
     */
    public GeneratedFileDescriptor(Long generatedFileId, String fileId, long sizeKb, String originalImageFileId,
            long originalImageFileSize, FileType fileType)
    {
        super(fileId, sizeKb, fileType);
        this.generatedFileId = generatedFileId;
        this.originalImageDownloadFile =
                new FileDescriptor(originalImageFileId, originalImageFileSize, FileType.IMAGE_CUBE);
    }
    
    public Long getImageCubeId()
    {
		return imageCubeId;
	}

	public void setImageCubeId(Long imageCubeId) 
	{
		this.imageCubeId = imageCubeId;
	}

	public Long getGeneratedFileId()
    {
        return generatedFileId;
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
        result = prime * result + ((generatedFileId == null) ? 0 : generatedFileId.hashCode());
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
        GeneratedFileDescriptor other = (GeneratedFileDescriptor) obj;
        if (generatedFileId == null)
        {
            if (other.generatedFileId != null)
            {
                return false;
            }
        }
        else if (!generatedFileId.equals(other.generatedFileId))
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
        builder.append("GeneratedFileDescriptor [fileId=");
        builder.append(getFileId());
        builder.append(", displayName=");
        builder.append(getDisplayName());
        builder.append(", sizeKb=");
        builder.append(getSizeKb());
        builder.append(", fileType=");
        builder.append(getFileType());
        builder.append(", complete=");
        builder.append(isComplete());
        builder.append(", generatedFileId=");
        builder.append(generatedFileId);
        builder.append(", originalImageDownloadFile=");
        builder.append(originalImageDownloadFile);
        builder.append(", originalImageFilePath=");
        builder.append(originalImageFilePath);
        builder.append("]");
        return builder.toString();
    }

    
}
