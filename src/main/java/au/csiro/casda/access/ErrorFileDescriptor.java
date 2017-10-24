package au.csiro.casda.access;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;

/**
 * File descriptor for image cutout error messages, contains the text of the error message.
 * <p>
 * Copyright 2016, CSIRO Australia. All rights reserved.
 */
public class ErrorFileDescriptor extends FileDescriptor
{
    private String errorMessage;

    /**
     * Create a new ErrorFileDescriptor instance for a cutout error message.
     * 
     * @param fileId
     *            The file id of the cutout error file
     * @param errorMessage
     *            The text of the error message.
     */
    public ErrorFileDescriptor(String fileId, String errorMessage)
    {
        super(fileId, 1, FileType.ERROR);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage()
    {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage)
    {
        this.errorMessage = errorMessage;
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
        result = prime * result + ((errorMessage == null) ? 0 : errorMessage.hashCode());
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
        ErrorFileDescriptor other = (ErrorFileDescriptor) obj;
        if (errorMessage == null)
        {
            if (other.errorMessage != null)
            {
                return false;
            }
        }
        else if (!errorMessage.equals(other.errorMessage))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("ErrorFileDescriptor [fileId=");
        builder.append(getFileId());
        builder.append(", displayName=");
        builder.append(getDisplayName());
        builder.append(", sizeKb=");
        builder.append(getSizeKb());
        builder.append(", fileType=");
        builder.append(getFileType());
        builder.append(", complete=");
        builder.append(isComplete());
        builder.append(", errorMessage=");
        builder.append(errorMessage);
        builder.append("]");
        return builder.toString();
    }

}
