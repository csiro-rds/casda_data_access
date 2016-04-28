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

import java.util.ArrayList;
import java.util.List;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.observation.CatalogueType;

/**
 * Represents one of the files we have generated in response to a user request to download catalogue information.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
public class CatalogueDownloadFile extends FileDescriptor
{
    private String filename;
    private List<Long> catalogueIds = new ArrayList<>();
    private CatalogueDownloadFormat downloadFormat;
    private CatalogueType catalogueType;
    private String qualifiedTablename;

    /**
     * Constructor, sets file type to CATALOGUE by default.
     */
    public CatalogueDownloadFile()
    {
        super();
        this.setFileType(FileType.CATALOGUE);
    }

    /**
     * Constructor with args, sets file type to CATALOGUE by default.
     * 
     * @param fileId
     *            the file id (corresponds to the file id in the CachedFile table)
     * @param sizeKb
     *            the size of the file
     * @param filename
     *            the name of the file (can be different to the file id, for display on the UI)
     * @param catalogueIds
     *            the list of catalogue ids associated with this download file
     * @param downloadFormat
     *            the requested download format, eg CSV_INDIVIDUAL
     * @param catalogueType
     *            the catalogue type
     * @param qualifiedTablename
     *            the qualified database table name if it is a level 7 catalogue
     */
    public CatalogueDownloadFile(String fileId, long sizeKb, String filename, List<Long> catalogueIds,
            CatalogueDownloadFormat downloadFormat, CatalogueType catalogueType, String qualifiedTablename)
    {
        super(fileId, sizeKb, FileType.CATALOGUE);
        this.filename = filename;
        this.catalogueIds.addAll(catalogueIds);
        this.downloadFormat = downloadFormat;
        this.catalogueType = catalogueType;
        this.qualifiedTablename = qualifiedTablename;
    }

    public String getFilename()
    {
        return filename;
    }

    public void setFilename(String filename)
    {
        this.filename = filename;
    }

    public List<Long> getCatalogueIds()
    {
        return catalogueIds;
    }

    public CatalogueDownloadFormat getDownloadFormat()
    {
        return downloadFormat;
    }

    public CatalogueType getCatalogueType()
    {
        return catalogueType;
    }

    public void setDownloadFormat(CatalogueDownloadFormat downloadFormat)
    {
        this.downloadFormat = downloadFormat;
    }

    public void setCatalogueType(CatalogueType catalogueType)
    {
        this.catalogueType = catalogueType;
    }

    public String getQualifiedTablename()
    {
        return qualifiedTablename;
    }

    public void setQualifiedTablename(String qualifiedTablename)
    {
        this.qualifiedTablename = qualifiedTablename;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((catalogueIds == null) ? 0 : catalogueIds.hashCode());
        result = prime * result + ((catalogueType == null) ? 0 : catalogueType.hashCode());
        result = prime * result + ((downloadFormat == null) ? 0 : downloadFormat.hashCode());
        result = prime * result + ((filename == null) ? 0 : filename.hashCode());
        result = prime * result + ((qualifiedTablename == null) ? 0 : qualifiedTablename.hashCode());
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
        CatalogueDownloadFile other = (CatalogueDownloadFile) obj;
        if (catalogueIds == null)
        {
            if (other.catalogueIds != null)
            {
                return false;
            }
        }
        else if (!catalogueIds.equals(other.catalogueIds))
        {
            return false;
        }
        if (catalogueType != other.catalogueType)
        {
            return false;
        }
        if (downloadFormat != other.downloadFormat)
        {
            return false;
        }
        if (filename == null)
        {
            if (other.filename != null)
            {
                return false;
            }
        }
        else if (!filename.equals(other.filename))
        {
            return false;
        }
        if (qualifiedTablename == null)
        {
            if (other.qualifiedTablename != null)
            {
                return false;
            }
        }
        else if (!qualifiedTablename.equals(other.qualifiedTablename))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("CatalogueDownloadFile [fileId=");
        builder.append(getFileId());
        builder.append(", displayName=");
        builder.append(getDisplayName());
        builder.append(", sizeKb=");
        builder.append(getSizeKb());
        builder.append(", fileType=");
        builder.append(getFileType());
        builder.append(", complete=");
        builder.append(isComplete());
        builder.append(", filename=");
        builder.append(filename);
        builder.append(", catalogueIds=");
        builder.append(catalogueIds);
        builder.append(", downloadFormat=");
        builder.append(downloadFormat);
        builder.append(", catalogueType=");
        builder.append(catalogueType);
        builder.append(", qualifiedTablename=");
        builder.append(qualifiedTablename);
        builder.append("]");
        return builder.toString();
    }

    
}
