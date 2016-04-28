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


import org.apache.commons.lang3.StringUtils;

/**
 * Available download formats for catalogue information.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
public enum CatalogueDownloadFormat
{
    /** CSV format, each catalogue should be an individual file */
    CSV_INDIVIDUAL,
    /** CSV format, catalogues should be grouped by type */
    CSV_GROUPED,
    /** VO Table format, each catalogue should be an individual file */
    VOTABLE_INDIVIDUAL,
    /** VO Table format, catalogues should be grouped by type */
    VOTABLE_GROUPED;

    private static final String CSV_EXTENSION = "csv";
    private static final String VO_TABLE_EXTENSION = "votable";

    public boolean isIndividual()
    {
        return this.name().endsWith("INDIVIDUAL");
    }

    public boolean isGrouped()
    {
        return this.name().endsWith("GROUPED");
    }

    public boolean isCsv()
    {
        return this.name().startsWith("CSV");
    }

    public boolean isVoTable()
    {
        return this.name().startsWith("VOTABLE");
    }

    /**
     * Gets the file extension associated with this download format.
     * 
     * @return eg csv, votable
     */
    public String getFileExtension()
    {
        if (this.isCsv())
        {
            return CSV_EXTENSION;
        }
        else if (this.isVoTable())
        {
            return VO_TABLE_EXTENSION;
        }
        return StringUtils.EMPTY;
    }
}
