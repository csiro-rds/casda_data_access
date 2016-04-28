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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the catalogue download format enum.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
public class CatalogueDownloadFormatTest
{

    @Test
    public void testCsvIndividual()
    {
        assertTrue(CatalogueDownloadFormat.CSV_INDIVIDUAL.isIndividual());
        assertTrue(CatalogueDownloadFormat.CSV_INDIVIDUAL.isCsv());
        assertFalse(CatalogueDownloadFormat.CSV_INDIVIDUAL.isGrouped());
        assertFalse(CatalogueDownloadFormat.CSV_INDIVIDUAL.isVoTable());
        assertEquals("csv", CatalogueDownloadFormat.CSV_INDIVIDUAL.getFileExtension());
    }

    @Test
    public void testCsvGrouped()
    {
        assertFalse(CatalogueDownloadFormat.CSV_GROUPED.isIndividual());
        assertTrue(CatalogueDownloadFormat.CSV_GROUPED.isCsv());
        assertTrue(CatalogueDownloadFormat.CSV_GROUPED.isGrouped());
        assertFalse(CatalogueDownloadFormat.CSV_GROUPED.isVoTable());
        assertEquals("csv", CatalogueDownloadFormat.CSV_GROUPED.getFileExtension());
    }

    @Test
    public void testVoTableIndividual()
    {
        assertTrue(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL.isIndividual());
        assertFalse(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL.isCsv());
        assertFalse(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL.isGrouped());
        assertTrue(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL.isVoTable());
        assertEquals("votable", CatalogueDownloadFormat.VOTABLE_INDIVIDUAL.getFileExtension());
    }

    @Test
    public void testVoTableGrouped()
    {
        assertFalse(CatalogueDownloadFormat.VOTABLE_GROUPED.isIndividual());
        assertFalse(CatalogueDownloadFormat.VOTABLE_GROUPED.isCsv());
        assertTrue(CatalogueDownloadFormat.VOTABLE_GROUPED.isGrouped());
        assertTrue(CatalogueDownloadFormat.VOTABLE_GROUPED.isVoTable());
        assertEquals("votable", CatalogueDownloadFormat.VOTABLE_GROUPED.getFileExtension());
    }

}
