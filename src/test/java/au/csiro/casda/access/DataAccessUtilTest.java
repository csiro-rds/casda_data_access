package au.csiro.casda.access;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.dataaccess.ImageCutout;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.Level7Collection;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.ParentDepositableArtefact;
import au.csiro.casda.entity.observation.Project;

/**
 * Tests for DataAccessUtil
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
public class DataAccessUtilTest
{

    @Test
    public void testGetRelativeLinkForFile() throws Exception
    {
        for (CasdaDownloadMode downloadMode : CasdaDownloadMode.values())
        {
            String link = downloadMode == CasdaDownloadMode.PAWSEY_HTTP
                    ? "pawsey/requests/123-abc/some-file-name_with.extension"
                    : "requests/123-abc/some-file-name_with.extension";
            assertEquals(link,
                    DataAccessUtil.getRelativeLinkForFile(downloadMode, "123-abc", "some-file-name_with.extension"));
        }
    }

    @Test
    public void testGetRelativeLinkForFileChecksum() throws Exception
    {
        for (CasdaDownloadMode downloadMode : CasdaDownloadMode.values())
        {
            String link = downloadMode == CasdaDownloadMode.PAWSEY_HTTP
                    ? "pawsey/requests/123-abc/some-file-name_with.extension.checksum"
                    : "requests/123-abc/some-file-name_with.extension.checksum";
            assertEquals(link, DataAccessUtil.getRelativeLinkForFileChecksum(downloadMode, "123-abc",
                    "some-file-name_with.extension"));
        }
    }

    @Test
    public void testGetTimeDifferenceInHoursDisplayString()
    {
        DateTime start;
        DateTime end;

        start = DateTime.now(DateTimeZone.UTC);

        // less than 1 hour
        end = start.plusHours(1).minusMillis(1);
        assertEquals("less than 1 hour", DataAccessUtil.getTimeDifferenceInHoursDisplayString(start, end));

        // 1 hour
        end = start.plusHours(1);
        assertEquals("approximately 1 hour", DataAccessUtil.getTimeDifferenceInHoursDisplayString(start, end));

        // > 1 < 2 hour
        end = start.plusHours(2).minusMillis(1);
        assertEquals("approximately 1 hour", DataAccessUtil.getTimeDifferenceInHoursDisplayString(start, end));

        // > 2 hours
        int hours = RandomUtils.nextInt(2, 10);
        end = start.plusHours(hours);
        assertEquals("approximately " + hours + " hours",
                DataAccessUtil.getTimeDifferenceInHoursDisplayString(start, end));
    }

    @Test
    public void testFormatDateToUTC()
    {
        // null returns empty string
        assertEquals(StringUtils.EMPTY, DataAccessUtil.formatDateTimeToUTC(null));

        // otherwise should be dd/MM/yyyy HH:mm:ss UTC
        DateTime dateTimeInUtc = new DateTime(0, DateTimeZone.UTC);
        assertEquals("01/01/1970 00:00:00 UTC", DataAccessUtil.formatDateTimeToUTC(dateTimeInUtc));

        dateTimeInUtc = new DateTime(2014, 12, 12, 12, 01, DateTimeZone.UTC);
        assertEquals("12/12/2014 12:01:00 UTC", DataAccessUtil.formatDateTimeToUTC(dateTimeInUtc));

        DateTime dateTimeInCanberra = new DateTime(2014, 12, 12, 12, 01, DateTimeZone.forID("Australia/Canberra"));
        assertEquals("12/12/2014 01:01:00 UTC", DataAccessUtil.formatDateTimeToUTC(dateTimeInCanberra));

        DateTime dateTimeInPerth = new DateTime(2014, 12, 12, 12, 01, DateTimeZone.forID("Australia/Perth"));
        assertEquals("12/12/2014 04:01:00 UTC", DataAccessUtil.formatDateTimeToUTC(dateTimeInPerth));
    }

    @Test
    public void testGetCatalogueDownloadFilesCsvIndividual()
    {
        DataAccessJob job = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.CSV_INDIVIDUAL);
        List<DownloadFile> downloadFiles =
                DataAccessUtil.getCatalogueDownloadFiles(job, new File("src/test/resources"));
        assertEquals(5, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("AS123_Continuum_Component_Catalogue_12145_12.csv", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("AS123_Continuum_Component_Catalogue_12145_13.csv", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(2);
        assertEquals("AS124_Continuum_Island_Catalogue_12245_14.csv", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(3);
        assertEquals("AS133_Continuum_Island_Catalogue_12445_15.csv", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(4);
        assertEquals("AS523_Continuum_Component_Catalogue_12545_16.csv", downloadFile.getFilename());
    }

    @Test
    public void testGetCatalogueDownloadFilesCsvGrouped()
    {
        DataAccessJob job = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.CSV_GROUPED);
        job.setStatus(DataAccessJobStatus.READY);
        List<DownloadFile> downloadFiles =
                DataAccessUtil.getCatalogueDownloadFiles(job, new File("src/test/resources/jobs/12345"));
        assertEquals(2, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("Continuum_Component_Catalogue.csv", downloadFile.getFilename());
        System.out.println(job.getRequestId());
        assertEquals(1, downloadFile.getSizeKb());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("Continuum_Island_Catalogue.csv", downloadFile.getFilename());
    }

    @Test
    public void testGetCatalogueDownloadFilesVoTableIndividual()
    {
        DataAccessJob job = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        List<DownloadFile> downloadFiles =
                DataAccessUtil.getCatalogueDownloadFiles(job, new File("src/test/resources"));
        assertEquals(5, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("AS123_Continuum_Component_Catalogue_12145_12.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("AS123_Continuum_Component_Catalogue_12145_13.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(2);
        assertEquals("AS124_Continuum_Island_Catalogue_12245_14.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(3);
        assertEquals("AS133_Continuum_Island_Catalogue_12445_15.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(4);
        assertEquals("AS523_Continuum_Component_Catalogue_12545_16.votable", downloadFile.getFilename());
    }

    @Test
    public void testGetCatalogueDownloadFilesVoTableGrouped()
    {
        DataAccessJob job = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.VOTABLE_GROUPED);
        List<DownloadFile> downloadFiles =
                DataAccessUtil.getCatalogueDownloadFiles(job, new File("src/test/resources"));
        assertEquals(2, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("Continuum_Component_Catalogue.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("Continuum_Island_Catalogue.votable", downloadFile.getFilename());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoProject()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 13L, null);
        catalogue.setProject(null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNull(catalogue.getProject());
        assertNotNull(catalogue.getCatalogueType());
        assertNotNull(catalogue.getParent());
        assertNotNull(((Observation) catalogue.getParent()).getSbid());
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoProjectCode()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 15L, null);
        catalogue.getProject().setOpalCode(null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNotNull(catalogue.getProject());
        assertNull(catalogue.getProject().getOpalCode());
        assertNotNull(catalogue.getCatalogueType());
        assertNotNull(catalogue.getParent());
        assertNotNull(((Observation) catalogue.getParent()).getSbid());
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoCatalogueType()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 15L, null);
        catalogue.setCatalogueType(null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNotNull(catalogue.getProject());
        assertNotNull(catalogue.getProject().getOpalCode());
        assertNull(catalogue.getCatalogueType());
        assertNotNull(catalogue.getParent());
        assertNotNull(((Observation) catalogue.getParent()).getSbid());
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoObservation()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 16L, null);
        catalogue.setParent(null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNotNull(catalogue.getProject());
        assertNotNull(catalogue.getProject().getOpalCode());
        assertNotNull(catalogue.getCatalogueType());
        assertNull(catalogue.getParent());
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameWrongDownloadFormat()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 20L, null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_GROUPED;

        assertNotNull(catalogue.getProject());
        assertNotNull(catalogue.getProject().getOpalCode());
        assertNotNull(catalogue.getCatalogueType());
        assertNotNull(catalogue.getParent());
        assertNotNull(((Observation) catalogue.getParent()).getSbid());
        assertFalse(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test
    public void testGetIndividualCatalogueFilenameFormat()
    {
        Catalogue catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 21L, null);
        String filename =
                DataAccessUtil.getIndividualCatalogueFilename(catalogue, CatalogueDownloadFormat.CSV_INDIVIDUAL);
        assertEquals("AS123_Continuum_Component_Catalogue_123_21.csv", filename);

        catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_ISLAND, 22L, null);
        filename = DataAccessUtil.getIndividualCatalogueFilename(catalogue, CatalogueDownloadFormat.CSV_INDIVIDUAL);
        assertEquals("AS123_Continuum_Island_Catalogue_123_22.csv", filename);

        catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_ISLAND, 22L, null);
        filename = DataAccessUtil.getIndividualCatalogueFilename(catalogue, CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        assertEquals("AS123_Continuum_Island_Catalogue_123_22.votable", filename);

        catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 21L, null);
        filename = DataAccessUtil.getIndividualCatalogueFilename(catalogue, CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        assertEquals("AS123_Continuum_Component_Catalogue_123_21.votable", filename);

        catalogue = generateCatalogue("AS123", 123, CatalogueType.POLARISATION_COMPONENT, 23L, null);
        filename = DataAccessUtil.getIndividualCatalogueFilename(catalogue, CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        assertEquals("AS123_Polarisation_Component_Catalogue_123_23.votable", filename);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetGroupedCatalogueFilenameNullCatalogueType()
    {
        DataAccessUtil.getGroupedCatalogueFilename(null, CatalogueDownloadFormat.CSV_GROUPED);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetGroupedCatalogueFilenameNullCatalogueDownloadFormat()
    {
        DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.CONTINUUM_COMPONENT, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetGroupedCatalogueFilenameNullInvalidDownloadFormat()
    {
        DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.CONTINUUM_COMPONENT,
                CatalogueDownloadFormat.CSV_INDIVIDUAL);
    }

    @Test
    public void testGetGroupedCatalogueFilenameFormat()
    {
        String filename = DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.CONTINUUM_COMPONENT,
                CatalogueDownloadFormat.CSV_GROUPED);
        assertEquals("Continuum_Component_Catalogue.csv", filename);

        filename = DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.CONTINUUM_ISLAND,
                CatalogueDownloadFormat.CSV_GROUPED);
        assertEquals("Continuum_Island_Catalogue.csv", filename);

        filename = DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.CONTINUUM_ISLAND,
                CatalogueDownloadFormat.VOTABLE_GROUPED);
        assertEquals("Continuum_Island_Catalogue.votable", filename);

        filename = DataAccessUtil.getGroupedCatalogueFilename(CatalogueType.POLARISATION_COMPONENT,
                CatalogueDownloadFormat.VOTABLE_GROUPED);
        assertEquals("Polarisation_Component_Catalogue.votable", filename);
    }

    @Test
    public void testGetIndividualLevel7Catalogue()
    {
        String filename = DataAccessUtil.getLevel7CatalogueFilename(
                generateCatalogue("abc123", 111222, CatalogueType.LEVEL7, 1001L, "casda.table_name"),
                CatalogueDownloadFormat.CSV_INDIVIDUAL);
        assertEquals("abc123_Level7_Catalogue_table_name_1001.csv", filename);
    }

    @Test
    public void testGetMeasurementSetFiles()
    {
        assertEquals(0, DataAccessUtil.getMeasurementSetFiles(new ArrayList<>()).size());

        List<MeasurementSet> measurementSets = new ArrayList<>();
        MeasurementSet measurementSet = mock(MeasurementSet.class);
        when(measurementSet.getFileId()).thenReturn("measurement-set-file-id");
        when(measurementSet.getFilesize()).thenReturn(55L);
        measurementSets.add(measurementSet);

        List<DownloadFile> measurementSetFiles = DataAccessUtil.getMeasurementSetFiles(measurementSets);
        assertEquals(1, measurementSetFiles.size());
        DownloadFile measurementSetFile = measurementSetFiles.get(0);
        assertEquals("measurement-set-file-id", measurementSetFile.getFileId());
        assertEquals(55L, measurementSetFile.getSizeKb());
    }

    @Test
    public void testGetImageCubeFiles()
    {
        assertEquals(0, DataAccessUtil.getImageCubeFiles(new ArrayList<>()).size());

        List<ImageCube> imageCubes = new ArrayList<>();
        ImageCube measurementSet = mock(ImageCube.class);
        when(measurementSet.getFileId()).thenReturn("image-cube-file-id");
        when(measurementSet.getFilesize()).thenReturn(56L);
        imageCubes.add(measurementSet);

        List<DownloadFile> imageCubeFiles = DataAccessUtil.getImageCubeFiles(imageCubes);
        assertEquals(1, imageCubeFiles.size());
        DownloadFile imageCubeFile = imageCubeFiles.get(0);
        assertEquals("image-cube-file-id", imageCubeFile.getFileId());
        assertEquals(56L, imageCubeFile.getSizeKb());
    }

    @Test
    public void testGetImageCutoutFiles()
    {
        assertEquals(0, DataAccessUtil.getImageCutoutFiles(new ArrayList<>()).size());

        List<ImageCutout> imageCutouts = new ArrayList<>();
        ImageCutout imageCutout = new ImageCutout();
        imageCutout.setId(152L);
        imageCutout.setFilesize(1L);
        ImageCube imageCube = mock(ImageCube.class);
        when(imageCube.getId()).thenReturn(13L);
        when(imageCube.getFilesize()).thenReturn(16L);
        when(imageCube.getFileId()).thenReturn("image-cube-file-id");
        imageCutout.setImageCube(imageCube);
        imageCutouts.add(imageCutout);

        List<DownloadFile> imageCutoutFiles = DataAccessUtil.getImageCutoutFiles(imageCutouts);
        assertEquals(1, imageCutoutFiles.size());
        CutoutFileDescriptor imageCutoutFile = (CutoutFileDescriptor) imageCutoutFiles.get(0);
        assertEquals("cutout-152-imagecube-13.fits", imageCutoutFile.getFileId());
        assertEquals(1L, imageCutoutFile.getSizeKb());
        assertNull(imageCutoutFile.getOriginalImageFilePath());
        assertEquals(FileType.IMAGE_CUTOUT, imageCutoutFile.getFileType());
        assertEquals("image-cube-file-id", imageCutoutFile.getOriginalImageDownloadFile().getFileId());
        assertEquals(16L, imageCutoutFile.getOriginalImageDownloadFile().getSizeKb());
    }

    @Test
    public void testConvertBytesToKb()
    {
        assertNull(DataAccessUtil.convertBytesToKb(null));
        assertEquals(0L, DataAccessUtil.convertBytesToKb(0L).longValue());
        assertEquals(1L, DataAccessUtil.convertBytesToKb(1L).longValue());
        assertEquals(1L, DataAccessUtil.convertBytesToKb(1024L).longValue());
        assertEquals(2L, DataAccessUtil.convertBytesToKb(1025L).longValue());
    }

    @Test
    public void testBuildParamsMapMerge() throws Exception
    {
        Map<String, String[]> parameters = new TreeMap<String, String[]>();
        parameters.put(" one", new String[] { "a", "b" });
        parameters.put(" ONE  ", new String[] { "c", "d" });
        parameters.put("TWO  ", new String[] { "aa" });
        Map<String, String[]> paramsMap = DataAccessUtil.buildParamsMap(parameters);
        assertEquals(2, paramsMap.keySet().size());
        assertEquals(4, paramsMap.get("one").length);
        assertEquals(1, paramsMap.get("two").length);
        assertThat(paramsMap.get("one"), arrayContainingInAnyOrder("a", "b", "c", "d"));
        assertThat(paramsMap.get("two"), arrayContainingInAnyOrder("aa"));
    }

    public static DataAccessJob getDummyDataAccessJobCatalogues(CatalogueDownloadFormat downloadFormat)
    {
        DataAccessJob dataAccessJob = new DataAccessJob()
        {
            public long getFileCount()
            {
                return 1;
            }
        };
        dataAccessJob.setCreatedTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));
        dataAccessJob.setAvailableTimestamp(new DateTime(System.currentTimeMillis(), DateTimeZone.UTC));

        dataAccessJob.setSizeKb(1l);
        dataAccessJob.setRequestId("12345");
        dataAccessJob.setStatus(DataAccessJobStatus.PREPARING);
        dataAccessJob.setDownloadFormat(downloadFormat.name());

        Catalogue catalogue1 = generateCatalogue("AS123", 12145, CatalogueType.CONTINUUM_COMPONENT, 12L, null);
        Catalogue catalogue2 = generateCatalogue("AS123", 12145, CatalogueType.CONTINUUM_COMPONENT, 13L, null);
        Catalogue catalogue3 = generateCatalogue("AS124", 12245, CatalogueType.CONTINUUM_ISLAND, 14L, null);
        Catalogue catalogue4 = generateCatalogue("AS133", 12445, CatalogueType.CONTINUUM_ISLAND, 15L, null);
        Catalogue catalogue5 = generateCatalogue("AS523", 12545, CatalogueType.CONTINUUM_COMPONENT, 16L, null);

        dataAccessJob.addCatalogue(catalogue1);
        dataAccessJob.addCatalogue(catalogue2);
        dataAccessJob.addCatalogue(catalogue3);
        dataAccessJob.addCatalogue(catalogue4);
        dataAccessJob.addCatalogue(catalogue5);

        return dataAccessJob;
    }

    private static Catalogue generateCatalogue(String opalCode, int parentId, CatalogueType catalogueType,
            Long catalogueId, String tablename)
    {
        Project project = new Project();
        project.setOpalCode(opalCode);

        ParentDepositableArtefact parent = null;
        if (catalogueType == CatalogueType.LEVEL7)
        {
            parent = new Level7Collection(parentId);
        }
        else
        {
            parent = new Observation(parentId);
        }

        Catalogue catalogue = new Catalogue();
        catalogue.setId(catalogueId);
        catalogue.setCatalogueType(catalogueType);
        catalogue.setProject(project);
        catalogue.setParent(parent);
        if (catalogueType == CatalogueType.LEVEL7)
        {
            catalogue.setEntriesTableName(tablename);
        }
        return catalogue;
    }

}
