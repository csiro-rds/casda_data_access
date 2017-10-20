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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import au.csiro.casda.access.util.Utils;
import au.csiro.casda.entity.dataaccess.CachedFile.FileType;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.EncapsulationFile;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Thumbnail;

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
            String link = Utils.PAWSEY_DOWNLOADS.contains(downloadMode)
                    ? "pawsey/123-abc/some-file-name_with.extension"
                    : "web/123-abc/some-file-name_with.extension";
            assertEquals(link,
                    DataAccessUtil.getRelativeLinkForFile(downloadMode, "123-abc", "some-file-name_with.extension"));
        }
    }

    @Test
    public void testGetRelativeLinkForFileChecksum() throws Exception
    {
        for (CasdaDownloadMode downloadMode : CasdaDownloadMode.values())
        {
            String link = Utils.PAWSEY_DOWNLOADS.contains(downloadMode)
                    ? "pawsey/123-abc/some-file-name_with.extension.checksum"
                    : "web/123-abc/some-file-name_with.extension.checksum";
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
        List<DownloadFile> downloadFiles = new ArrayList<DownloadFile>();
        DataAccessUtil.getCatalogueDownloadFiles(getDummyCatalogues(), downloadFiles, 
        		new File("src/test/resources"), job);
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
        List<DownloadFile> downloadFiles = new ArrayList<DownloadFile>();
        DataAccessUtil.getCatalogueDownloadFiles(getDummyCatalogues(), downloadFiles, 
        		new File("src/test/resources/jobs/12345"), job);
        
        assertEquals(2, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("Continuum_Component_Catalogue.csv", downloadFile.getFilename());
        assertEquals(1, downloadFile.getSizeKb());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("Continuum_Island_Catalogue.csv", downloadFile.getFilename());
    }

    @Test
    public void testGetCatalogueDownloadFilesVoTableIndividual()
    {
        DataAccessJob job = getDummyDataAccessJobCatalogues(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);       
        List<DownloadFile> downloadFiles = new ArrayList<DownloadFile>();
        DataAccessUtil.getCatalogueDownloadFiles(getDummyCatalogues(), downloadFiles, 
        		new File("src/test/resources"), job);
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
        List<DownloadFile> downloadFiles = new ArrayList<DownloadFile>();
        DataAccessUtil.getCatalogueDownloadFiles(getDummyCatalogues(), downloadFiles, 
        		new File("src/test/resources"), job);
        assertEquals(2, downloadFiles.size());
        CatalogueDownloadFile downloadFile = (CatalogueDownloadFile) downloadFiles.get(0);
        assertEquals("Continuum_Component_Catalogue.votable", downloadFile.getFilename());
        downloadFile = (CatalogueDownloadFile) downloadFiles.get(1);
        assertEquals("Continuum_Island_Catalogue.votable", downloadFile.getFilename());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoProject()
    {
        Map<String, Object> catalogue = generateCatalogue(null, 123, CatalogueType.CONTINUUM_COMPONENT, 13L, null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNull(catalogue.get(DataAccessUtil.PROJECT_ID));
        assertNotNull(catalogue.get(DataAccessUtil.CATALOGUE_TYPE));
        assertNotNull(catalogue.get(DataAccessUtil.OBSERVATION_ID));
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoCatalogueType()
    {
        Map<String, Object> catalogue = generateCatalogue("AS123", 123, null, 15L, null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNotNull(catalogue.get(DataAccessUtil.PROJECT_ID));
        assertNull(catalogue.get(DataAccessUtil.CATALOGUE_TYPE));
        assertNotNull(catalogue.get(DataAccessUtil.OBSERVATION_ID));
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameNoObservation()
    {
        Map<String, Object> catalogue = generateCatalogue("AS123", null, CatalogueType.CONTINUUM_COMPONENT, 16L, null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_INDIVIDUAL;

        assertNotNull(catalogue.get(DataAccessUtil.PROJECT_ID));
        assertNotNull(catalogue.get(DataAccessUtil.CATALOGUE_TYPE));
        assertNull(catalogue.get(DataAccessUtil.OBSERVATION_ID));
        assertTrue(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndividualCatalogueFilenameWrongDownloadFormat()
    {
        Map<String, Object> catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 20L, null);

        CatalogueDownloadFormat downloadFormat = CatalogueDownloadFormat.CSV_GROUPED;

        assertNotNull(catalogue.get(DataAccessUtil.PROJECT_ID));
        assertNotNull(catalogue.get(DataAccessUtil.CATALOGUE_TYPE));
        assertNotNull(catalogue.get(DataAccessUtil.OBSERVATION_ID));
        assertFalse(downloadFormat.isIndividual());

        DataAccessUtil.getIndividualCatalogueFilename(catalogue, downloadFormat);
    }

    @Test
    public void testGetIndividualCatalogueFilenameFormat()
    {
        Map<String, Object> catalogue = generateCatalogue("AS123", 123, CatalogueType.CONTINUUM_COMPONENT, 21L, null);
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
                generateCatalogue("abc123", 111222, CatalogueType.DERIVED_CATALOGUE, 1001L, "casda.table_name"),
                CatalogueDownloadFormat.CSV_INDIVIDUAL);
        assertEquals("abc123_Derived_Catalogue_table_name_1001.csv", filename);
    }

    @Test
    public void testGetMeasurementSetFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	
    	DataAccessUtil.getFileDescriptors(results, fileList, FileType.MEASUREMENT_SET);
        assertEquals(0, fileList.size());
        
        Map<String, Object> measurementSet = new HashMap<String, Object>();
        measurementSet.put(DataAccessUtil.ID, 23L);
        measurementSet.put(DataAccessUtil.OBSERVATION_ID, 13L);
        measurementSet.put(DataAccessUtil.FILENAME, "my_measurement_set");
        measurementSet.put(DataAccessUtil.FILE_SIZE, 382L);
        results.add(measurementSet);
        
        DataAccessUtil.getFileDescriptors(results, fileList, FileType.MEASUREMENT_SET);
        assertEquals(1, fileList.size());
        DownloadFile measurementSetFile = fileList.get(0);
        assertEquals("observations-13-measurement_sets-my_measurement_set", measurementSetFile.getFileId());
        assertEquals(382L, measurementSetFile.getSizeKb());
    }

    @Test
    public void testGetImageCubeFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getFileDescriptors(results, fileList, FileType.IMAGE_CUBE);
        assertEquals(0, fileList.size());

        Map<String, Object> imageCube = new HashMap<String, Object>();
        imageCube.put(DataAccessUtil.ID, 23L);
        imageCube.put(DataAccessUtil.OBSERVATION_ID, 13L);
        imageCube.put(DataAccessUtil.FILENAME, "my_image_cube");
        imageCube.put(DataAccessUtil.FILE_SIZE, 56L);
        results.add(imageCube);

    	DataAccessUtil.getFileDescriptors(results, fileList, FileType.IMAGE_CUBE);
        assertEquals(1, fileList.size());
        DownloadFile imageCubeFile = fileList.get(0);
        assertEquals("observations-13-image_cubes-my_image_cube", imageCubeFile.getFileId());
        assertEquals(56L, imageCubeFile.getSizeKb());
    }

    @Test
    public void testGetImageCutoutFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getGeneratedImageFiles(results, fileList, FileType.IMAGE_CUTOUT);
        assertEquals(0, fileList.size());

        Map<String, Object> imageCutout = new HashMap<String, Object>();
        imageCutout.put(DataAccessUtil.ID, 152L);
        imageCutout.put(DataAccessUtil.FILE_SIZE, 1L);
        imageCutout.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        imageCutout.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        imageCutout.put(DataAccessUtil.ID, 152L);
        imageCutout.put(DataAccessUtil.FILENAME, "my_image.fits");
        results.add(imageCutout);
        
    	DataAccessUtil.getGeneratedImageFiles(results, fileList, FileType.IMAGE_CUTOUT);
        assertEquals(1, fileList.size());
        GeneratedFileDescriptor imageCutoutFile = (GeneratedFileDescriptor) fileList.get(0);
        assertEquals("cutout-152-imagecube-13.fits", imageCutoutFile.getFileId());
        assertEquals(1L, imageCutoutFile.getSizeKb());
        assertNull(imageCutoutFile.getOriginalImageFilePath());
        assertEquals(FileType.IMAGE_CUTOUT, imageCutoutFile.getFileType());
        assertEquals("None-image_cubes-my_image.fits", imageCutoutFile.getOriginalImageDownloadFile().getFileId());
        assertEquals(16L, imageCutoutFile.getOriginalImageDownloadFile().getSizeKb());
    }
    
    @Test
    public void testGetGeneratedSpectrumFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getGeneratedImageFiles(results, fileList, FileType.GENERATED_SPECTRUM);
        assertEquals(0, fileList.size());

        Map<String, Object> spectrum = new HashMap<String, Object>();
        spectrum.put(DataAccessUtil.ID, 152L);
        spectrum.put(DataAccessUtil.FILE_SIZE, 1L);
        spectrum.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        spectrum.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        spectrum.put(DataAccessUtil.ID, 152L);
        spectrum.put(DataAccessUtil.OBSERVATION_ID, 224477);
        spectrum.put(DataAccessUtil.FILENAME, "my_image.fits");
        results.add(spectrum);

    	DataAccessUtil.getGeneratedImageFiles(results, fileList, FileType.GENERATED_SPECTRUM);
        assertEquals(1, fileList.size());
        GeneratedFileDescriptor spectrumFile = (GeneratedFileDescriptor) fileList.get(0);
        assertEquals("spectrum-152-imagecube-13.fits", spectrumFile.getFileId());
        assertEquals(1L, spectrumFile.getSizeKb());
        assertNull(spectrumFile.getOriginalImageFilePath());
        assertEquals(FileType.GENERATED_SPECTRUM, spectrumFile.getFileType());
        assertEquals(
        		"observations-224477-image_cubes-my_image.fits", spectrumFile.getOriginalImageDownloadFile().getFileId());
        assertEquals(16L, spectrumFile.getOriginalImageDownloadFile().getSizeKb());
    }
    
    @Test
    public void testGetMomentMapFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.MOMENT_MAP);
        assertEquals(0, fileList.size());

        EncapsulationFile encapFile = new EncapsulationFile();
        encapFile.setId(123L);
        encapFile.setFilename("encaps_1.tar");
        encapFile.setFilesize(34L);
        
        Map<String, Object> momentMapEncap = new HashMap<String, Object>();
        momentMapEncap.put(DataAccessUtil.ID, 152L);
        momentMapEncap.put(DataAccessUtil.FILE_SIZE, 3L);
        momentMapEncap.put(DataAccessUtil.OBSERVATION_ID, 12345);
        momentMapEncap.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        momentMapEncap.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        momentMapEncap.put(DataAccessUtil.FILENAME, "mm-2.fits");
        momentMapEncap.put("encapsulationfilename", "encap_1.tar");
        momentMapEncap.put("encapsulationid", 666L);
        momentMapEncap.put("encapsulationfilesize", 66L);
        momentMapEncap.put("imagecubesize", 16L);
        
        Map<String, Object> momentMapOrig = new HashMap<String, Object>();
        momentMapOrig.put(DataAccessUtil.ID, 152L);
        momentMapOrig.put(DataAccessUtil.FILE_SIZE, 1L);
        momentMapOrig.put(DataAccessUtil.OBSERVATION_ID, 12345);
        momentMapOrig.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        momentMapOrig.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        momentMapOrig.put(DataAccessUtil.FILENAME, "mm_1.fits");
        
        results.add(momentMapEncap);
        results.add(momentMapOrig);

    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.MOMENT_MAP);

        assertEquals(2, fileList.size());
        
        EncapsulatedFileDescriptor momentFileEncapsulated = (EncapsulatedFileDescriptor) fileList.get(0);
        
        assertEquals("observations-12345-moment_maps-mm-2.fits", momentFileEncapsulated.getFileId());
        assertEquals(3L, momentFileEncapsulated.getSizeKb());
        assertEquals(FileType.MOMENT_MAP, momentFileEncapsulated.getFileType());
        assertEquals("observations-12345-encapsulation_files-encap_1.tar",
        		momentFileEncapsulated.getEncapsulationFile().getFileId());
        assertEquals(66L, momentFileEncapsulated.getEncapsulationFile().getSizeKb());       
        
        EncapsulatedFileDescriptor momentFileOriginal = (EncapsulatedFileDescriptor) fileList.get(1);

        assertEquals("observations-12345-moment_maps-mm_1.fits", momentFileOriginal.getFileId());
        assertEquals(1L, momentFileOriginal.getSizeKb());
        assertEquals(FileType.MOMENT_MAP, momentFileOriginal.getFileType());
        assertNull(momentFileOriginal.getEncapsulationFile());
    }
    
    @Test
    public void testGetCubeletFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.CUBELET);
        assertEquals(0, fileList.size());

        EncapsulationFile encapFile = new EncapsulationFile();
        encapFile.setId(123L);
        encapFile.setFilename("encaps_1.tar");
        encapFile.setFilesize(34L);
        
        Map<String, Object> cubeletEncap = new HashMap<String, Object>();
        cubeletEncap.put(DataAccessUtil.ID, 152L);
        cubeletEncap.put(DataAccessUtil.FILE_SIZE, 3L);
        cubeletEncap.put(DataAccessUtil.OBSERVATION_ID, 12345);
        cubeletEncap.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        cubeletEncap.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        cubeletEncap.put(DataAccessUtil.FILENAME, "cubelet-2.fits");
        cubeletEncap.put("encapsulationfilename", "encap_1.tar");
        cubeletEncap.put("encapsulationid", 666L);
        cubeletEncap.put("encapsulationfilesize", 66L);
        cubeletEncap.put("imagecubesize", 16L);
        
        Map<String, Object> cubeletOrig = new HashMap<String, Object>();
        cubeletOrig.put(DataAccessUtil.ID, 152L);
        cubeletOrig.put(DataAccessUtil.FILE_SIZE, 1L);
        cubeletOrig.put(DataAccessUtil.OBSERVATION_ID, 12345);
        cubeletOrig.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        cubeletOrig.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        cubeletOrig.put(DataAccessUtil.FILENAME, "cubelet_1.fits");
        
        results.add(cubeletEncap);
        results.add(cubeletOrig);

    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.CUBELET);

        assertEquals(2, fileList.size());
        
        EncapsulatedFileDescriptor cubeletFileEncapsulated = (EncapsulatedFileDescriptor) fileList.get(0);
        
        assertEquals("observations-12345-cubelets-cubelet-2.fits", cubeletFileEncapsulated.getFileId());
        assertEquals(3L, cubeletFileEncapsulated.getSizeKb());
        assertEquals(FileType.CUBELET, cubeletFileEncapsulated.getFileType());
        assertEquals("observations-12345-encapsulation_files-encap_1.tar",
        		cubeletFileEncapsulated.getEncapsulationFile().getFileId());
        assertEquals(66L, cubeletFileEncapsulated.getEncapsulationFile().getSizeKb());       
        
        EncapsulatedFileDescriptor cubeletFileOriginal = (EncapsulatedFileDescriptor) fileList.get(1);

        assertEquals("observations-12345-cubelets-cubelet_1.fits", cubeletFileOriginal.getFileId());
        assertEquals(1L, cubeletFileOriginal.getSizeKb());
        assertEquals(FileType.CUBELET, cubeletFileOriginal.getFileType());
        assertNull(cubeletFileOriginal.getEncapsulationFile());
    }
    
    @Test
    public void testGetThumbnailFile()
    {
    	Observation obs = new Observation();
    	obs.setSbid(1234);
    	
        EncapsulationFile encapFile = new EncapsulationFile();
        encapFile.setId(123L);
        encapFile.setFilename("encaps_1.tar");
        encapFile.setFilesize(34L);
        encapFile.setParent(obs);
        
    	Thumbnail thumbnailOrig = mock(Thumbnail.class);
        when(thumbnailOrig.getFileId()).thenReturn("thumb-1-file-id");
        when(thumbnailOrig.getId()).thenReturn(112L);
        when(thumbnailOrig.getFilename()).thenReturn("thumb_1.fits");
        when(thumbnailOrig.getFilesize()).thenReturn(2L);
        
    	Thumbnail thumbnailEncap = mock(Thumbnail.class);
        when(thumbnailEncap.getFileId()).thenReturn("thumb-2-file-id");
        when(thumbnailEncap.getId()).thenReturn(117L);
        when(thumbnailEncap.getFilename()).thenReturn("thumb_2.fits");
        when(thumbnailEncap.getFilesize()).thenReturn(4L);
        when(thumbnailEncap.getEncapsulationFile()).thenReturn(encapFile);
        
        EncapsulatedFileDescriptor fileOrig = DataAccessUtil.getThumbnailFile(thumbnailOrig);
        assertEquals("thumb-1-file-id", fileOrig.getFileId());
        assertEquals("thumb_1.fits", fileOrig.getDisplayName());
        assertEquals(2L, fileOrig.getSizeKb());
        assertEquals(FileType.THUMBNAIL, fileOrig.getFileType());
        assertNull(fileOrig.getEncapsulationFile());
        
        EncapsulatedFileDescriptor fileEncap = DataAccessUtil.getThumbnailFile(thumbnailEncap);
        assertEquals("thumb-2-file-id", fileEncap.getFileId());
        assertEquals("thumb_2.fits", fileEncap.getDisplayName());
        assertEquals(4L, fileEncap.getSizeKb());
        assertEquals(FileType.THUMBNAIL, fileEncap.getFileType());
        assertNotNull(fileEncap.getEncapsulationFile());
        assertEquals("observations-1234-encapsulation_files-encaps_1.tar", 
        		fileEncap.getEncapsulationFile().getFileId());
        assertEquals(34L, fileEncap.getEncapsulationFile().getSizeKb());     
    }

    
    @Test
    public void testGetSpectrumFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.SPECTRUM);
        assertEquals(0, fileList.size());

        EncapsulationFile encapFile = new EncapsulationFile();
        encapFile.setId(123L);
        encapFile.setFilename("encaps_1.tar");
        encapFile.setFilesize(34L);
        
        Map<String, Object> spectrumEncap = new HashMap<String, Object>();
        spectrumEncap.put(DataAccessUtil.ID, 152L);
        spectrumEncap.put(DataAccessUtil.OBSERVATION_ID, 658782);
        spectrumEncap.put(DataAccessUtil.FILE_SIZE, 1L);
        spectrumEncap.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        spectrumEncap.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        spectrumEncap.put(DataAccessUtil.FILENAME, "spectrum_1.fits");
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_FILENAME, "encap_1.tar");
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_ID, 666L);
        spectrumEncap.put(DataAccessUtil.ENCAPSULATION_FILE_SIZE, 7L);
        
        Map<String, Object> spectrumOrig = new HashMap<String, Object>();
        spectrumOrig.put(DataAccessUtil.ID, 152L);
        spectrumOrig.put(DataAccessUtil.OBSERVATION_ID, 658782);
        spectrumOrig.put(DataAccessUtil.FILE_SIZE, 3L);
        spectrumOrig.put(DataAccessUtil.IMAGE_CUBE_ID, 13L);
        spectrumOrig.put(DataAccessUtil.IMAGE_CUBE_SIZE, 16L);
        spectrumOrig.put(DataAccessUtil.FILENAME, "spectrum-2.fits");
        
        results.add(spectrumEncap);
        results.add(spectrumOrig);

    	DataAccessUtil.getEncapsulatedFileDescriptor(results, fileList, FileType.SPECTRUM);

        assertEquals(2, fileList.size());
        
        EncapsulatedFileDescriptor spectrumFileEncapsulated = (EncapsulatedFileDescriptor) fileList.get(0);
        
        assertEquals("observations-658782-spectra-spectrum_1.fits", spectrumFileEncapsulated.getFileId());
        assertEquals(1L, spectrumFileEncapsulated.getSizeKb());
        assertEquals(FileType.SPECTRUM, spectrumFileEncapsulated.getFileType());
        assertEquals("observations-658782-encapsulation_files-encap_1.tar", 
        		spectrumFileEncapsulated.getEncapsulationFile().getFileId());
        assertEquals(7L, spectrumFileEncapsulated.getEncapsulationFile().getSizeKb());       
        
        EncapsulatedFileDescriptor spectrumFileOriginal = (EncapsulatedFileDescriptor) fileList.get(1);

        assertEquals("observations-658782-spectra-spectrum-2.fits", spectrumFileOriginal.getFileId());
        assertEquals(3L, spectrumFileOriginal.getSizeKb());
        assertEquals(FileType.SPECTRUM, spectrumFileOriginal.getFileType());
        assertNull(spectrumFileOriginal.getEncapsulationFile());
    }

    @Test
    public void testGetErrorFiles()
    {
    	List<DownloadFile> fileList = new ArrayList<DownloadFile>();
    	List<Map<String, Object>> errorResults = new ArrayList<Map<String, Object>>();
    	
    	DataAccessUtil.getErrorFiles(errorResults, fileList);
        assertEquals(0, fileList.size());

        Map<String, Object> error = new HashMap<String, Object>();
        error.put("message", "UsageError: Foo");
        error.put(DataAccessUtil.ID, 1L);
        String expectedMessage = "UsageError: Foo";
        errorResults.add(error);
        
        DataAccessUtil.getErrorFiles(errorResults, fileList);
        assertEquals(1, fileList.size());
        DownloadFile errorFile = fileList.get(0);
        assertEquals("error-01.txt", errorFile.getFileId());
        assertEquals(1L, errorFile.getSizeKb());
        assertEquals("error-01.txt", errorFile.getFilename());
        assertEquals(expectedMessage, ((ErrorFileDescriptor)errorFile).getErrorMessage());

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
    
    @Test
    public void testCompileUniqueFileId()
    {
    	//correct file id - observation
    	assertEquals("observations-2012-image_cubes-my_image_cube.fits", DataAccessUtil.compileUniqueFileId
    			(2012L, null, "image_cubes", "my_image_cube.fits", 22L));
    	//correct file id - Level 7 
    	assertEquals("level7-12345-image_cubes-my_l7_image_cube.fits", DataAccessUtil.compileUniqueFileId
    			(null, 12345L, "image_cubes", "my_l7_image_cube.fits", 31L));
    	//too long file id
    	assertEquals("observations-2012-image_cubes-12.fits", DataAccessUtil.compileUniqueFileId(2012L, null,
    			"image_cubes", "image.i.NGC7232.cont.sb2012.NGC7232.linmos.alt.restored.SelfCalLoop1.fits", 12L));
    	//not match regex
    	assertEquals("observations-2012-image_cubes-57.fits", 
    			DataAccessUtil.compileUniqueFileId(2012L, null, "image_cubes", "my_image&Cube.fits", 57L));
    }

    private static DataAccessJob getDummyDataAccessJobCatalogues(CatalogueDownloadFormat downloadFormat)
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

        return dataAccessJob;
    }
    
    private List<Map<String, Object>> getDummyCatalogues()
    {
    	List<Map<String, Object>> catalogues = new ArrayList<Map<String, Object>>();
 	
    	Map<String,Object> catalogue1 = generateCatalogue("AS123", 12145, CatalogueType.CONTINUUM_COMPONENT, 12L, null);
    	Map<String,Object> catalogue2 = generateCatalogue("AS123", 12145, CatalogueType.CONTINUUM_COMPONENT, 13L, null);
    	Map<String,Object> catalogue3 = generateCatalogue("AS124", 12245, CatalogueType.CONTINUUM_ISLAND, 14L, null);
    	Map<String,Object> catalogue4 = generateCatalogue("AS133", 12445, CatalogueType.CONTINUUM_ISLAND, 15L, null);
    	Map<String,Object> catalogue5 = generateCatalogue("AS523", 12545, CatalogueType.CONTINUUM_COMPONENT, 16L, null);

        catalogues.add(catalogue1);
        catalogues.add(catalogue2);
        catalogues.add(catalogue3);
        catalogues.add(catalogue4);
        catalogues.add(catalogue5);
        
        return catalogues;
    }

    private static Map<String,Object> generateCatalogue(String opalCode, Integer parentId, CatalogueType catalogueType,
            Long catalogueId, String tablename)
    {
        Map<String,Object> catalogue = new HashMap<String,Object>();
        catalogue.put(DataAccessUtil.ID, catalogueId);
        if(catalogueType != null)
        {
            catalogue.put(DataAccessUtil.CATALOGUE_TYPE, catalogueType.name());
        }
        catalogue.put(DataAccessUtil.PROJECT_ID, opalCode);
        if(catalogueType == CatalogueType.DERIVED_CATALOGUE)
        {
            catalogue.put(DataAccessUtil.LEVEL7_ID, new Long(parentId));
        }
        else
        {
            catalogue.put(DataAccessUtil.OBSERVATION_ID, parentId);
        }
        
        if (catalogueType == CatalogueType.DERIVED_CATALOGUE)
        {
        	catalogue.put(DataAccessUtil.TABLE_NAME, tablename);
        }
        return catalogue;
    }

}
