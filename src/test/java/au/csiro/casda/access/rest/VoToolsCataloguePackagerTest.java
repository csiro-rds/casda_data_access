package au.csiro.casda.access.rest;

import static au.csiro.casda.access.rest.VoToolsCataloguePackager.TAP_SYNC_CSV_PARAMS;
import static au.csiro.casda.access.rest.VoToolsCataloguePackager.TAP_SYNC_VO_PARAMS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;

import au.csiro.casda.access.CatalogueDownloadFile;
import au.csiro.casda.access.CatalogueDownloadFormat;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.jpa.TapTableRepository;
import au.csiro.casda.access.security.SecuredRestTemplate;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.entity.TapTableExtract;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;

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

/**
 * Tests for the vo tools catalogue packager.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class VoToolsCataloguePackagerTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Mock
    private InlineScriptService inlineScriptService;
    
    @Mock
    private TapTableRepository tapTableRepository;

    private String csvResponse = "col1,col2,col3,col4 \n" + "abc,def,ghi,jkl \n";

    private String xmlResponse = "<xml><table><tr><td>10000</td></tr></table</xml>";

    private String countResponse = "count \n10 \n";

    private String url = "http://votools/";
    private String query = "select%20*%20from%20casda.continuum_component%20where%20catalogue_id%20%3D%201";

    private MockRestServiceServer mockServer;
    private VoToolsCataloguePackager cataloguePackager;

    private String tempDir;

    private static final int HOURS_TO_EXPIRY = 2;

    /**
     * Set up the service before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);

        SecuredRestTemplate restTemplate = new SecuredRestTemplate();
        mockServer = MockRestServiceServer.createServer(restTemplate);

        tempDir = tempFolder.newFolder("CatTest").getAbsolutePath();

        String calcChecksumScript = "script/calc_checksum.sh";
        cataloguePackager =
                spy(new VoToolsCataloguePackager(inlineScriptService, tapTableRepository, url, calcChecksumScript));
        cataloguePackager.setRestTemplate(restTemplate);
    }

    @After
    public void tearDown() throws Exception
    {
         tempFolder.delete();
    }

    private Catalogue createCatalogue()
    {
        Catalogue cat = new Catalogue();
        cat.setId(1l);
        Project project = new Project();
        project.setId(1l);
        project.setOpalCode("AS007");
        cat.setProject(project);
        cat.setCatalogueType(CatalogueType.CONTINUUM_COMPONENT);
        cat.setParent(new Observation(11111));

        return cat;
    }

    @Test
    public void testCreateVoTableXML() throws Exception
    {
        String expectedChecksum = "1 2 3";
        when(inlineScriptService.callScriptInline(contains("checksum"), anyString())).thenReturn(expectedChecksum);
        when(inlineScriptService.createInlineJobMonitor(contains("checksum"), anyString())).thenCallRealMethod();

        String voUri = url + TAP_SYNC_VO_PARAMS.replace("+", "%2B") + query;
        System.out.println("VO Uri: " + voUri);

        mockServer.expect(requestTo(voUri)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(xmlResponse, MediaType.APPLICATION_XHTML_XML));

        Catalogue cat = createCatalogue();
        String fileName =
                DataAccessUtil.getIndividualCatalogueFilename(cat, CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        CatalogueDownloadFile catalogueDownloadFile = new CatalogueDownloadFile();
        catalogueDownloadFile.setCatalogueType(cat.getCatalogueType());
        catalogueDownloadFile.setDownloadFormat(CatalogueDownloadFormat.VOTABLE_INDIVIDUAL);
        catalogueDownloadFile.setFilename(fileName);
        catalogueDownloadFile.setFileId("1001-" + fileName);
        catalogueDownloadFile.getCatalogueIds().add(cat.getId());
        DateTime unlock = DateTime.now(DateTimeZone.UTC).plusHours(HOURS_TO_EXPIRY);

        cataloguePackager.generateCatalogueAndChecksumFile(new File(tempDir + "/jobs/1001"), catalogueDownloadFile,
                unlock);

        // verify file and checksum exist
        File voFile = new File(tempDir + "/jobs/1001/" + catalogueDownloadFile.getFilename());
        File checksumFile = new File(voFile.getCanonicalPath() + ".checksum");
        assertTrue(voFile.exists());
        assertTrue(checksumFile.exists());

        String contents = FileUtils.readFileToString(voFile);
        assertEquals(contents, xmlResponse);
        String checksumContents = FileUtils.readFileToString(checksumFile);
        assertEquals(checksumContents, expectedChecksum);
    }

    @Test
    public void testCreateCsv() throws Exception
    {
        String expectedChecksum = "1 2 3";
        when(inlineScriptService.callScriptInline(contains("checksum"), anyString())).thenReturn(expectedChecksum);
        when(inlineScriptService.createInlineJobMonitor(contains("checksum"), anyString())).thenCallRealMethod();

        String csvUri = url + TAP_SYNC_CSV_PARAMS + query;
        mockServer.expect(requestTo(csvUri)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(csvResponse, MediaType.TEXT_PLAIN));

        Catalogue cat = createCatalogue();
        String fileName = DataAccessUtil.getIndividualCatalogueFilename(cat, CatalogueDownloadFormat.CSV_INDIVIDUAL);
        CatalogueDownloadFile catalogueDownloadFile = new CatalogueDownloadFile();
        catalogueDownloadFile.setCatalogueType(cat.getCatalogueType());
        catalogueDownloadFile.setDownloadFormat(CatalogueDownloadFormat.CSV_INDIVIDUAL);
        catalogueDownloadFile.setFilename(fileName);
        catalogueDownloadFile.setFileId("1001-" + fileName);
        catalogueDownloadFile.getCatalogueIds().add(cat.getId());
        DateTime unlock = DateTime.now(DateTimeZone.UTC).plusHours(HOURS_TO_EXPIRY);

        cataloguePackager.generateCatalogueAndChecksumFile(new File(tempDir + "/jobs/1002"), catalogueDownloadFile,
                unlock);

        // verify file exists
        File voFile = new File(tempDir + "/jobs/1002/" + fileName);
        File checksumFile = new File(voFile.getCanonicalFile() + ".checksum");
        assertTrue(voFile.exists());
        assertTrue(checksumFile.exists());

        String contents = FileUtils.readFileToString(voFile);
        assertEquals(contents, csvResponse);
        String checksumContents = FileUtils.readFileToString(checksumFile);
        assertEquals(checksumContents, expectedChecksum);
    }

    @Test
    public void testCreateTapQuery()
    {
        String expectedContinuumComponentQuery =
                "select * from casda.continuum_component where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";

        String expectedContinuumIslandQuery =
                "select * from casda.continuum_island where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";

        String expectedPolarisationComponentQuery =
                "select * from casda.polarisation_component where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";

        String expectedContinuumComponentCountQuery =
                "select count(*) from casda.continuum_component where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";

        String expectedContinuumIslandCountQuery =
                "select count(*) from casda.continuum_island where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";

        String expectedPolarisationComponentCountQuery =
                "select count(*) from casda.polarisation_component where catalogue_id = 1 " + "or catalogue_id = 2 "
                        + "or catalogue_id = 3";
        
        String expectedLevel7CountQuery =
                "select count(*) from tapschema.taptablename";
        
        String expectedLevel7Query =
                "select * from tapschema.taptablename";


        List<Long> ids = new ArrayList<Long>();
        ids.add(1l);
        ids.add(2l);
        ids.add(3l);

        String continuumComponentQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.CONTINUUM_COMPONENT, null, false);
        assertEquals(expectedContinuumComponentQuery, continuumComponentQueryResult);

        String continuumIslandQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.CONTINUUM_ISLAND, null, false);
        assertEquals(expectedContinuumIslandQuery, continuumIslandQueryResult);

        String polarisationComponentQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.POLARISATION_COMPONENT, null, false);
        assertEquals(expectedPolarisationComponentQuery, polarisationComponentQueryResult);

        String continuumComponentCountQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.CONTINUUM_COMPONENT, null, true);
        assertEquals(expectedContinuumComponentCountQuery, continuumComponentCountQueryResult);

        String continuumIslandCountQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.CONTINUUM_ISLAND, null, true);
        assertEquals(expectedContinuumIslandCountQuery, continuumIslandCountQueryResult);

        String polarisationComponentCountQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.POLARISATION_COMPONENT, null, true);
        assertEquals(expectedPolarisationComponentCountQuery, polarisationComponentCountQueryResult);
        
        // note we store the qualified tap table in the table_name column of the casda.TAP_TABLES 
        when(tapTableRepository.findByTapTable("casda", "table_name")).thenReturn(
                new TapTableExtract("somethingelse", "tapschema.taptablename", "casda", "table_name"));
        String level7QueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.LEVEL7, "casda.table_name", false);
        assertEquals(expectedLevel7Query, level7QueryResult);
        
        String level7CountQueryResult =
                cataloguePackager.createTapQuery(ids, CatalogueType.LEVEL7, "casda.table_name", true);
        assertEquals(expectedLevel7CountQuery, level7CountQueryResult);
    }

    @Test
    public void testCreateChecksumEmptyResponseThrowsException() throws Exception
    {
        thrown.expect(CreateChecksumException.class);
        thrown.expectMessage("Script generated an empty checksum response");

        doReturn(" ").when(inlineScriptService).callScriptInline(contains("checksum"), contains("file"));
        cataloguePackager.createChecksumFile(new File("file"));
    }
    
    /* this test is needed, because if a job gets restarted, we want to make sure it can rewrite the file */
    @Test
    public void testCreateChecksumWriteTwiceOk() throws Exception
    {
        doReturn("something").when(inlineScriptService).callScriptInline(contains("checksum"), contains("fileA"));
        cataloguePackager.createChecksumFile(new File("fileA"));
        assertTrue(new File("fileA.checksum").exists());
        cataloguePackager.createChecksumFile(new File("fileA"));
        assertTrue(new File("fileA.checksum").exists());
    }

    @Test
    public void testEstimateFileSizeKbCsv()
    {
        String countQuery = "select%20count(*)%20from%20casda.continuum_component%20where%20catalogue_id%20%3D%201";
        String countUri = url + TAP_SYNC_CSV_PARAMS + countQuery;
        mockServer.expect(requestTo(countUri)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(countResponse, MediaType.TEXT_PLAIN));

        Catalogue catalogue = createCatalogue();
        CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
        downloadFile.setCatalogueType(catalogue.getCatalogueType());
        downloadFile.setDownloadFormat(CatalogueDownloadFormat.CSV_INDIVIDUAL);
        downloadFile.setFileId("1003-filename");
        downloadFile.setFilename("filename");
        downloadFile.getCatalogueIds().add(1L);

        long sizeKb = cataloguePackager.estimateFileSizeKb(downloadFile);
        assertEquals(3, sizeKb);
    }

    @Test
    public void testEstimateFileSizeKbVoTable()
    {
        String countQuery =
                "select%20count(*)%20from%20casda.continuum_component%20where%20catalogue_id%20%3D%201"
                        + "%20or%20catalogue_id%20%3D%202";
        String countUri = url + TAP_SYNC_CSV_PARAMS + countQuery;
        mockServer.expect(requestTo(countUri)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(countResponse, MediaType.TEXT_PLAIN));

        Catalogue catalogue = createCatalogue();
        CatalogueDownloadFile downloadFile = new CatalogueDownloadFile();
        downloadFile.setCatalogueType(catalogue.getCatalogueType());
        downloadFile.setDownloadFormat(CatalogueDownloadFormat.VOTABLE_GROUPED);
        downloadFile.setFileId("1004-filename");
        downloadFile.setFilename("filename");
        downloadFile.getCatalogueIds().add(1L);
        downloadFile.getCatalogueIds().add(2L);

        long sizeKb = cataloguePackager.estimateFileSizeKb(downloadFile);
        assertEquals(6, sizeKb);
    }
}
