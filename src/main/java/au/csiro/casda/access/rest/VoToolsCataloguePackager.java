package au.csiro.casda.access.rest;

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;

import au.csiro.casda.access.CatalogueDownloadFile;
import au.csiro.casda.access.CatalogueDownloadFormat;
import au.csiro.casda.access.DataAccessApplication;
import au.csiro.casda.access.InlineScriptException;
import au.csiro.casda.access.jpa.TapTableRepository;
import au.csiro.casda.access.security.SecuredRestTemplate;
import au.csiro.casda.access.services.InlineScriptService;
import au.csiro.casda.entity.TapTableExtract;
import au.csiro.casda.entity.observation.CatalogueType;

/**
 * 
 * Queries the VO Tools REST service to extract the catalogue data and saves it into a file in cache jobs directory
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Component
public class VoToolsCataloguePackager
{
    /** Estimate of KB per row in VOTABLE XML */
    public static final double KB_PER_ROW_VOTABLE = 0.6;
    /** Estimate of KB per row in CSV */
    public static final double KB_PER_ROW_CSV = 0.3;
    /** Time in milliseconds */
    public static final int POLLING_INTERVAL = 5000;
    /** Cutoff for requests to be done async, size in KB */
    public static final int SYNC_CUTOFF_SIZE = 2000;
    /** Query params for sync vo table request */
    public static final String TAP_ASYNC_VO_PARAMS =
            "tap/async?request=doQuery&lang=ADQL&format=application/x-votable+xml&query=";
    /** Query params for sync csv request */
    public static final String TAP_ASYNC_CSV_PARAMS = "tap/async?request=doQuery&lang=ADQL&format=text/csv&query=";
    /** Query params for async vo table request */
    public static final String TAP_SYNC_VO_PARAMS =
            "tap/sync?request=doQuery&lang=ADQL&format=application/x-votable+xml&query=";
    /** Query params for async csv request */
    public static final String TAP_SYNC_CSV_PARAMS = "tap/sync?request=doQuery&lang=ADQL&format=text/csv&query=";

    /**
     * The VO authorisation header key for user id
     */
    public static final String VO_AUTH_HEADER_USER_ID = "X-VOTools-userId";
    /**
     * The VO authorisation header key for user projects
     */
    public static final String VO_AUTH_HEADER_USER_PROJECTS = "X-VOTools-projects";
    /**
     * Allow access to all projects
     */
    public static final String VO_PROJECTS_ALL = "all";

    private final InlineScriptService inlineScriptService;

    private String voToolsUrl;
    private final String calculateChecksumScript;

    private static Logger logger = LoggerFactory.getLogger(VoToolsCataloguePackager.class);

    private SecuredRestTemplate restTemplate;

    private TapTableRepository tapTableRepository;

    /**
     * Constructor
     * 
     * @param inlineScriptService
     *            service for calling shell scripts inline, used here to create checksums
     * @param tapTableRepository
     *            the jpa repository to access tap table information
     * @param voToolsUrl
     *            VO Tools url property
     * @param calculateChecksumScript
     *            the path to the calculate checksum script
     */
    @Autowired
    public VoToolsCataloguePackager(InlineScriptService inlineScriptService, TapTableRepository tapTableRepository,
            @Value("${casda_vo_tools.url}") String voToolsUrl,
            @Value("${calculate.checksum.script}") String calculateChecksumScript)
    {
        this.inlineScriptService = inlineScriptService;
        this.tapTableRepository = tapTableRepository;
        this.voToolsUrl = voToolsUrl;
        this.calculateChecksumScript = calculateChecksumScript;
        this.restTemplate = new SecuredRestTemplate("", "", false);
    }

    /**
     * Calls VO Tools to generate the catalogue file, and creates its checksum file.
     * 
     * @param jobDir
     *            the data access job directory, location for creating the file
     * @param catalogueDownloadFile
     *            the details of the catalogue download file
     * @param unlock
     *            the time to set the files to unlock
     * @throws CatalogueRetrievalException
     *             if there is any problem retrieving the catalogue files from VO Tools
     * @throws CreateChecksumException
     *             if there is any problem creating a checksum for a catalogue file
     * @throws InterruptedException
     *             if the packager processing is interrupted during the polling process
     */
    public void generateCatalogueAndChecksumFile(File jobDir, CatalogueDownloadFile catalogueDownloadFile,
            DateTime unlock) throws CatalogueRetrievalException, CreateChecksumException, InterruptedException
    {
        try
        {
            // create files for the real data
            File file = new File(jobDir, catalogueDownloadFile.getFilename());

            logger.debug("Catalogue file path: {}", file.getAbsolutePath());

            // create the parent directory
            file.getParentFile().mkdirs();

            HttpStatus status;

            try (OutputStream fos = new FileOutputStream(file))
            {

                if (catalogueDownloadFile.getSizeKb() < SYNC_CUTOFF_SIZE)
                {
                    status = voSyncCatalogueRequest(catalogueDownloadFile.getCatalogueIds(),
                            catalogueDownloadFile.getDownloadFormat(), fos, catalogueDownloadFile.getCatalogueType(),
                            catalogueDownloadFile.getQualifiedTablename());
                }
                else
                {
                    status = voAsyncCatalogueRequest(catalogueDownloadFile.getCatalogueIds(),
                            catalogueDownloadFile.getDownloadFormat(), fos, catalogueDownloadFile.getCatalogueType(),
                            catalogueDownloadFile.getQualifiedTablename());
                }
            }

            if (status != HttpStatus.OK)
            {
                throw new CatalogueRetrievalException("HTTP Status not OK");
            }

            createChecksumFile(file);

        }
        catch (HttpClientErrorException | IOException e)
        {
            throw new CatalogueRetrievalException(e);
        }
    }

    /**
     * Calculates an estimate of the space required for each of the files by calling VO tools to request the row count
     * for the user's query.
     * 
     * @param downloadFile
     *            the details of the catalogue file to estimate the size of
     * @return long the estimated size of the catalogue file
     * @throws HttpClientErrorException
     *             if there is a problem calling VO tools
     */
    public long estimateFileSizeKb(CatalogueDownloadFile downloadFile) throws HttpClientErrorException
    {
        int rowCount = voCatalogueSizeRequest(downloadFile.getCatalogueIds(), downloadFile.getCatalogueType(),
                downloadFile.getQualifiedTablename());

        double sizePerRowKb = downloadFile.getDownloadFormat().isVoTable() ? KB_PER_ROW_VOTABLE : KB_PER_ROW_CSV;

        return (long) Math.ceil(rowCount * sizePerRowKb);

    }

    /**
     * Run an async query on VO Tools
     * 
     * @param catalogueIds
     *            List of ids to be retrieved
     * @param format
     *            The download format
     * @param outputStream
     *            An output stream that accepts the body of the REST call
     * @param catalogueType
     *            The catalogue type
     * @param qualifiedTablename
     *            the qualified database table name, only valid for level 7 catalogues
     * @return The REST call status result
     * @throws HttpClientErrorException
     *             if there is a problem calling VO Tools
     * @throws InterruptedException
     *             if the packager processing is interrupted during the polling process
     */
    private HttpStatus voAsyncCatalogueRequest(List<Long> catalogueIds, CatalogueDownloadFormat format,
            OutputStream outputStream, CatalogueType catalogueType, String qualifiedTablename)
                    throws HttpClientErrorException, InterruptedException
    {
        String query = createTapQuery(catalogueIds, catalogueType, qualifiedTablename, false);

        String queryUri;

        if (format.isVoTable())
        {
            queryUri = voToolsUrl + TAP_ASYNC_VO_PARAMS + query;
        }
        else
        {
            queryUri = voToolsUrl + TAP_ASYNC_CSV_PARAMS + query;
        }

        ResponseEntity<String> response = restTemplate.postForEntity(queryUri, null, String.class);
        String location = response.getHeaders().get("Location").get(0);

        String phaseRunUri = location + "/phase" + "?phase=RUN";
        restTemplate.postForEntity(phaseRunUri, null, String.class);

        String phaseUri = location + "/phase";
        String phase;

        do
        {
            Thread.sleep(POLLING_INTERVAL);
            phase = restTemplate.getForEntity(phaseUri, String.class).getBody();
        }
        while ("QUEUED".equals(phase) || "EXECUTING".equals(phase));

        if ("COMPLETED".equals(phase))
        {

            String resultUri = location + "/results/result";

            final RequestCallback requestCallback = new RequestCallback()
            {
                @Override
                public void doWithRequest(final ClientHttpRequest request) throws IOException
                {
                    request.getHeaders().add(VO_AUTH_HEADER_USER_ID, DataAccessApplication.APPLICATION_NAME);
                    request.getHeaders().add(VO_AUTH_HEADER_USER_PROJECTS, VO_PROJECTS_ALL);
                }
            };

            final ResponseExtractor<HttpStatus> extractor = new ResponseExtractor<HttpStatus>()
            {
                @Override
                public HttpStatus extractData(ClientHttpResponse response) throws IOException
                {
                    IOUtils.copyLarge(response.getBody(), outputStream);
                    return response.getStatusCode();
                }
            };

            return restTemplate.execute(resultUri, HttpMethod.GET, requestCallback, extractor);
        }
        else
        {
            return null;
        }
    }

    /**
     * Creates a checksum file for a given file. The destination will be file.checksum
     * 
     * @param file
     *            the file to calculate the checksum for
     * @throws CreateChecksumException
     *             if there is a problem creating the checksum file
     */
    protected void createChecksumFile(File file) throws CreateChecksumException
    {
        logger.debug("Creating checksum file for: {} exists: {}", file, file.exists());
        try
        {
            String response = inlineScriptService.callScriptInline(calculateChecksumScript, file.getCanonicalPath());
            if (StringUtils.isNotBlank(response))
            {
                FileUtils.writeStringToFile(new File(file.getCanonicalPath() + ".checksum"), response);
            }
            else
            {
                throw new CreateChecksumException(
                        "Script generated an empty checksum response for file: " + file.getCanonicalPath());
            }
        }
        catch (IOException | InlineScriptException e)
        {
            throw new CreateChecksumException(e);
        }
    }

    /**
     * Do a synchronous call to VO Tools to create a catalogue file
     * 
     * @param catalogueIds
     *            List of ids to be retrieved
     * @param format
     *            The download format
     * @param outputStream
     *            An output stream that accepts the body of the REST call
     * @param catalogueType
     *            The catalogue type
     * @param qualifiedTablename
     *            the qualified database table name, only valid for level 7 catalogues
     * @return The REST call status result
     * @throws HttpClientErrorException
     */
    private HttpStatus voSyncCatalogueRequest(List<Long> catalogueIds, CatalogueDownloadFormat format,
            OutputStream outputStream, CatalogueType catalogueType, String qualifiedTablename)
                    throws HttpClientErrorException
    {
        String query = createTapQuery(catalogueIds, catalogueType, qualifiedTablename, false);

        String uri;

        if (format.isVoTable())
        {
            uri = voToolsUrl + TAP_SYNC_VO_PARAMS + query;
        }
        else
        {
            uri = voToolsUrl + TAP_SYNC_CSV_PARAMS + query;
        }
        logger.debug("URI: {}", uri);

        final RequestCallback requestCallback = new RequestCallback()
        {
            @Override
            public void doWithRequest(final ClientHttpRequest request) throws IOException
            {
                request.getHeaders().add(VO_AUTH_HEADER_USER_ID, DataAccessApplication.APPLICATION_NAME);
                request.getHeaders().add(VO_AUTH_HEADER_USER_PROJECTS, VO_PROJECTS_ALL);
            }
        };

        final ResponseExtractor<HttpStatus> extractor = new ResponseExtractor<HttpStatus>()
        {
            @Override
            public HttpStatus extractData(ClientHttpResponse response) throws IOException
            {
                IOUtils.copy(response.getBody(), outputStream);
                return response.getStatusCode();
            }
        };

        return restTemplate.execute(uri, HttpMethod.GET, requestCallback, extractor);
    }

    /**
     * Get the number of rows the catalogue will have
     * 
     * @param catalogueIds
     *            List of catalogue IDs
     * @param catalogueType
     *            The catalogue type
     * @param qualifiedTablename
     *            the qualified database table name, only valid for level 7 catalogues
     * @return the row count
     * @throws IOException
     */
    private int voCatalogueSizeRequest(List<Long> catalogueIds, CatalogueType catalogueType, String qualifiedTablename)
    {
        String query = createTapQuery(catalogueIds, catalogueType, qualifiedTablename, true);

        String uri = voToolsUrl + TAP_SYNC_CSV_PARAMS + query;

        ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);

        if (response.getStatusCode() == HttpStatus.OK)
        {
            String body = response.getBody();
            int startLine = body.indexOf("\n") + 1;
            String size = body.substring(startLine, body.indexOf("\n", startLine) - 1);
            return Integer.parseInt(size);
        }
        else
        {
            return 0;
        }
    }

    /**
     * Generate the query to retrieve TAP continuum data
     * 
     * @param catalogueIds
     *            The ids being downloaded
     * @param catalogueType
     *            Catalogue type, component or island
     * @param qualifiedTablename
     *            the qualified database table name if it is a level 7 catalogue
     * @param countOnly
     *            Create a query for the number of records
     * @return The generated query
     */
    protected String createTapQuery(List<Long> catalogueIds, CatalogueType catalogueType, String qualifiedTablename,
            boolean countOnly)
    {
        StringBuffer query = new StringBuffer();

        if (countOnly)
        {
            query.append("select count(*) ");
        }
        else
        {
            query.append("select * ");
        }

        query.append("from ");

        if (catalogueType == CatalogueType.LEVEL7)
        {
            // The level 7 catalogue record (in the catalogue table) stores the qualified database table
            // We need to translate this to the VO TAP schema and database so we can download the data from VO Tools.
            String[] dbSchemaAndTable = qualifiedTablename.split("\\.");
            if (dbSchemaAndTable.length != 2)
            {
                throw new IllegalArgumentException("Invalid table name " + qualifiedTablename);
            }
            TapTableExtract tapTableExtract =
                    tapTableRepository.findByTapTable(dbSchemaAndTable[0], dbSchemaAndTable[1]);
            // note we store the qualified tap table in the table_name column of the casda.TAP_TABLES
            query.append(tapTableExtract.getTableName());
        }
        else
        {
            query.append("casda." + catalogueType.name().toLowerCase());
            query.append(" where catalogue_id = " + catalogueIds.get(0));

            if (catalogueIds.size() > 1)
            {
                for (int i = 1; i < catalogueIds.size(); i++)
                {
                    query.append(" or catalogue_id = " + catalogueIds.get(i));
                }
            }
        }

        return query.toString();
    }

    /**
     * Set the rest template
     * 
     * @param restTemplate
     *            The REST Template
     */
    public void setRestTemplate(SecuredRestTemplate restTemplate)
    {
        this.restTemplate = restTemplate;
    }
}
