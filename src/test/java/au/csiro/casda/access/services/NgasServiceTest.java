package au.csiro.casda.access.services;

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


import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.CharEncoding;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.ResponseEntity;

import au.csiro.casda.access.security.SecuredRestTemplate;
import au.csiro.casda.access.services.NgasService.ServiceCallException;
import au.csiro.casda.access.services.NgasService.Status;

/**
 * Tests the ngas service, copied from DataDeposit project.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class NgasServiceTest
{

    @Mock
    SecuredRestTemplate restTemplate;

    @Mock
    ResponseEntity<String> responseEntity;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetStatusEmptyFail() throws ServiceCallException
    {
        NgasService service = new NgasService(restTemplate, "http://vbcvb:567");
        when(restTemplate.getForEntity("http://vbcvb:567/STATUS?file_id={file_id}", String.class, "fileId1234"))
                .thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn("<?xml version=\"1.0\" ?><NgamsStatus></NgamsStatus>");

        assertThat(service.getStatus("fileId1234").wasSuccess(), is(false));
    }

    @Test
    public void testGetStatusSuccess() throws ServiceCallException, IOException, URISyntaxException
    {
        NgasService service = new NgasService(restTemplate, "http://vbcvb:567");
        when(restTemplate.getForEntity("http://vbcvb:567/STATUS?file_id={file_id}", String.class, "fileId1234"))
                .thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(this.getNgasResponse("ngas_file_status_example.xml"));

        Status status = service.getStatus("fileId1234");
        assertThat(status.wasSuccess(), is(true));
        assertThat(status.getFileName(),
                is("ASKAPArchive/2014-12-19/1/observations-8888883-catalogues-AS007a-continuum.xml"));
        assertThat(status.getMountPoint(), is("/CASDA/application/ngas/ngas_T/NGAS/volume2"));
    }

    @Test
    public void testGetStatusFileNotFound() throws ServiceCallException, IOException, URISyntaxException
    {
        NgasService service = new NgasService(restTemplate, "http://vbcvb:567");
        when(restTemplate.getForEntity("http://vbcvb:567/STATUS?file_id={file_id}", String.class, "fileId1234"))
                .thenReturn(responseEntity);
        when(responseEntity.getBody()).thenReturn(this.getNgasResponse("ngas_file_status_not_found_example.xml"));

        assertThat(service.getStatus("fileId1234").wasSuccess(), is(false));
    }

    private String getNgasResponse(String name) throws IOException, URISyntaxException
    {
        URL url = getClass().getResource("/ngas/" + name);
        String content = new String(Files.readAllBytes(Paths.get(url.toURI())), CharEncoding.UTF_8);
        return content;
    }

}
