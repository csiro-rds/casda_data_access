package au.csiro.casda.access.health;

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


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import au.csiro.casda.access.security.SecuredRestTemplate;

/**
 * Tests the NGAS Service Health check
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
public class NgasServiceHealthTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String HTTP_THE_URL = "http://theUrl:3456";

    @Mock
    private SecuredRestTemplate restTemplate;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSuccess()
    {
        NgasServiceHealth health = new NgasServiceHealth(restTemplate, HTTP_THE_URL);

        when(restTemplate.getForEntity(eq("http://theUrl:3456/STATUS"), eq(String.class))).thenReturn(
                new ResponseEntity<String>(
                        "<NgamsStatus><Status State=\"ONLINE\" Status=\"SUCCESS\" SubState=\"IDLE\"/></NgamsStatus>",
                        HttpStatus.OK));

        Health healthResp = health.health();
        assertThat(healthResp.getStatus(), is(Status.UP));
        assertThat((String) healthResp.getDetails().get("NGAS_ARCHIVE"), containsString("found at " + HTTP_THE_URL));
    }

    @Test
    public void testFailWithWarning()
    {
        NgasServiceHealth health = new NgasServiceHealth(restTemplate, HTTP_THE_URL);

        when(restTemplate.getForEntity(eq("http://theUrl:3456/STATUS"), eq(String.class))).thenReturn(
                new ResponseEntity<String>(
                        "<NgamsStatus><Status State=\"OFFLINE\" Status=\"SUCCESS\" SubState=\"IDLE\"/></NgamsStatus>",
                        HttpStatus.OK));
        Health healthResp = health.health();
        assertThat(healthResp.getStatus().getCode(), is("WARN"));
        assertThat((String) healthResp.getDetails().get("NGAS_ARCHIVE"), containsString("not available"));
    }

    @Test
    public void testFailWithWarningException()
    {
        NgasServiceHealth health = new NgasServiceHealth(restTemplate, HTTP_THE_URL);

        when(restTemplate.getForEntity(eq("http://theUrl:3456/STATUS"), eq(String.class))).thenThrow(
                new RuntimeException("BOB"));
        Health healthResp = health.health();
        assertThat(healthResp.getStatus().getCode(), is("WARN"));
        assertThat((String) healthResp.getDetails().get("NGAS_ARCHIVE"), containsString("not available"));
        assertThat((String) healthResp.getDetails().get("error"), containsString("BOB"));
    }

    @Test
    public void testConstructorBaseUrlMissing()
    {
        thrown.expect(IllegalArgumentException.class);
        new NgasServiceHealth(restTemplate, null);
    }

}
