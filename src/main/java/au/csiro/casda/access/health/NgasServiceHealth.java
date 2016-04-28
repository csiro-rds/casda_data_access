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


import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import au.csiro.casda.access.security.SecuredRestTemplate;

/**
 * Health check for Ngas service
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
@Component
public class NgasServiceHealth implements HealthIndicator
{
    /** Health rest template read timeout in milliseconds */
    public static final int RESTTEMPLATE_READ_TIMEOUT = 5000;
    /** Health rest template connection timeout in milliseconds */
    public static final int RESTTEMPLATE_CONNECTION_TIMEOUT = 2000;

    private final String baseUrl;
    private final String serviceName;
    private final SecuredRestTemplate restTemplate;

    private final static String NGAS_STATE_ONLINE = "State=\"ONLINE\"";
    private final static String NGAS_STATUS_SUCCESS = "Status=\"SUCCESS\"";

    /**
     * Create a new NgasServiceHealth instance.
     * 
     * @param baseUrl The base URL of the NGAS service we are using. 
     */
    @Autowired
    public NgasServiceHealth(@Value("${ngas.baseurl}") String baseUrl)
    {
        this(baseUrl, "NGAS_ARCHIVE");
    }

    /**
     * Testing only constructor to allow a rest template to be injected.
     * 
     * @param restTemplate The template for communicating with NGAS.
     * @param baseUrl The base URL of the NGAS service we are using. 
     */
    NgasServiceHealth(SecuredRestTemplate restTemplate, String baseUrl)
    {
        if (StringUtils.isEmpty(baseUrl))
        {
            throw new IllegalArgumentException("Url is required for service health check");
        }
        this.baseUrl = baseUrl;
        this.serviceName = "NGAS_ARCHIVE";
        this.restTemplate = restTemplate;
    }

    private NgasServiceHealth(String baseUrl, String serviceName)
    {
        if (StringUtils.isEmpty(baseUrl))
        {
            throw new IllegalArgumentException("Url is required for service health check");
        }
        this.baseUrl = baseUrl;
        this.serviceName = serviceName;
        restTemplate = new SecuredRestTemplate("", "", false);
        restTemplate.setConnectionAndReadTimeout(RESTTEMPLATE_CONNECTION_TIMEOUT, RESTTEMPLATE_READ_TIMEOUT);
    }

    @Override
    public Health health()
    {
        String uri = baseUrl + "/STATUS";
        Health.Builder healthDown = new Health.Builder().withDetail(serviceName, "not available at " + baseUrl);
        healthDown.status("WARN");
        try
        {
            ResponseEntity<String> response = restTemplate.getForEntity(uri, String.class);
            String xmlBody = response.getBody();
            if (StringUtils.contains(xmlBody, NGAS_STATE_ONLINE) && StringUtils.contains(xmlBody, NGAS_STATUS_SUCCESS))
            {
                return new Health.Builder().withDetail(serviceName, "found at " + baseUrl).up().build();
            }
            else
            {
                return healthDown.build();
            }
        }
        catch (Exception e)
        {
            return healthDown.withException(e).build();
        }

    }

}