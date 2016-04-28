package au.csiro.casda.access.health;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import au.csiro.casda.access.cache.CacheManager;

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
 * <add description here>
 * 
 * Copyright 2015, CSIRO Australia
 * All rights reserved.
 * 
 * @author      James Dempsey on 4 Feb 2015
 * @version     $Revision$  $Date$
 */
public class CacheManagerHealthTest
{

    @Mock
    private CacheManager cacheManager;

    @Before
    public void setup()
    {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test method for {@link au.csiro.casda.access.health.CacheManagerHealth#health()}.
     */
    @Test
    public void testSuccess()
    {
        CacheManagerHealth health = new CacheManagerHealth(cacheManager, 1000l);
        
        when(cacheManager.getMaxCacheSizeKb()).thenReturn(5200l);
        when(cacheManager.getUsedCacheSizeKb()).thenReturn(1700l);
        
        Health healthResp = health.health();
        assertThat(healthResp.getStatus(), is(Status.UP));
        assertThat((String) healthResp.getDetails().get("maxSizeKb"), is("5,200"));
        assertThat((String) healthResp.getDetails().get("usedSizeKb"), is("1,700"));
        assertThat((String) healthResp.getDetails().get("freeSizeKb"), is("3,500"));
    }

    /**
     * Test method for {@link au.csiro.casda.access.health.CacheManagerHealth#health()}.
     */
    @Test
    public void testWarningFull()
    {
        CacheManagerHealth health = new CacheManagerHealth(cacheManager, 1000l);
        
        when(cacheManager.getMaxCacheSizeKb()).thenReturn(5200l);
        when(cacheManager.getUsedCacheSizeKb()).thenReturn(5200l);
        
        Health healthResp = health.health();
        assertThat(healthResp.getStatus().getCode(), is("WARN"));
        assertThat((String) healthResp.getDetails().get("maxSizeKb"), is("5,200"));
        assertThat((String) healthResp.getDetails().get("usedSizeKb"), is("5,200"));
        assertThat((String) healthResp.getDetails().get("freeSizeKb"), is("0"));
        assertThat((String) healthResp.getDetails().get("Warning"), is("Cache full!"));
    }

    /**
     * Test method for {@link au.csiro.casda.access.health.CacheManagerHealth#health()}.
     */
    @Test
    public void testWarningOverFull()
    {
        CacheManagerHealth health = new CacheManagerHealth(cacheManager, 1000l);
        
        when(cacheManager.getMaxCacheSizeKb()).thenReturn(5200l);
        when(cacheManager.getUsedCacheSizeKb()).thenReturn(6000l);
        
        Health healthResp = health.health();
        assertThat(healthResp.getStatus().getCode(), is("WARN"));
        assertThat((String) healthResp.getDetails().get("maxSizeKb"), is("5,200"));
        assertThat((String) healthResp.getDetails().get("usedSizeKb"), is("6,000"));
        assertThat((String) healthResp.getDetails().get("freeSizeKb"), is("-800"));
        assertThat((String) healthResp.getDetails().get("Warning"), is("Cache full!"));
    }

    /**
     * Test method for {@link au.csiro.casda.access.health.CacheManagerHealth#health()}.
     */
    @Test
    public void testWarningAlmostFull()
    {
        CacheManagerHealth health = new CacheManagerHealth(cacheManager, 1000l);
        
        when(cacheManager.getMaxCacheSizeKb()).thenReturn(5200l);
        when(cacheManager.getUsedCacheSizeKb()).thenReturn(5000l);
        
        Health healthResp = health.health();
        assertThat(healthResp.getStatus().getCode(), is("WARN"));
        assertThat((String) healthResp.getDetails().get("maxSizeKb"), is("5,200"));
        assertThat((String) healthResp.getDetails().get("usedSizeKb"), is("5,000"));
        assertThat((String) healthResp.getDetails().get("freeSizeKb"), is("200"));
        assertThat((String) healthResp.getDetails().get("Warning"), is("Cache almost full!"));
    }

}
