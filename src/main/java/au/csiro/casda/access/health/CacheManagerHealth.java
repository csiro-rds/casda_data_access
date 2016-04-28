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


import java.text.NumberFormat;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import au.csiro.casda.access.cache.CacheManagerInterface;

/**
 * Health check for the file cache service
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
@Component
public class CacheManagerHealth implements HealthIndicator
{
    private final CacheManagerInterface cacheManager;
    private Long warningThreshold;

    /**
     * Create a new health check service instance.
     * 
     * @param cacheManager
     *            The manager of the cache.
     * @param warningThreshold
     *            The minimum free space in the cache, below which a warning will be shown in the health check.
     */
    @Autowired
    public CacheManagerHealth(CacheManagerInterface cacheManager,
            @Value("${cache.warn.threshold.kb:1000}") Long warningThreshold)
    {
        this.cacheManager = cacheManager;
        this.warningThreshold = warningThreshold;
    }    
    
    @Override
    public Health health()
    {
        long maxSizeKb = cacheManager.getMaxCacheSizeKb();
        long usedSizeKb = cacheManager.getUsedCacheSizeKb();
        long freeSizeKb = maxSizeKb - usedSizeKb;
        NumberFormat formatter = NumberFormat.getIntegerInstance();

        Health.Builder health = new Health.Builder().withDetail("maxSizeKb", formatter.format(maxSizeKb))
                .withDetail("usedSizeKb", formatter.format(usedSizeKb))
                .withDetail("freeSizeKb", formatter.format(freeSizeKb));
        
        if (usedSizeKb >= maxSizeKb)
        {
            health.status("WARN");
            health.withDetail("Warning", "Cache full!");
            return health.build();
        }
        else if (freeSizeKb < warningThreshold)
        {
            health.status("WARN");
            health = health.withDetail("Warning", "Cache almost full!");
            return health.build();
        }
        
        return health.up().build();
    }

}