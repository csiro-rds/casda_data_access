package au.csiro.casda.access.uws;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.services.DataAccessService;
import uws.UWSException;
import uws.job.JobThread;
import uws.job.UWSJob;
import uws.service.AbstractUWSFactory;

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
 * Factory class used to provide UWS service with instances of UWSJobs and JobThreads for data access requests.
 *
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class AccessUwsFactory extends AbstractUWSFactory
{
    @SuppressWarnings({ "checkstyle:visibilitymodifier", "checkstyle:javadocvariable" })
    protected final DataAccessService dataAccessService;

    @SuppressWarnings({ "checkstyle:visibilitymodifier", "checkstyle:javadocvariable" })
    protected final Packager packager;

    @SuppressWarnings({ "checkstyle:visibilitymodifier", "checkstyle:javadocvariable" })
    protected final int hoursToExpiryDefault;

    @SuppressWarnings({ "checkstyle:visibilitymodifier", "checkstyle:javadocvariable" })
    protected final int hoursToExpirySodaSync;
    
    private final String siapSharedSecretKey;

    /**
     * Create a new AccessUwsFactory instance.
     * 
     * @param dataAccessService
     *            The service instance managing data access objects for the job.
     * @param packager
     *            The packager instance which will be doing the work for each job.
     * @param hoursToExpiryDefault
     *            the default number of hours until a job will expire
     * @param hoursToExpirySodaSync
     *            the number of hours to expiry for a SODA sync job
     * @param siapSharedSecretKey
     *            the key for decrypting request tokens
     */
    @Autowired
    public AccessUwsFactory(DataAccessService dataAccessService, Packager packager,
            @Value("${hours.to.expiry.default}") int hoursToExpiryDefault,
            @Value("${hours.to.expiry.soda_sync}") int hoursToExpirySodaSync, 
            @Value("${siap.shared.secret.key}") String siapSharedSecretKey)
    {
        this.dataAccessService = dataAccessService;
        this.packager = packager;
        this.hoursToExpiryDefault = hoursToExpiryDefault;
        this.hoursToExpirySodaSync = hoursToExpirySodaSync;
        this.siapSharedSecretKey = siapSharedSecretKey;
    }

    /*
     * (non-Javadoc)
     * 
     * @see au.csiro.casda.access.uws.AccessUwsFactoryInterface#createJobThread(uws.job.UWSJob)
     */
    @Override
    public JobThread createJobThread(UWSJob uwsJob) throws UWSException
    {
        return new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, 
        		hoursToExpirySodaSync, siapSharedSecretKey);
    }
}
