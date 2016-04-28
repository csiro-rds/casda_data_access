package au.csiro.casda.access.uws;

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

import uws.UWSException;
import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;

/**
 * CASDA UWS Job, which sets the uws job id to the request id if available.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class CasdaUwsJob extends UWSJob
{
    private static final long serialVersionUID = 3617572690292123388L;

    /**
     * Constructor
     * 
     * @param params
     *            UWS job parameters
     * @throws UWSException
     *             if there is a problem creating the UWS job
     */
    public CasdaUwsJob(UWSParameters params) throws UWSException
    {
        super(params);
    }

    /**
     * If the request id is available in the job parameters, will set the
     * 
     * Otherwise calls the generatedJobId method from {@link UWSJob}
     * 
     * @return the job id
     * @throws UWSException
     *             if there is a problem creating the UWS job
     */
    @Override
    public String generateJobId() throws UWSException
    {
        String jobId = null;
        if (inputParams != null)
        {
            jobId = (String) inputParams.get(AccessJobManager.REQUEST_ID);
        }
        jobId = StringUtils.isNotBlank(jobId) ? jobId : super.generateJobId();
        return jobId;
    }
}
