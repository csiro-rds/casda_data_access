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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import uws.job.parameters.UWSParameters;

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
 * Tests the CasdaUwsJob constructors
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class CasdaUwsJobTest
{

    @Test
    public void testConstructorParamsOnly() throws Exception
    {
        CasdaUwsJob job = new CasdaUwsJob(new UWSParameters());
        assertNotNull(job.getJobId());
        assertNull(job.getOwner());

        UWSParameters params = new UWSParameters();
        params.set(AccessJobManager.REQUEST_ID, "ABC-123");

        CasdaUwsJob job2 = new CasdaUwsJob(params);
        assertEquals("ABC-123", job2.getJobId());
        assertNull(job2.getOwner());
    }

}
