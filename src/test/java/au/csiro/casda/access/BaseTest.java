package au.csiro.casda.access;

import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestContextManager;

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
 * Base test class that loads up the test class like SpringJUnitTestRunner would, with a sensible local (test) bean
 * configuration.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@ActiveProfiles("local")
@ContextConfiguration(classes = { TestAppConfig.class })
public abstract class BaseTest
{
    public BaseTest() throws Exception
    {
        // Standard Spring test config in the absence of a SpringJUnitTestRunner
        TestContextManager testContextManager = new TestContextManager(getClass());
        testContextManager.prepareTestInstance(this);
    }
}
