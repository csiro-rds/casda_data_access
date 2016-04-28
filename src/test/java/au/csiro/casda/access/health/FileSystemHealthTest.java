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

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

/**
 * Tests the File System Health check
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class FileSystemHealthTest
{

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testSuccess() throws IOException
    {
        String existsDirName = tempFolder.newFolder("exists").getCanonicalPath();
        File existsDirReadOnly = tempFolder.newFolder("existsRO");
        existsDirReadOnly.setReadOnly(); // Note that this does not work for a folder on windows
        String existsDirReadOnlyName = existsDirReadOnly.getCanonicalPath();
        FileSystemHealth health = new FileSystemHealth(existsDirName, existsDirReadOnlyName, existsDirName);

        Health healthResp = health.health();
        assertThat(healthResp.getStatus(), is(Status.UP));
        assertThat((String) healthResp.getDetails().get("deposit.tools.working.directory"), is(existsDirName));
        assertThat((String) healthResp.getDetails().get("deposit.tools.installation.directory"),
                is(existsDirReadOnlyName));
        assertThat((String) healthResp.getDetails().get("cache.home.dir"), is(existsDirName));
    }

    @Test
    public void testFailMissing() throws IOException
    {
        File existsDir = tempFolder.newFolder("exists");
        String existsDirName = existsDir.getCanonicalPath();
        String notExistsDirName = existsDir.getCanonicalPath() + "not";
        FileSystemHealth health = new FileSystemHealth(existsDirName, existsDirName, notExistsDirName);

        Health healthResp = health.health();
        assertThat(healthResp.getStatus(), is(Status.DOWN));
        assertThat((String) healthResp.getDetails().get("deposit.tools.working.directory"), is(existsDirName));
        assertThat((String) healthResp.getDetails().get("deposit.tools.installation.directory"), is(existsDirName));
        assertThat((String) healthResp.getDetails().get("cache.home.dir"),
                containsString(notExistsDirName + " does not exist"));
    }

    @Test
    public void testFailFile() throws IOException
    {
        File existsDir = tempFolder.newFolder("existsFolder");
        String existsDirName = existsDir.getCanonicalPath();
        String existsFileName = tempFolder.newFile("aFile").getCanonicalPath();

        FileSystemHealth health = new FileSystemHealth(existsDirName, existsFileName, existsDirName);

        Health healthResp = health.health();
        assertThat(healthResp.getStatus(), is(Status.DOWN));
        assertThat((String) healthResp.getDetails().get("deposit.tools.working.directory"), is(existsDirName));
        assertThat((String) healthResp.getDetails().get("deposit.tools.installation.directory"),
                containsString(existsFileName + " is not a directory"));
        assertThat((String) healthResp.getDetails().get("cache.home.dir"), is(existsDirName));
    }

}
