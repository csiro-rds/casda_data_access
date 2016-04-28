package au.csiro;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.commons.lang3.SystemUtils;

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
 * Common test utility methods
 * <p>
 * Copyright 2014, CSIRO Australia All rights reserved.
 */
public final class TestUtils
{
    /**
     * @param output
     *            the output of the external echo command
     * @return an EL string suitable for parsing by a SimpleToolJobProcessBuilder to create an ProcessJob that will
     *         execute an external echo command
     */
    public static String getCommandAndArgsElStringForEchoOutput(String output)
    {
        return getCommandAndArgsElStringForEchoOutput(output, false);
    }

    /**
     * @param output
     *            the output of the external echo command
     * @param fail
     *            whether the external command should fail
     * @return an EL string suitable for parsing by a SimpleToolJobProcessBuilder to create an ProcessJob that will
     *         execute an external echo command
     */
    public static String getCommandAndArgsElStringForEchoOutput(String output, boolean fail)
    {
        if (SystemUtils.IS_OS_WINDOWS)
        {
            return "{\"cmd\",\"/c\",\"\"\"echo " + output + "&& exit " + (fail ? 1 : 0) + "\"\"\"}";
        }
        else
        {
            return "{\"bash\",\"-c\",\"echo '" + output + "' && exit " + (fail ? 1 : 0) + "\"}";
        }
    }

    /**
     * @param filename
     *            the filename whose contents will be output
     * @return an EL string suitable for parsing by a SimpleToolJobProcessBuilder to create an ProcessJob that will
     *         execute an external 'cat' command
     */
    public static String getCommandAndArgsElStringForFileContentsOutput(String filename)
    {
        return getCommandAndArgsElStringForFileContentsOutput(filename, false);
    }

    /**
     * @param filename
     *            the filename whose contents will be output
     * @param fail
     *            whether the external command should fail
     * @return an EL string suitable for parsing by a SimpleToolJobProcessBuilder to create an ProcessJob that will
     *         execute an external 'cat' command
     */
    public static String getCommandAndArgsElStringForFileContentsOutput(String filename, boolean fail)
    {
        if (SystemUtils.IS_OS_WINDOWS)
        {
            return "{\"cmd\",\"/c\",\"\"\"type " + filename.replace("/", "\\\\") + "&& exit " + (fail ? 1 : 0)
                    + "\"\"\"}";
        }
        else
        {
            return "{\"bash\",\"-c\",\"cat '" + filename + "' && exit " + (fail ? 1 : 0) + "\"}";
        }
    }

    public static void makeFileUnreadable(File dataFile) throws IOException
    {
        if (SystemUtils.IS_OS_WINDOWS)
        {
            AclFileAttributeView aclAttr = Files.getFileAttributeView(dataFile.toPath(), AclFileAttributeView.class);
            UserPrincipalLookupService upls = dataFile.toPath().getFileSystem().getUserPrincipalLookupService();
            UserPrincipal user = upls.lookupPrincipalByName(System.getProperty("user.name"));
            AclEntry.Builder denyBuilder = AclEntry.newBuilder();
            denyBuilder.setPermissions(EnumSet.allOf(AclEntryPermission.class));
            denyBuilder.setType(AclEntryType.DENY);
            denyBuilder.setPrincipal(user);
            aclAttr.setAcl(Arrays.asList(denyBuilder.build()));
        }
        else
        {
            Files.setPosixFilePermissions(dataFile.getParentFile().toPath(),
                    EnumSet.complementOf(EnumSet.allOf(PosixFilePermission.class)));
        }
    }

}
