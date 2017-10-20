package au.csiro.casda.access;

import java.util.HashMap;
import java.util.Map;

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

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;

/**
 * Pojo with validation to create new data access jobs
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class JobDto
{

    @NotEmpty
    private String[] ids;

    private String userIdent;

    private String userLoginSystem;

    private String userName;

    private String userEmail;

    private Map<String, String[]> params;

    @NotNull
    private CasdaDownloadMode downloadMode;
    
    private String jobType;

    private String downloadFormat;

    public String[] getIds()
    {
        return ids;
    }

    public void setIds(String[] ids)
    {
        this.ids = ids;
    }

    /**
     * @return The map of the job parameters. Will never be null. 
     */
    public Map<String, String[]> getParams()
    {
        if (params == null)
        {
            params = new HashMap<>();
        }
        return params;
    }

    public void setParams(Map<String, String[]> params)
    {
        this.params = params;
    }

    public String getUserIdent()
    {
        return userIdent;
    }

    public void setUserIdent(String userIdent)
    {
        this.userIdent = userIdent;
    }

    public String getUserLoginSystem()
    {
        return userLoginSystem;
    }

    public void setUserLoginSystem(String userLoginSystem)
    {
        this.userLoginSystem = userLoginSystem;
    }

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getUserEmail()
    {
        return userEmail;
    }

    public void setUserEmail(String userEmail)
    {
        this.userEmail = userEmail;
    }

    public CasdaDownloadMode getDownloadMode()
    {
        return downloadMode;
    }

    public void setDownloadMode(CasdaDownloadMode downloadMode)
    {
        this.downloadMode = downloadMode;
    }

    public String getDownloadFormat()
    {
        return downloadFormat;
    }

    public void setDownloadFormat(String downloadFormat)
    {
        this.downloadFormat = downloadFormat;
    }

	public String getJobType()
	{
		return jobType;
	}

	public void setJobType(String jobType) 
	{
		this.jobType = jobType;
	}
}
