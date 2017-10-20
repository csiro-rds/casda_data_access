package au.csiro.casda.access;

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

import java.util.Arrays;
import java.util.List;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import com.wordnik.swagger.annotations.Api;

import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;

/**
 * UI Controller for the Data Access Jobs.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
@Api(value = "Requests", description = "Data Access Requests")
@RestController
public class DataAccessJobController
{

    private static Logger logger = LoggerFactory.getLogger(DataAccessJobController.class);

    private final AccessJobManager accessJobManager;

    /**
     * Create a new DataAccessJobController instance.
     * 
     * @param accessJobManager
     *            the access job manager for interacting with the UWS job queue
     */
    @Autowired
    public DataAccessJobController(AccessJobManager accessJobManager)
    {
        this.accessJobManager = accessJobManager;
    }

    /**
     * Web end point for creating a new data access job. This will register the request, issue a unique id (a UUID) and
     * then create and schedule a job to retrieve and package the data ready for use.
     * 
     * @param job
     *            The details of the data to be retrieved, the delivery method and the user requesting the data.
     * @return A response with code 201 and the UUID issued for the request.
     */
    @RequestMapping(//
            method = RequestMethod.POST, //
            value = "/requests", //
            consumes = { MediaType.APPLICATION_FORM_URLENCODED_VALUE, // the original, as used by DAP
                    MediaType.MULTIPART_FORM_DATA_VALUE, // to support curl -F usage
                    MediaType.TEXT_PLAIN_VALUE // to support Swagger
    }, //
            produces = MediaType.APPLICATION_JSON_VALUE)
    public @ResponseBody ResponseEntity<List<String>> createJob(@Valid @ModelAttribute JobDto job)
    {
        logger.info("Hit the controller for the '/requests with ids={}' url mapping - servicing request",
                String.join(",", job.getIds()));

        DataAccessJob newJob = accessJobManager.createDataAccessJob(job);

        try
        {
            accessJobManager.scheduleJob(newJob.getRequestId());
        }
        catch (ResourceNotFoundException | ResourceIllegalStateException | ScheduleJobException e)
        {
            // suppress exception that this couldn't be scheduled because the job is created and we assume the user or
            // admin will follow it up.
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add("Unable to schedule job " + newJob);
            logger.error(builder.toString(), e);
        }

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.setLocation(ServletUriComponentsBuilder.fromCurrentRequestUri().path("/{id}/page/1")
                .buildAndExpand(newJob.getRequestId()).toUri());

        return new ResponseEntity<List<String>>(Arrays.asList(newJob.getRequestId()), httpHeaders, HttpStatus.CREATED);
    }

}
