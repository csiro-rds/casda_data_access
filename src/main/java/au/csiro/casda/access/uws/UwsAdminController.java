package au.csiro.casda.access.uws;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.wordnik.swagger.annotations.Api;

import au.csiro.casda.access.ResourceIllegalStateException;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.services.dto.Message.MessageCode;
import au.csiro.casda.services.dto.MessageDTO;
import uws.UWSException;

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
 * UI Controller for administering the UWS job system for processing data access requests.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
@Api(value = "Requests", description = "Admin of Data Access Requests")
@Controller
public class UwsAdminController
{
    private static final Logger logger = LoggerFactory.getLogger(UwsAdminController.class);

    @Autowired
    private AccessJobManager accessJobManager;

    @Autowired
    private DataAccessJobRepository dataAccessJobRepository;

    /**
     * Display the queue of data access jobs on the server.
     * 
     * @param request
     *            The web request.
     * @param model
     *            The model to be populated with job queue details, for use by the jsp.
     * @return The name of the jsp page to be displayed.
     */
    @RequestMapping(value = "/jobs", method = RequestMethod.GET)
    public String viewJobQueue(HttpServletRequest request, Model model)
    {

        logger.info("Data access job queue requested.");

        List<DataAccessJobDto> jobList = accessJobManager.getRunningJobList();
        List<DataAccessJobDto> availableJobList = accessJobManager.getRecentlyCompletedJobList();
        List<DataAccessJobDto> failedJobList = accessJobManager.getRecentlyFailedJobList();

        model.addAttribute("jobList", jobList);
        model.addAttribute("failed", failedJobList);
        model.addAttribute("available", availableJobList);
        model.addAttribute("faileddays", accessJobManager.getDisplayDaysOfFailedJobs());
        model.addAttribute("availabledays", accessJobManager.getDisplayDaysOfAvailableJobs());

        model.addAttribute("pausableQueues",
                Arrays.asList(
                        new Queue(AccessJobManager.CATEGORY_A_JOB_LIST_NAME,
                                accessJobManager.isQueuePaused(AccessJobManager.CATEGORY_A_JOB_LIST_NAME),
                                String.format("%s runs jobs with size <= %s", AccessJobManager.CATEGORY_A_JOB_LIST_NAME,
                                        FileUtils.byteCountToDisplaySize(
                                                accessJobManager.getCategoryAJobMaxSize() * FileUtils.ONE_KB))),
                new Queue(AccessJobManager.CATEGORY_B_JOB_LIST_NAME,
                        accessJobManager.isQueuePaused(AccessJobManager.CATEGORY_B_JOB_LIST_NAME),
                        String.format("%s runs jobs with size > %s", AccessJobManager.CATEGORY_B_JOB_LIST_NAME,
                                FileUtils.byteCountToDisplaySize(
                                        accessJobManager.getCategoryAJobMaxSize() * FileUtils.ONE_KB)))));

        return "jobQueue";
    }

    /**
     * Admin endpoint to allow a job to be retried or rerun.
     * 
     * @param requestId
     *            The request id of the job to be retried.
     * @return A MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws UWSException
     *             If there is a problem retrying a job.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     * @throws ScheduleJobException
     *             if the job could not be (re)scheduled
     */
    @RequestMapping(value = "/requests/{requestId}/retry", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> retryJob(@PathVariable() String requestId)
            throws UWSException, ResourceNotFoundException, ResourceIllegalStateException, ScheduleJobException
    {
        logger.info("Hit the controller for the '/requests/{}/retry' url mapping - servicing request", requestId);

        accessJobManager.retryJob(requestId);
        return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.SUCCESS,
                "Data access job for request " + requestId + " submitted for retry"), HttpStatus.OK);
    }

    /**
     * Admin endpoint to allow a job to be prioritised.
     * 
     * @param requestId
     *            The request id to be prioritised.
     * @param position
     *            the position index in the queue (doesn't include running jobs)
     * @return A MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws UWSException
     *             If there is a problem prioritising the job.
     */
    @RequestMapping(value = "/requests/{requestId}/prioritise", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> prioritise(@PathVariable() String requestId,
            @RequestParam(required = true) int position) throws UWSException
    {
        logger.info("Hit the controller for the '/requests/{}/prioritise?position={}' url mapping - servicing request",
                requestId, position);

        if (accessJobManager.prioritise(requestId, position))
        {
            return new ResponseEntity<MessageDTO>(
                    new MessageDTO(MessageCode.SUCCESS,
                            "Data access job for request " + requestId + " priority set to " + position),
                    HttpStatus.OK);
        }
        return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.FAILURE, "No suitable job found"),
                HttpStatus.NOT_FOUND);
    }

    /**
     * Admin endpoint to allow a job to be cancelled.
     * 
     * @param requestId
     *            The request id of the job to be cancelled.
     * @return A MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    @RequestMapping(value = "/requests/{requestId}/cancel", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> cancel(@PathVariable() String requestId)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        logger.info("Hit the controller for the '/requests/{}/cancel' url mapping - servicing request", requestId);

        accessJobManager.cancelJob(requestId, new DateTime(DateTimeZone.UTC));
        return new ResponseEntity<MessageDTO>(
                new MessageDTO(MessageCode.SUCCESS, "Data access job for request " + requestId + " cancelled"),
                HttpStatus.OK);
    }

    /**
     * Admin endpoint to allow a job to be paused.
     * 
     * @param requestId
     *            The request id of the job to be paused.
     * @return A MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    @RequestMapping(value = "/requests/{requestId}/pause", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> pause(@PathVariable() String requestId)
            throws ResourceNotFoundException, ResourceIllegalStateException
    {
        logger.info("Hit the controller for the '/requests/{}/pause' url mapping - servicing request", requestId);

        accessJobManager.pauseJob(requestId);
        return new ResponseEntity<MessageDTO>(
                new MessageDTO(MessageCode.SUCCESS, "Data access job for request " + requestId + " paused"),
                HttpStatus.OK);
    }

    /**
     * Put method to pause a given queue
     * 
     * @param queue
     *            the queue name to pause
     * @return MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws UWSException
     *             If there is a problem unpausing the queue
     */
    @RequestMapping(value = "/queues/{queue}/pause", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> pauseQueue(@PathVariable() String queue) throws UWSException
    {
        if (accessJobManager.pauseQueue(queue))
        {
            return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.SUCCESS, "Queue " + queue + " paused"),
                    HttpStatus.OK);
        }
        return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.FAILURE, "No suitable queue found"),
                HttpStatus.NOT_FOUND);
    }

    /**
     * Put method to unpause a given queue
     * 
     * @param queue
     *            the queue name to unpause
     * @return MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws UWSException
     *             If there is a problem unpausing the queue
     */
    @RequestMapping(value = "/queues/{queue}/resume", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> unpauseQueue(@PathVariable() String queue) throws UWSException
    {
        if (accessJobManager.unpauseQueue(queue))
        {
            return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.SUCCESS, "Queue " + queue + " resumed"),
                    HttpStatus.OK);
        }
        return new ResponseEntity<MessageDTO>(new MessageDTO(MessageCode.FAILURE, "No suitable queue found"),
                HttpStatus.NOT_FOUND);
    }

    /**
     * Admin endpoint to allow a job to be resumed. If the request id is 'failed' or 'preparing' it will attempt to
     * restart all the failed or preparing jobs displayed on the page - these are mainly intended for testing purposes.
     * 
     * @param requestId
     *            The request id of the job to be resumed (or 'failed' or 'preparing').
     * @return A MessageDTO with the result, and the response code of 200 if found, 404 if not
     * @throws UWSException
     *             If there is a problem resuming the job.
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     * @throws ScheduleJobException
     *             if the job could not be resumed
     */
    @RequestMapping(value = "/requests/{requestId}/resume", method = RequestMethod.PUT)
    public @ResponseBody ResponseEntity<MessageDTO> resume(@PathVariable() String requestId)
            throws UWSException, ResourceNotFoundException, ResourceIllegalStateException, ScheduleJobException
    {
        logger.info("Hit the controller for the '/requests/{}/resume' url mapping - servicing request", requestId);

        if ("failed".equalsIgnoreCase(requestId))
        {
            if (restartAllFailedJobs())
            {
                return new ResponseEntity<MessageDTO>(
                        new MessageDTO(MessageCode.SUCCESS, "All failed data access jobs were resumed"), HttpStatus.OK);
            }
            else
            {
                return new ResponseEntity<MessageDTO>(
                        new MessageDTO(MessageCode.FAILURE, "Unable to resume all unqueued data access jobs."),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        else if ("preparing".equalsIgnoreCase(requestId))
        {
            if (restartAllPreparingJobs())
            {
                return new ResponseEntity<MessageDTO>(
                        new MessageDTO(MessageCode.SUCCESS, "All preparing data access jobs were resumed"),
                        HttpStatus.OK);
            }
            else
            {
                return new ResponseEntity<MessageDTO>(
                        new MessageDTO(MessageCode.FAILURE, "Unable to resume all preparing data access jobs."),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        else
        {
            accessJobManager.retryJob(requestId);
            return new ResponseEntity<MessageDTO>(
                    new MessageDTO(MessageCode.SUCCESS, "Data access job for request " + requestId + " resumed"),
                    HttpStatus.OK);
        }
    }

    private boolean restartAllFailedJobs()
    {
        // these are ordered by descending failure order
        List<DataAccessJob> failedJobs = dataAccessJobRepository.findFailedJobsAfterTime(
                DateTime.now(DateTimeZone.UTC).minusDays(accessJobManager.getDisplayDaysOfAvailableJobs()));
        Collections.reverse(failedJobs);
        return restartJobs(failedJobs);
    }

    private boolean restartAllPreparingJobs()
    {
        // these are ordered by created timestamp
        List<DataAccessJob> preparingJobs = dataAccessJobRepository.findPreparingJobs();
        return restartJobs(preparingJobs);
    }

    private boolean restartJobs(List<DataAccessJob> jobs)
    {
        logger.info("Number of jobs to restart {}", jobs.size());
        boolean success = true;
        for (DataAccessJob preparingJob : jobs)
        {
            try
            {
                accessJobManager.retryJob(preparingJob.getRequestId());
            }
            catch (Throwable e)
            {
                logger.error("Couldn't restart job with request id: " + preparingJob.getRequestId());
                success = false;
            }
        }
        return success;
    }

    /**
     * Queue model for display on uws admin page
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public static class Queue
    {
        private String name;
        private boolean paused;
        private String description;

        /**
         * Constructor
         * 
         * @param name
         *            uws queue name
         * @param paused
         *            whether the queue is paused
         * @param description
         *            the description to display for this queue
         */
        public Queue(String name, boolean paused, String description)
        {
            this.name = name;
            this.paused = paused;
            this.description = description;
        }

        public String getName()
        {
            return name;
        }

        public boolean isPaused()
        {
            return paused;
        }

        public String getDescription()
        {
            return description;
        }

    }

}
