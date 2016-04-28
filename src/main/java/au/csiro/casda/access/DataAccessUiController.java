package au.csiro.casda.access;

import java.io.IOException;

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

import java.util.Collection;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.codec.Charsets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.view.RedirectView;

import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.services.dto.Message.MessageCode;
import uws.UWSException;
import uws.job.UWSJob;

/**
 * UI Controller for the Data Access application.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
@Controller
public class DataAccessUiController
{
    private static final Logger logger = LoggerFactory.getLogger(DataAccessUiController.class);

    private DataAccessService dataAccessService;

    private AccessJobManager accessJobManager;

    /**
     * Constructor
     * 
     * @param dataAccessService
     *            the data access service
     * @param accessJobManager
     *            the job manager service
     */
    @Autowired
    public DataAccessUiController(DataAccessService dataAccessService, AccessJobManager accessJobManager)
    {
        this.dataAccessService = dataAccessService;
        this.accessJobManager = accessJobManager;
    }

    /**
     * Endpoint for the job status page.
     * 
     * @param request
     *            the HTTP servlet request, used to get the URL back to the current page
     * @param model
     *            the web app model
     * @param requestId
     *            the GUID of the job to look up the details
     * @return the view
     * @throws ResourceNotFoundException
     *             if the job could not be found
     */
    @RequestMapping(value = "/requests/{requestId}", method = RequestMethod.GET)
    public String viewJob(HttpServletRequest request, Model model, @PathVariable() String requestId)
            throws ResourceNotFoundException
    {
        logger.info("Data access landing page requested for job {}", requestId);

        DataAccessJob dataAccessJob = dataAccessService.getExistingJob(requestId);
        if (dataAccessJob == null)
        {
            throw new ResourceNotFoundException("Job with request id " + requestId + " not found");
        }

        model.addAttribute("requestId", requestId);
        if (request != null && request.getRequestURL() != null)
        {
            String url = request.getRequestURL().toString();
            model.addAttribute("requestURL", url);

            String baseUrl =
                    url.substring(0, url.length() - request.getRequestURI().length()) + request.getContextPath() + "/";
            model.addAttribute("baseUrl", baseUrl);

            model.addAttribute("downloadLink", url.replace("requests", "download"));
        }

        model.addAttribute("dataAccessJob", dataAccessJob);

        if (dataAccessJob.isReady())
        {
            model.addAttribute("remainingTime", DataAccessUtil.getTimeDifferenceInHoursDisplayString(
                    DateTime.now(DateTimeZone.UTC), dataAccessJob.getExpiredTimestamp()));
        }

        Collection<DownloadFile> downloadFiles = DataAccessUtil.getDataAccessJobDownloadFiles(dataAccessJob,
                dataAccessService.getJobDirectory(dataAccessJob.getRequestId()));
        model.addAttribute("downloadFiles", downloadFiles);

        long totalSizeKb = downloadFiles.stream().mapToLong(file -> file.getSizeKb()).sum();

        // Each file should have a checksum file to accompany it.
        long totalFiles = downloadFiles.size() * 2;

        // display size information if it's an image cube or measurement set request, or if the job is ready (and so all
        // files are available)
        if (CollectionUtils.isNotEmpty(dataAccessJob.getImageCubes())
                || CollectionUtils.isNotEmpty(dataAccessJob.getMeasurementSets()) || dataAccessJob.isReady())
        {
            model.addAttribute("totalSize",
                    String.format(" totalling %s.", FileUtils.byteCountToDisplaySize(totalSizeKb * FileUtils.ONE_KB)));
        }
        else
        {
            model.addAttribute("totalSize", ".");
        }
        model.addAttribute("totalFiles", totalFiles);

        model.addAttribute("pausable", accessJobManager.isPausable(dataAccessJob.getRequestId()));
        UWSJob uwsjob = accessJobManager.getJobStatus(dataAccessJob.getRequestId());
        model.addAttribute("executionPhase", uwsjob.getPhase().name());

        logger.debug("Going to display page");
        return "viewJob";
    }

    /**
     * downloads the reqeust id in a txt file
     * 
     * @param request
     *            the HTTP servlet request, used to get the URL back to the current page
     * @param response
     *            the HTTP servlet Response, provides  HTTP-specific functionality in sending a response
     * @param model
     *            the web app model
     * @param requestId
     *            the GUID of the job to look up the details
     * @throws IOException
     *             an exception thrown when writing link to file
     */
    @RequestMapping(value = "/download/{requestId}", method = RequestMethod.GET)
    public void downloadLink(HttpServletRequest request, HttpServletResponse response, Model model,
            @PathVariable() String requestId) throws IOException
    {
        String url = request.getRequestURL().toString();
        
        String baseUrl =
                url.substring(0, url.length() - request.getRequestURI().length()) + request.getContextPath() + "/";
        
        response.setContentType("text/plain");
        response.setHeader("Content-Disposition", "attachment;filename=" + requestId + ".txt");

        ServletOutputStream out = response.getOutputStream();

        DataAccessJob dataAccessJob = dataAccessService.getExistingJob(requestId);
        
        Collection<DownloadFile> downloadFiles = DataAccessUtil.getDataAccessJobDownloadFiles(dataAccessJob,
                dataAccessService.getJobDirectory(dataAccessJob.getRequestId()));
        
        for(DownloadFile df : downloadFiles)
        {
            String fileLink = DataAccessUtil.getRelativeLinkForFile(dataAccessJob.getDownloadMode(), 
                    dataAccessJob.getRequestId(), df.getFilename());
            out.write(baseUrl.concat(fileLink).concat("\r\n").getBytes(Charsets.UTF_8));
            out.write(baseUrl.concat(fileLink).concat(".checksum\r\n").getBytes(Charsets.UTF_8));
        }

        out.flush();
        out.close();
    }

    /**
     * Login page
     * 
     * @param model
     *            Model
     * @return login String view name of login
     */
    @RequestMapping(value = "/login")
    public String login(Model model)
    {
        return "login";
    }

    /**
     * Home page
     * 
     * @param model
     *            Model
     * @return home String view name of home page
     */
    @RequestMapping(value = "/")
    public String home(Model model)
    {
        return "home";
    }

    /**
     * Allow admin to do the following: Restart a failed job, Cancel a job, Pause a job, Priority of a job
     * 
     * @param request
     *            the HTTP servlet request, used to get the URL back to the current page
     * @param model
     *            the web app model
     * @param redirectAttributes
     *            attributes for a redirect
     * @param action
     *            restart or cancel or pause or priority of a job
     * @param requestId
     *            the GUID of the job
     * @return redirect to caller page
     * @throws ResourceNotFoundException
     *             if the job could not be found
     * @throws ResourceIllegalStateException
     *             if the job is not in a legal state for this operation
     */
    @RequestMapping(value = "/requests/modifyJob", method = RequestMethod.POST)
    public RedirectView modifyJob(HttpServletRequest request, Model model, final RedirectAttributes redirectAttributes,
            @RequestParam String action, @RequestParam String requestId)
                    throws ResourceNotFoundException, ResourceIllegalStateException
    {
        RedirectView redirect = null;
        logger.debug("modify job {} request {}", action, requestId);
        try
        {
            if ("restart".equalsIgnoreCase(action))
            {
                logger.debug("restart: " + requestId);
                redirect = new RedirectView("/requests/" + requestId, true);
                try
                {
                    accessJobManager.retryJob(requestId);
                    redirectAttributes.addFlashAttribute("message", MessageCode.SUCCESS
                            + ". Data access job for request " + requestId + " submitted for retry");
                }
                catch (ResourceNotFoundException ex)
                {
                    redirectAttributes.addFlashAttribute("message",
                            MessageCode.FAILURE + ". Data access job for request " + requestId + " does not exist");
                }
                catch (ResourceIllegalStateException ex)
                {
                    redirectAttributes.addFlashAttribute("message", MessageCode.FAILURE
                            + ". Data access job for request " + requestId + " could not be retried");
                }
                catch (ScheduleJobException e)
                {
                    redirectAttributes.addFlashAttribute("message",
                            MessageCode.FAILURE + ". Data access job for request " + requestId
                                    + " could not be retried due to scheduling error");
                }
            }
            else if ("cancel".equalsIgnoreCase(action))
            {
                logger.debug("cancel: " + requestId);
                redirect = new RedirectView("/requests/" + requestId, true);
                accessJobManager.cancelJob(requestId, DateTime.now(DateTimeZone.UTC));
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". Data access job for request " + requestId + " cancelled");

            }
            else if ("pause".equalsIgnoreCase(action))
            {
                logger.debug("pause: " + requestId);
                redirect = new RedirectView("/requests/" + requestId, true);
                accessJobManager.pauseJob(requestId);
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". Data access job for request " + requestId + " paused");

            }
            else if ("resume".equalsIgnoreCase(action))
            {
                logger.debug("resume: " + requestId);
                redirect = new RedirectView("/requests/" + requestId, true);
                try
                {
                    accessJobManager.retryJob(requestId);
                    redirectAttributes.addFlashAttribute("message", MessageCode.SUCCESS
                            + ". Data access job for request " + requestId + " submitted for resume");
                }
                catch (ResourceNotFoundException ex)
                {
                    redirectAttributes.addFlashAttribute("message",
                            MessageCode.FAILURE + ". Data access job for request " + requestId + " does not exist");
                }
                catch (ResourceIllegalStateException ex)
                {
                    redirectAttributes.addFlashAttribute("message", MessageCode.FAILURE
                            + ". Data access job for request " + requestId + " could not be resumed");
                }
                catch (ScheduleJobException e)
                {
                    redirectAttributes.addFlashAttribute("message",
                            MessageCode.FAILURE + ". Data access job for request " + requestId
                                    + " could not be resumed due to scheduling error");
                }

            }
            else if ("back".equalsIgnoreCase(action))
            {
                logger.debug("pause: " + requestId);
                redirect = new RedirectView("/jobs", true);
            }
            else if ("first".equalsIgnoreCase(action))
            {
                logger.debug("first: " + requestId);
                redirect = new RedirectView("/jobs", true);
                accessJobManager.prioritise(requestId, 0);
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". Data access job for request " + requestId + " priority set to First");

            }
            else if ("last".equalsIgnoreCase(action))
            {
                logger.debug("last: " + requestId);
                redirect = new RedirectView("/jobs", true);
                accessJobManager.prioritise(requestId, Integer.MAX_VALUE);
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". Data access job for request " + requestId + " priority set to Last");

            }
            else if (StringUtils.startsWith(action, "Pause ") && StringUtils.endsWith(action, " Queue"))
            {
                logger.debug("pause queue: " + requestId);
                redirect = new RedirectView("/jobs", true);
                accessJobManager.pauseQueue(requestId);
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". " + requestId + " Queue paused");
            }
            else if (StringUtils.startsWith(action, "Resume ") && StringUtils.endsWith(action, " Queue"))
            {
                logger.debug("resume queue: " + requestId);
                redirect = new RedirectView("/jobs", true);
                accessJobManager.unpauseQueue(requestId);
                redirectAttributes.addFlashAttribute("message",
                        MessageCode.SUCCESS + ". " + requestId + " Queue resumed");
            }
        }
        catch (UWSException ex)
        {
            logger.error(action + " : " + requestId, ex);
            redirectAttributes.addFlashAttribute("message",
                    MessageCode.FAILURE + ". No data access job could be found for request " + requestId);
        }

        if (redirect == null)
        {
            String referer = request.getHeader("referer");
            if (StringUtils.isBlank(referer) || referer.endsWith("jobs"))
            {
                redirect = new RedirectView("/jobs", true);
            }
            else
            {
                redirect = new RedirectView("/requests/" + requestId, true);
            }
        }
        return redirect;
    }

}
