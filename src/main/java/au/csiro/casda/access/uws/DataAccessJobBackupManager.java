package au.csiro.casda.access.uws;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.Charsets;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uws.UWSException;
import uws.job.ExecutionPhase;
import uws.job.JobList;
import uws.job.UWSJob;
import uws.job.user.JobOwner;
import uws.service.UWS;
import uws.service.backup.DefaultUWSBackupManager;
import uws.service.file.UWSFileManager;
import au.csiro.casda.access.DataAccessApplication;
import au.csiro.casda.logging.CasdaLoggingSettings;

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
 * A specialised implementation of the UWS backup manager which will restore the phase of the recovered job.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class DataAccessJobBackupManager extends DefaultUWSBackupManager
{

    private static final Logger logger = LoggerFactory.getLogger(DataAccessJobBackupManager.class);

    private CasdaLoggingSettings loggingSettings = new CasdaLoggingSettings(DataAccessApplication.APPLICATION_NAME);

    /**
     * Create a new instance of our DataAccessJobBackupManager.
     * 
     * @param uws
     *            The UWS to save/restore.
     */
    public DataAccessJobBackupManager(UWS uws)
    {
        super(uws);
    }

    /*
     * If the backup is trying to retore a paused job, it will pause the queue. Otherwise it will call the regular
     * restoreJob method.
     */
    @Override
    protected boolean restoreJob(final JSONObject json, Map<String, JobOwner> users) throws UWSException
    {
        try
        {
            if (json.getString(UWSJob.PARAM_JOB_ID).startsWith(PriorityQueueExecutionManager.PAUSE_QUEUE_JOB_PREFIX))
            {
                String jobListName = json.getString("jobListName");
                JobList joblist = uws.getJobList(jobListName);
                if (joblist != null && joblist.getExecutionManager() instanceof PriorityQueueExecutionManager)
                {
                    ((PriorityQueueExecutionManager) joblist.getExecutionManager()).pauseQueue();
                }
                return false; // response is false because we do not want it to be counted as a restored job
            }
            else
            {
                return super.restoreJob(json, users);
            }
        }
        catch (JSONException e)
        {
            logger.error("Couldn't read job", e);
            return false;
        }
    }

    /*
     * This is mostly copied from DefaultUWSBackupManager.saveAll(), but I have removed the part that writes user
     * information because we don't use it and the current config would prevent the jobs from being reloaded on start
     * up, and I have updated this method to read our ordered job list to preserve the queue order.
     */
    @Override
    public int[] saveAll()
    {
        loggingSettings.addGeneralLoggingSettings();
        loggingSettings.addLoggingInstanceId();
        logger.debug("Backing up job queue");

        if (!enabled)
        {
            return null;
        }

        int nbSavedJobs = 0, nbSavedOwners = 0;
        int nbJobs = 0, nbOwners = 0;

        UWSFileManager fileManager = uws.getFileManager();
        PrintWriter writer = null;
        try
        {
            // Create a writer toward the backup file:
            writer = new PrintWriter(new OutputStreamWriter(fileManager.getBackupOutput(), Charsets.UTF_8));
            JSONWriter out = new JSONWriter(writer);

            // JSON structure: { date: ..., users: [...], jobs: [...] }
            out.object();

            // Write the backup date:
            out.key("date").value((new Date()).toString());

            // Write all jobs:
            out.key("jobs").array();
            for (JobList jl : uws)
            {
                List<UWSJob> orderedJobList = new ArrayList<>();
                if (jl.getExecutionManager() instanceof PriorityQueueExecutionManager)
                {
                    orderedJobList = ((PriorityQueueExecutionManager) jl.getExecutionManager()).getOrderedJobList();
                }
                else
                {
                    for (Iterator<UWSJob> jobListIterator = jl.getJobs(); jobListIterator.hasNext();)
                    {
                        orderedJobList.add(jobListIterator.next());
                    }
                }
                Set<String> requestIds = orderedJobList.stream().map(job -> job.getJobId()).collect(Collectors.toSet());
                for (UWSJob job : jl)
                {
                    if (!requestIds.contains(job.getJobId()))
                    {
                        orderedJobList.add(job);
                    }
                }
                for (UWSJob job : orderedJobList)
                {
                    nbJobs++;
                    try
                    {
                        out.value(getJSONJob(job, jl.getName()));
                        nbSavedJobs++;
                        writer.flush();
                    }
                    catch (UWSException ue)
                    {
                        getLogger().error("Unexpected UWS error while saving the job '" + job.getJobId() + "' !", ue);
                    }
                    catch (JSONException je)
                    {
                        getLogger().error("Unexpected JSON error while saving the job '" + job.getJobId() + "' !", je);
                    }
                }
            }
            out.endArray();

            // End the general structure:
            out.endObject();

        }
        catch (JSONException je)
        {
            getLogger().error("Unexpected JSON error while saving the whole UWS !", je);
        }
        catch (IOException ie)
        {
            getLogger().error("Unexpected IO error while saving the whole UWS !", ie);
        }
        finally
        {
            // Close the writer:
            if (writer != null)
            {
                writer.close();
            }
        }

        // Build the report and log it:
        int[] report = new int[] { nbSavedJobs, nbJobs, nbSavedOwners, nbOwners };
        getLogger().uwsSaved(uws, report);

        lastBackup = new Date();

        return report;
    }

    @Override
    protected void restoreOtherJobParams(JSONObject json, UWSJob job) throws UWSException
    {
        String phaseWhenShutdown;
        try
        {
            phaseWhenShutdown = (String) json.get(UWSJob.PARAM_PHASE);
        }
        catch (JSONException e)
        {
            throw new UWSException(e);
        }

        if (logger.isDebugEnabled() && !"ERROR".equals(phaseWhenShutdown))
        {
            logger.debug("Job id {} was at phase {} when shut down", job.getJobId(), phaseWhenShutdown);
        }
        // Set status as ready to start if it was queued or executing before
        if ("PENDING".equals(phaseWhenShutdown) || "EXECUTING".equals(phaseWhenShutdown)
                || "QUEUED".equals(phaseWhenShutdown))
        {
            job.setPhase(ExecutionPhase.PENDING);
            job.addOrUpdateParameter(UWSJob.PARAM_PHASE, UWSJob.PHASE_RUN);
        }
        else
        {
            job.setPhase(ExecutionPhase.valueOf(phaseWhenShutdown), true);
        }

        super.restoreOtherJobParams(json, job);
    }

}
