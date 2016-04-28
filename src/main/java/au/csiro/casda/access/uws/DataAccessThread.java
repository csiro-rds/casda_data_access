package au.csiro.casda.access.uws;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.DataAccessApplication;
import au.csiro.casda.access.DataAccessUtil;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.cache.CacheException;
import au.csiro.casda.access.cache.CacheFullException;
import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.rest.CatalogueRetrievalException;
import au.csiro.casda.access.rest.CreateChecksumException;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaLoggingSettings;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;
import uws.UWSException;
import uws.job.ErrorType;
import uws.job.JobThread;
import uws.job.Result;
import uws.job.UWSJob;

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
 * UWS JobThread that will handle processing a data access request when the job reaches the top of the queue.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class DataAccessThread extends JobThread
{

    private static Logger logger = LoggerFactory.getLogger(DataAccessThread.class);

    private final DataAccessService dataAccessService;
    private final Packager packager;
    private final int hoursToExpiryDefault;
    private final int hoursToExpirySiapSync;

    /**
     * Create a new DataAccessThread instance.
     * 
     * @param uwsJob
     *            The job the thread will be processing.
     * @param dataAccessService
     *            The service instance managing data access objects.
     * @param packager
     *            The packager instance which will be doing the work.
     * @param hoursToExpiryDefault
     *            the default number of hours to expiry for a job
     * @param hoursToExpirySiapSync
     *            the number of hours to expiry for a SIAP sync job
     * @throws UWSException
     *             If the job thread cannot be created.
     */
    public DataAccessThread(UWSJob uwsJob, DataAccessService dataAccessService, Packager packager,
            int hoursToExpiryDefault, int hoursToExpirySiapSync) throws UWSException
    {
        super(uwsJob);
        this.dataAccessService = dataAccessService;
        this.packager = packager;
        this.hoursToExpiryDefault = hoursToExpiryDefault;
        this.hoursToExpirySiapSync = hoursToExpirySiapSync;
    }

    @Override
    protected void jobWork() throws UWSException, InterruptedException
    {
        UWSJob job = getJob();

        /*
         * Sanity check to make sure the owner has not been set because our use of UWS does not currently support that.
         */
        if (job.getOwner() != null)
        {
            throw new IllegalStateException("USWJob with id '" + job.getJobId()
                    + "' has an owner set but we don't currently support an owner for UWSJobs");
        }

        String id = (String) job.getParameter(AccessJobManager.REQUEST_ID);

        this.updateLoggingSettings(job.getJobId());

        boolean success = false;
        Exception exception = null;

        DataAccessJob dataAccessJob = null;

        try
        {
            Result result = this.createResult();

            logger.info(CasdaDataAccessEvents.E038.messageBuilder().add(id)
                    .addCustomMessage("Working on UWS job " + job).toString());

            long start = System.currentTimeMillis();

            dataAccessJob = dataAccessService.getExistingJob(id);
            if (dataAccessJob == null)
            {
                throw new ResourceNotFoundException("No data access job with request id " + id);
            }

            /*
             * We need to set a initial value for the expiryDate so that any files in the cache associated with this job
             * will have been essentially 'claimed' by the job and therefore won't be cleaned up as part of the usual
             * purging process.
             */
            int hoursToExpiryForJob = dataAccessJob.getDownloadMode() == CasdaDownloadMode.SIAP_SYNC
                    ? hoursToExpirySiapSync : hoursToExpiryDefault;
            dataAccessService.updateExpiryDate(id, DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob));

            logger.debug("Packaging files...");
            if (dataAccessJob.getDownloadMode() == CasdaDownloadMode.SIAP_ASYNC && dataAccessJob.getFileCount() == 0)
            {
                String errorMessage;
                if (DataAccessUtil.imageCutoutsShouldBeCreated(dataAccessJob))
                {
                    errorMessage = "UsageError: None of the selected image cubes had data matching the "
                            + "supplied cutout parameters.";
                }
                else
                {
                    errorMessage = "UsageError: No data products were selected for retrieval.";
                }
                dataAccessJob.setErrorMessage(errorMessage);
                dataAccessService.saveJob(dataAccessJob);
                throw new IllegalStateException(errorMessage);
            }

            Packager.Result packagerResult = packager.pack(dataAccessJob, hoursToExpiryForJob);
            logger.debug("Successfully packaged files");

            /*
             * Update the job expriy date based on the time the packaging finished.
             */
            dataAccessService.markRequestCompleted(id, packagerResult.getExpiryDate());
            logger.debug("Marking DataAccessJob {} READY", id);

            this.publishResult(result);

            long duration = System.currentTimeMillis() - start;

            logger.info(CasdaDataAccessEvents.E039.messageBuilder().addTimeTaken(duration).add(id)
                    .add(packagerResult.getTotalSizeKb())
                    .add(packagerResult.getTotalSizeKb() - packagerResult.getCachedSizeKb())
                    .add(packagerResult.getCachedSizeKb()).addCustomMessage("Finished UWS job " + job).toString());

            success = true;
        }
        catch (InterruptedException e)
        {
            // let UWS handle the interrupted thread
            logger.info("Thread interrupted and will throw to UWS to handle it", e);
            throw e;
        }
        catch (CacheFullException e)
        {
            logger.error(CasdaDataAccessEvents.E082.messageBuilder().add(id).toString(), e);
            exception = e;
        }
        catch (CacheException e)
        {
            logger.error(CasdaDataAccessEvents.E147.messageBuilder().add(id).toString(), e);
            exception = e;
        }
        catch (CatalogueRetrievalException e)
        {
            logger.error(CasdaDataAccessEvents.E102.messageBuilder().add("job").add(id).toString(), e);
            exception = e;
        }
        catch (CreateChecksumException e)
        {
            logger.error(CasdaDataAccessEvents.E106.messageBuilder().add(id).toString(), e);
            exception = e;
        }
        catch (Exception e)
        {
            // log and report the error.
            CasdaMessageBuilder<?> builder =
                    CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
            builder.add(String.format("An unexpected exception occured trying to process data access job '%s'", id));
            logger.error(builder.toString(), e);
            exception = e;
        }

        if (!success)
        {
            if (dataAccessJob != null)
            {
                int hoursToExpiryForJob = dataAccessJob.getDownloadMode() == CasdaDownloadMode.SIAP_SYNC
                        ? hoursToExpirySiapSync : hoursToExpiryDefault;
                dataAccessService.markRequestError(id, DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForJob));
            }
            // If there is an error, encapsulate it in an UWSException so that an error summary can be published:
            throw new UWSException(UWSException.INTERNAL_SERVER_ERROR, exception, "Unable to complete data access job.",
                    ErrorType.TRANSIENT);
        }
    }

    /**
     * Updates the logging settings to set instance id to job id
     * 
     * @param jobId
     *            the UWS job id
     */
    public void updateLoggingSettings(String jobId)
    {
        CasdaLoggingSettings loggingSettings = new CasdaLoggingSettings(DataAccessApplication.APPLICATION_NAME, null);
        // add the application name and other general logging settings
        loggingSettings.addGeneralLoggingSettings();
        // sets the instance id for log messages to the uws job id
        loggingSettings.updateLoggingInstanceId(job.getJobId());

    }
}
