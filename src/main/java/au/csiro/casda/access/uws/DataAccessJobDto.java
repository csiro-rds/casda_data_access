package au.csiro.casda.access.uws;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import uws.job.ExecutionPhase;
import uws.job.UWSJob;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;

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
 * A data transfer object for use when transferring data access jobs for display.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class DataAccessJobDto implements Comparable<DataAccessJobDto>
{
    /** The order in which we want the phases to be sorted for display. */
    private static final List<ExecutionPhase> PHASE_ORDER = Arrays.asList(new ExecutionPhase[] {
            ExecutionPhase.EXECUTING, ExecutionPhase.PENDING, ExecutionPhase.HELD, ExecutionPhase.SUSPENDED,
            ExecutionPhase.QUEUED, ExecutionPhase.COMPLETED, ExecutionPhase.ABORTED, ExecutionPhase.ERROR,
            ExecutionPhase.UNKNOWN });

    /** The phases which should be in sorted ascending order of request date for display. */
    private static final EnumSet<ExecutionPhase> ASCENDING_DATE_SORTED_PHASES = EnumSet.of(ExecutionPhase.EXECUTING,
            ExecutionPhase.PENDING, ExecutionPhase.HELD, ExecutionPhase.SUSPENDED, ExecutionPhase.QUEUED);

    private String queue;
    private final DataAccessJob dataAccessJob;
    private UWSJob uwsJob;

    /**
     * Constructor
     * 
     * @param dataAccessJob
     *            details of the data access job from the database record
     * @param uwsJob
     *            details of the uws job for this data access job
     * @param queue
     *            the queue the job is being run on
     */
    public DataAccessJobDto(DataAccessJob dataAccessJob, UWSJob uwsJob, String queue)
    {
        this.dataAccessJob = dataAccessJob;
        this.uwsJob = uwsJob;
        this.queue = queue == null ? "UNKNOWN" : queue;
    }

    /**
     * Gets the data access job request id
     * 
     * @return the data access job request id
     */
    public String getRequestId()
    {
        return this.dataAccessJob.getRequestId();
    }

    /**
     * Gets the data access job requester's name
     * 
     * @return the requester's name
     */
    public String getUser()
    {
        String user = this.dataAccessJob.getUserName();
        if (StringUtils.isBlank(user))
        {
            user = this.dataAccessJob.getUserIdent();
        }
        if (StringUtils.isBlank(user))
        {
            user = "ANONYMOUS";
        }
        return user;
    }

    public String getQueue()
    {
        return queue;
    }

    /**
     * Gets the status of the job in the UWS queue
     * 
     * @return the status of the job in the UWS queue
     */
    public String getUwsStatus()
    {
        if (this.getPhase() != null)
        {
            // display PAUSED if the job is paused and not running, queued or pending, otherwise display the uws status
            if (this.dataAccessJob.getStatus() == DataAccessJobStatus.PAUSED
                    && this.getPhase() != ExecutionPhase.PENDING && this.getPhase() != ExecutionPhase.QUEUED
                    && this.getPhase() != ExecutionPhase.EXECUTING)
            {
                return this.dataAccessJob.getStatus().name();
            }
            return this.getPhase().name();
        }
        else
        {
            return "Not Queued";
        }
    }
    
    /**
     * Gets the status of the job, according to the database.
     * 
     * @return the job status
     */
    public String getStatus()
    {
        if (this.dataAccessJob.isExpired())
        {
            return DataAccessJobStatus.EXPIRED.name();
        }
        else
        {
            return this.dataAccessJob.getStatus().name();
        }
    }

    public DateTime getDateRequested()
    {
        return dataAccessJob.getCreatedTimestamp();
    }

    /**
     * @return The requested date formatted in ISO 8601 format
     */
    public String getFormattedDateRequested()
    {
        return ISODateTimeFormat.dateTime().withZoneUTC().print(getDateRequested());
    }

    /**
     * Gets the date the job started running, if there is a record of the job in UWS.
     * 
     * @return the date the job started running
     */
    public DateTime getDateStarted()
    {
        if (uwsJob != null && uwsJob.getStartTime() != null)
        {
            return new DateTime(uwsJob.getStartTime().getTime(), DateTimeZone.UTC);
        }
        else
        {
            return null;
        }
    }

    /**
     * @return The start date formatted in ISO 8601 format
     */
    public String getFormattedDateStarted()
    {
        DateTime started = getDateStarted();
        if (started != null)
        {
            return ISODateTimeFormat.dateTime().withZoneUTC().print(getDateStarted());
        }
        else
        {
            return "NOT STARTED";
        }
    }
    
    /**
     * Gets the date the job failed - this will be the last modified date.
     * 
     * @return the date the job failed
     */
    public DateTime getDateFailed()
    {
        return dataAccessJob.getLastModified();
    }
    
    /**
     * @return The failed date (last updated date) formatted in ISO 8601 format
     */
    public String getFormattedDateFailed()
    {
        return ISODateTimeFormat.dateTime().withZoneUTC().print(getDateFailed());
    }

    public DateTime getDateCompleted()
    {
        return dataAccessJob.getAvailableTimestamp();
    }

    /**
     * @return The completed date formatted in ISO 8601 format
     */
    public String getFormattedDateCompleted()
    {
        return ISODateTimeFormat.dateTime().withZoneUTC().print(getDateCompleted());
    }

    private ExecutionPhase getPhase()
    {
        return uwsJob == null ? null : uwsJob.getPhase();
    }

    public String getUwsJobId()
    {
        return uwsJob == null ? null : uwsJob.getJobId();
    }

    /**
     * @return The number of seconds the job took to run.
     */
    public long getExecutionDuration()
    {
        if (this.getDateStarted() == null || this.getDateCompleted() == null)
        {
            return 0;
        }
        return TimeUnit.MILLISECONDS.toSeconds(this.getDateCompleted().getMillis() - this.getDateStarted().getMillis());
    }

    public String getEmail()
    {
        return StringUtils.defaultString(dataAccessJob.getUserEmail(), "UNKNOWN");
    }

    public String getAccessMethod()
    {
        return dataAccessJob.getDownloadMode().name();
    }

    public String getFormat()
    {
        return StringUtils.defaultString(dataAccessJob.getDownloadFormat(), "N/A");
    }

    @Override
    public int compareTo(DataAccessJobDto o)
    {
        if (this == o)
        {
            return 0;
        }
        if (this.getPhase() != o.getPhase())
        {
            Integer myPhaseOrder = PHASE_ORDER.indexOf(this.getPhase());
            Integer otherPhaseOrder = PHASE_ORDER.indexOf(o.getPhase());
            return myPhaseOrder.compareTo(otherPhaseOrder);
        }
        if (!this.getQueue().equals(o.getQueue()))
        {
            return this.getQueue().compareTo(o.getQueue());
        }
        // Date requested in ascending order for active jobs, otherwise descending
        if (ASCENDING_DATE_SORTED_PHASES.contains(this.getPhase()))
        {
            return this.getDateRequested().compareTo(o.getDateRequested());
        }
        return -1 * this.getDateRequested().compareTo(o.getDateRequested());
    }

}
