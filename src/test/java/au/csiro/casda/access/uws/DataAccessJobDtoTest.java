package au.csiro.casda.access.uws;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import uws.job.ExecutionPhase;
import uws.job.Result;
import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;
import uws.job.user.DefaultJobOwner;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
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
 * Test the functionality of the DataAccessJobDto class.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
public class DataAccessJobDtoTest
{

    @Test
    public void testConstructor() throws Exception
    {
        DateTime availableTime = DateTime.now().minusDays(1);
        DateTime startTime = DateTime.now().minusHours(36);
        DateTime createdTime = DateTime.now().minusDays(3);
        DateTime expiredTime = DateTime.now().plusDays(2);

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setAvailableTimestamp(availableTime);
        dataAccessJob.setCreatedTimestamp(createdTime);
        dataAccessJob.setExpiredTimestamp(expiredTime);
        dataAccessJob.setDownloadFormat("downloadFormat");
        dataAccessJob.setDownloadMode(CasdaDownloadMode.WEB);
        dataAccessJob.setId(12L);
        dataAccessJob.setRequestId("abc-123");
        dataAccessJob.setStatus(DataAccessJobStatus.READY);
        dataAccessJob.setUserEmail("userEmail");
        dataAccessJob.setUserIdent("userIdent");
        dataAccessJob.setUserLoginSystem("userLoginSystem");
        dataAccessJob.setUserName("userName");

        UWSJob uwsJob = new CasdaUwsJob(new UWSParameters());
        uwsJob.setPhase(ExecutionPhase.EXECUTING);

        DataAccessJobDto dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
        assertEquals("WEB", dto.getAccessMethod());
        assertEquals(availableTime.getMillis(), dto.getDateCompleted().getMillis());
        assertEquals(createdTime.getMillis(), dto.getDateRequested().getMillis());
        assertEquals(null, dto.getDateStarted());
        assertEquals("userEmail", dto.getEmail());
        assertEquals("downloadFormat", dto.getFormat());
        assertEquals("queuename", dto.getQueue());
        assertEquals("userName", dto.getUser());
        assertEquals(uwsJob.getJobId(), dto.getUwsJobId());
        assertEquals(0, dto.getExecutionDuration());
        assertEquals("READY", dto.getStatus());

        uwsJob =
                new UWSJob("abc-123", new DefaultJobOwner("id", "pseudo"), new UWSParameters(), 1,
                        startTime.getMillis(), availableTime.getMillis(), new ArrayList<Result>(), null);

        dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
        assertEquals(uwsJob.getJobId(), dto.getUwsJobId());
        assertEquals("abc-123", dto.getRequestId());
        assertEquals(startTime.toDateTime(DateTimeZone.UTC), dto.getDateStarted().toDateTime(DateTimeZone.UTC));
        assertEquals(availableTime.toDateTime(DateTimeZone.UTC), dto.getDateCompleted().toDateTime(DateTimeZone.UTC));
        assertEquals(43199L, dto.getExecutionDuration(), 1);
        assertEquals(availableTime.toDateTime(DateTimeZone.UTC).toString(), dto.getFormattedDateCompleted());
        assertEquals(startTime.toDateTime(DateTimeZone.UTC).toString(), dto.getFormattedDateStarted());
        assertEquals(createdTime.toDateTime(DateTimeZone.UTC).toString(), dto.getFormattedDateRequested());

        dataAccessJob.setUserName(null);
        dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
        assertEquals("userIdent", dto.getUser());

        dataAccessJob.setUserIdent(null);
        dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
        assertEquals("ANONYMOUS", dto.getUser());

        dataAccessJob.setExpiredTimestamp(DateTime.now().minusDays(1));
        dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
        assertEquals("EXPIRED", dto.getStatus());

        List<ExecutionPhase> maskedPhases =
                Arrays.asList(ExecutionPhase.ABORTED, ExecutionPhase.COMPLETED, ExecutionPhase.ERROR,
                        ExecutionPhase.HELD, ExecutionPhase.SUSPENDED, ExecutionPhase.UNKNOWN);
        dataAccessJob.setStatus(DataAccessJobStatus.PAUSED);
        for (ExecutionPhase phase : ExecutionPhase.values())
        {
            uwsJob.setPhase(phase, true);
            dto = new DataAccessJobDto(dataAccessJob, uwsJob, "queuename");
            if (maskedPhases.contains(phase))
            {
                assertEquals("PAUSED", dto.getUwsStatus());
            }
            else
            {
                assertEquals(phase.name(), dto.getUwsStatus());
            }
        }

    }

    /**
     * Test method for
     * {@link au.csiro.casda.access.uws.DataAccessJobDto#compareTo(au.csiro.casda.access.uws.DataAccessJobDto)}. Verify
     * phases are ordered correctly.
     * 
     * @throws Exception
     *             if the uws job can't be created
     */
    @Test
    public void testCompareToPhaseOrder() throws Exception
    {
        DataAccessJobDto executing = createDataAccessJob(ExecutionPhase.EXECUTING, "test", null, "abc-123-111");
        DataAccessJobDto queued = createDataAccessJob(ExecutionPhase.QUEUED, "test", null, "abc-123-222");
        DataAccessJobDto completed = createDataAccessJob(ExecutionPhase.COMPLETED, "test", null, "abc-123-333");

        assertEquals(-1, executing.compareTo(queued));
        assertEquals(-1, executing.compareTo(completed));
        assertEquals(1, queued.compareTo(executing));
        assertEquals(-1, queued.compareTo(completed));
        assertEquals(1, completed.compareTo(executing));
        assertEquals(1, completed.compareTo(queued));
    }

    /**
     * Test method for
     * {@link au.csiro.casda.access.uws.DataAccessJobDto#compareTo(au.csiro.casda.access.uws.DataAccessJobDto)}. Verify
     * queues are ordered correctly when phases are the same.
     * 
     * @throws Exception
     *             if the uws job can't be created
     */
    @Test
    public void testCompareToQueueOrder() throws Exception
    {
        DataAccessJobDto slow = createDataAccessJob(ExecutionPhase.EXECUTING, "slow", null, "abc-123-111");
        DataAccessJobDto fast = createDataAccessJob(ExecutionPhase.EXECUTING, "fast", null, "abc-123-222");

        assertThat(slow.compareTo(fast), is(greaterThan(0)));
        assertThat(fast.compareTo(slow), is(lessThan(0)));
    }

    /**
     * Test method for
     * {@link au.csiro.casda.access.uws.DataAccessJobDto#compareTo(au.csiro.casda.access.uws.DataAccessJobDto)}. Verify
     * that completed jobs in the same queue are sorted in descending date order.
     * 
     * @throws Exception
     *             if the uws job can't be created
     */
    @Test
    public void testCompareToDateOrderComplete() throws Exception
    {
        DataAccessJobDto earlyComplete =
                createDataAccessJob(ExecutionPhase.COMPLETED, "test", "2015-01-19T13:15:05Z", "abc-123-111");
        DataAccessJobDto lateComplete =
                createDataAccessJob(ExecutionPhase.COMPLETED, "test", "2015-01-19T13:15:06Z", "abc-123-222");

        assertThat(earlyComplete.compareTo(lateComplete), is(greaterThan(0)));
        assertThat(lateComplete.compareTo(earlyComplete), is(lessThan(0)));
    }

    /**
     * Test method for
     * {@link au.csiro.casda.access.uws.DataAccessJobDto#compareTo(au.csiro.casda.access.uws.DataAccessJobDto)}. Verify
     * that queued jobs in the same queue are sorted in ascending date order.
     * 
     * @throws Exception
     *             if the uws job can't be created
     */
    @Test
    public void testCompareToDateOrderQueued() throws Exception
    {
        DataAccessJobDto earlyComplete =
                createDataAccessJob(ExecutionPhase.QUEUED, "test", "2015-01-19T12:59:59Z", "abc-123-111");
        DataAccessJobDto lateComplete =
                createDataAccessJob(ExecutionPhase.QUEUED, "test", "2015-01-19T13:00:00Z", "abc-123-222");

        assertThat(earlyComplete.compareTo(lateComplete), is(lessThan(0)));
        assertThat(lateComplete.compareTo(earlyComplete), is(greaterThan(0)));
    }

    private DateTime getDate(String string) throws Exception
    {
        return ISODateTimeFormat.dateTimeNoMillis().withZoneUTC().parseDateTime(string);
    }

    private DataAccessJobDto createDataAccessJob(ExecutionPhase phase, String queue, String dateRequested,
            String requestId) throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setRequestId(requestId);
        if (dateRequested != null)
        {
            dataAccessJob.setCreatedTimestamp(getDate(dateRequested));
        }
        UWSJob uwsJob = new CasdaUwsJob(new UWSParameters());
        uwsJob.setPhase(phase, true);

        DataAccessJobDto jobDto = new DataAccessJobDto(dataAccessJob, uwsJob, queue);

        return jobDto;
    }

}
