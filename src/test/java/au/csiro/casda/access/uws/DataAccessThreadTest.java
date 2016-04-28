package au.csiro.casda.access.uws;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.Level;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import au.csiro.casda.access.CasdaDataAccessEvents;
import au.csiro.casda.access.Log4JTestAppender;
import au.csiro.casda.access.cache.CacheException;
import au.csiro.casda.access.cache.CacheFullException;
import au.csiro.casda.access.cache.Packager;
import au.csiro.casda.access.cache.Packager.Result;
import au.csiro.casda.access.rest.CatalogueRetrievalException;
import au.csiro.casda.access.rest.CreateChecksumException;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import uws.UWSException;
import uws.job.UWSJob;
import uws.job.parameters.UWSParameters;

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
 * Test the DataAccessThread class.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@RunWith(Parameterized.class)
public class DataAccessThreadTest
{
    @Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][] { { CasdaDownloadMode.WEB }, { CasdaDownloadMode.SIAP_SYNC },
                { CasdaDownloadMode.SIAP_ASYNC } });
    }

    private int hoursToExpiryForDownloadMode;

    private int hoursToExpiryDefault;

    private int hoursToExpirySync;

    private CasdaDownloadMode casdaDownloadMode;

    public DataAccessThreadTest(Object casdaDownloadMode) throws Exception
    {
        this.casdaDownloadMode = (CasdaDownloadMode) casdaDownloadMode;
        hoursToExpirySync = RandomUtils.nextInt(1, 10);
        hoursToExpiryDefault = RandomUtils.nextInt(11, 20);
        if (CasdaDownloadMode.SIAP_SYNC == this.casdaDownloadMode)
        {
            this.hoursToExpiryForDownloadMode = this.hoursToExpirySync;
        }
        else
        {
            this.hoursToExpiryForDownloadMode = this.hoursToExpiryDefault;
        }
    }

    @Mock
    private DataAccessService dataAccessService;

    @Mock
    private Packager packager;

    private Log4JTestAppender testAppender;

    /**
     * Set up the mocks before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        testAppender = Log4JTestAppender.createAppender();
    }

    /**
     * Test method for {@link au.csiro.casda.access.uws.DataAccessThread#jobWork()}.
     * 
     * @throws Exception
     */
    @Test
    public void testJobWork() throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.addImageCube(new ImageCube());
        String requestId = "42abc-123";
        DateTime expiryDate = DateTime.now(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);

        Result packagerResult = new Result(expiryDate, 1201000L, 5000000L);
        doReturn(packagerResult).when(packager).pack(Mockito.eq(dataAccessJob), Mockito.any(Integer.class));

        UWSParameters params = new UWSParameters();
        params.set(AccessJobManager.REQUEST_ID, requestId);
        UWSJob uwsJob = new CasdaUwsJob(params);
        DataAccessThread dataAccessThread =
                new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);

        dataAccessThread.jobWork();

        verify(dataAccessService, times(1)).getExistingJob(requestId);
        verify(packager, times(1)).pack(dataAccessJob, hoursToExpiryForDownloadMode);
        verify(dataAccessService, times(1)).markRequestCompleted(eq(requestId), any(DateTime.class));

        String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
        testAppender.verifyLogMessage(Level.INFO, commenceMessage);
        assertThat(commenceMessage, containsString("[E038] "));
        assertThat(commenceMessage, containsString(requestId));

        String completeLogMessage = CasdaDataAccessEvents.E039.messageBuilder().add(requestId).add(5000000L)
                .add(3799000).add(1201000L).toString();
        testAppender.verifyLogMessage(Level.INFO, completeLogMessage);
        assertThat(completeLogMessage, containsString("[E039] "));
        assertThat(completeLogMessage, containsString(requestId));
    }

    // Arrange a cache overflow event and make sure it is logged, and that the job is marked as ERROR
    @Test
    public void testCacheFullExceptionLoggedAndJobMarkedAsError() throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.addImageCube(new ImageCube());
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);
        doThrow(new CacheFullException()).when(packager).pack(Mockito.eq(dataAccessJob), any(Integer.class));

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped CacheFullException"); // exception is expected
        }
        catch (UWSException e)
        {
            assertEquals(CacheFullException.class, e.getCause().getClass());

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            String errorMessage = CasdaDataAccessEvents.E082.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.ERROR, errorMessage, e.getCause());

            assertThat(errorMessage, containsString("[E082] "));
            assertThat(errorMessage, containsString(requestId));

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));

        }
    }

    @Test
    public void testCacheExceptionLoggedAndJobMarkedAsError() throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.addImageCube(new ImageCube());
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);
        doThrow(new CacheException()).when(packager).pack(Mockito.eq(dataAccessJob), any(Integer.class));

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped CacheException"); // exception is expected
        }
        catch (UWSException e)
        {
            assertEquals(CacheException.class, e.getCause().getClass());

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            String errorMessage = CasdaDataAccessEvents.E147.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.ERROR, errorMessage, e.getCause());

            assertThat(errorMessage, containsString("[E147] "));
            assertThat(errorMessage, containsString(requestId));

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    // Arrange a catalogue retrieval exception event and make sure it is logged, and that the job is marked as ERROR
    @Test
    public void testCatalogueRetrievalExceptionLoggedAndJobMarkedAsError() throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.addCatalogue(new Catalogue());
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);
        doThrow(new CatalogueRetrievalException("test message")).when(packager).pack(Mockito.eq(dataAccessJob),
                any(Integer.class));

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped CatalogueRetrievalException"); // exception is expected
        }
        catch (UWSException e)
        {
            assertEquals(CatalogueRetrievalException.class, e.getCause().getClass());

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            String errorMessage = CasdaDataAccessEvents.E102.messageBuilder().add("job").add(requestId).toString();
            testAppender.verifyLogMessage(Level.ERROR, errorMessage, e.getCause());

            assertThat(errorMessage, containsString("[E102] "));
            assertThat(errorMessage, containsString("job"));
            assertThat(errorMessage, containsString(requestId));

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    // Arrange a create checksum exception event and make sure it is logged, and that the job is marked as ERROR
    @Test
    public void testCreateChecksumExceptionLoggedAndJobMarkedAsError() throws Exception
    {
        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.addImageCube(new ImageCube());
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);
        doThrow(new CreateChecksumException("test message")).when(packager).pack(Mockito.eq(dataAccessJob),
                any(Integer.class));

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped CatalogueRetrievalException"); // exception is expected
        }
        catch (UWSException e)
        {
            assertEquals(CreateChecksumException.class, e.getCause().getClass());

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            String errorMessage = CasdaDataAccessEvents.E106.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.ERROR, errorMessage, e.getCause());

            assertThat(errorMessage, containsString("[E106] "));
            assertThat(errorMessage, containsString(requestId));

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    @Test
    public void testNoMatchingDataAccessJobLogged() throws Exception
    {
        String requestId = "42";
        doThrow(new IllegalArgumentException("couldn't find job")).when(dataAccessService).getExistingJob(requestId);

        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped IllegalArgumentException"); // exception is expected
        }
        catch (UWSException e)
        {
            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            String errorMessage = CasdaLogMessageBuilderFactory
                    .getCasdaMessageBuilder(au.csiro.casda.logging.LogEvent.UNKNOWN_EVENT).toString();
            testAppender.verifyLogMessage(Level.ERROR, errorMessage, e.getCause());

            assertThat(errorMessage, containsString("[Exxx] "));

            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    @Test
    public void testNoDataProductsForAsyncMarkedAsError() throws Exception
    {
        if (this.casdaDownloadMode != CasdaDownloadMode.SIAP_ASYNC)
        {
            return;
        }

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            fail("jobWork should have failed with a wrapped IllegalStateException"); // exception is expected
        }
        catch (UWSException e)
        {
            assertEquals(IllegalStateException.class, e.getCause().getClass());
            String expectedErrMsg = "UsageError: No data products were selected for retrieval.";
            assertEquals(expectedErrMsg, e.getCause().getMessage());
            assertThat(dataAccessJob.getErrorMessage(), is(expectedErrMsg));

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            testAppender.verifyLogMessage(Level.ERROR, allOf(containsString("Exxx"),
                    containsString(requestId)), e.getCause());

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

    @Test
    public void testNoCutoutsForAsyncMarkedAsError() throws Exception
    {
        if (this.casdaDownloadMode != CasdaDownloadMode.SIAP_ASYNC)
        {
            return;
        }

        DataAccessJob dataAccessJob = new DataAccessJob();
        dataAccessJob.setDownloadMode(this.casdaDownloadMode);
        dataAccessJob.getParamMap().add(ParamKeyWhitelist.ID.name(), "secretid1");
        dataAccessJob.getParamMap().add(ParamKeyWhitelist.COORD.name(), "CIRCLE 1 1 1");
        String requestId = "42";
        stub(dataAccessService.getExistingJob(requestId)).toReturn(dataAccessJob);

        DateTime approximateErrorDateTime = null;
        try
        {
            UWSParameters params = new UWSParameters();
            params.set(AccessJobManager.REQUEST_ID, requestId);
            UWSJob uwsJob = new CasdaUwsJob(params);

            approximateErrorDateTime = new DateTime(DateTimeZone.UTC).plusHours(hoursToExpiryForDownloadMode);
            DataAccessThread dataAccessThread =
                    new DataAccessThread(uwsJob, dataAccessService, packager, hoursToExpiryDefault, hoursToExpirySync);
            dataAccessThread.jobWork();
            if (this.casdaDownloadMode == CasdaDownloadMode.SIAP_ASYNC)
            {
                fail("jobWork should have failed with a wrapped IllegalStateException"); // exception is expected
            }
        }
        catch (UWSException e)
        {
            assertEquals(IllegalStateException.class, e.getCause().getClass());
            String expectedErrMsg = "UsageError: None of the selected image cubes had data matching the "
                    + "supplied cutout parameters.";
            assertEquals(expectedErrMsg, e.getCause().getMessage());
            assertThat(dataAccessJob.getErrorMessage(), is(expectedErrMsg));

            String commenceMessage = CasdaDataAccessEvents.E038.messageBuilder().add(requestId).toString();
            testAppender.verifyLogMessage(Level.INFO, commenceMessage);

            assertThat(commenceMessage, containsString("[E038] "));
            assertThat(commenceMessage, containsString(requestId));

            testAppender.verifyLogMessage(Level.ERROR, allOf(containsString("Exxx"),
                    containsString(requestId)), e.getCause());

            ArgumentCaptor<DateTime> errorDateTimeCaptor = ArgumentCaptor.forClass(DateTime.class);

            verify(dataAccessService, times(1)).markRequestError(Mockito.eq(requestId), errorDateTimeCaptor.capture());

            DateTime errorDateTime = errorDateTimeCaptor.getValue();
            assertThat(new Period(approximateErrorDateTime, errorDateTime).toStandardSeconds().getSeconds(),
                    allOf(is(greaterThanOrEqualTo(0)), is(lessThanOrEqualTo(10))));
        }
    }

}
