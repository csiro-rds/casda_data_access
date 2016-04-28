package au.csiro.casda.access;

import java.io.IOException;
import java.util.EnumSet;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.siap2.AccessDataController;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;

/**
 * RESTful web service controller. Endpoint bundles requested files together for download.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
@RestController
public class DataAccessDownloadController
{
    private static Logger logger = LoggerFactory.getLogger(DataAccessDownloadController.class);

    /** The user id header from vo tools */
    protected static final String VO_TOOLS_HEADER_USER_ID = "X-VOTools-userId";
    /** The login system header from vo tools */
    protected static final String VO_TOOLS_HEADER_LOGIN_SYSTEM = "X-VOTools-loginSystem";

    private final DataAccessJobRepository dataAccessJobRepository;

    private final DataAccessService dataAccessService;

    /**
     * Constructor
     * 
     * @param dataAccessJobRepository
     *            a DataAccessJobRepository
     * @param dataAccessService
     *            a DataAccessService
     */
    @Autowired
    public DataAccessDownloadController(DataAccessJobRepository dataAccessJobRepository,
            DataAccessService dataAccessService)

    {
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.dataAccessService = dataAccessService;
    }

    /**
     * Download the requested file - this will redirect to view the job if the job has expired.
     * 
     * @param requestId
     *            the data access request id
     * @param filename
     *            the filename
     * @param response
     *            the response
     * 
     * @throws IOException
     *             if it can't redirect to the expired page
     * @throws BadRequestException
     *             if it can't stream the file
     * @throws ResourceNotFoundException
     *             if the requested file could not be found
     */
    @RequestMapping(value = "/requests/{requestId}/{filename:.+}", method = RequestMethod.GET, produces = {
            MediaType.APPLICATION_OCTET_STREAM_VALUE, MediaType.TEXT_PLAIN_VALUE, "text/csv", "application/x-tar" })
    public void downloadFileWeb(@PathVariable() String requestId, @PathVariable() String filename,
            HttpServletResponse response) throws IOException, BadRequestException, ResourceNotFoundException
    {
        downloadFile(requestId, filename, response, EnumSet.of(CasdaDownloadMode.WEB, CasdaDownloadMode.SIAP_ASYNC));
    }

    /**
     * Download the requested file - within Pawsey only. This will redirect to view the job if the job has expired.
     * 
     * @param requestId
     *            the data access request id
     * @param filename
     *            the filename
     * @param response
     *            the response
     * 
     * @throws IOException
     *             if it can't redirect to the expired page
     * @throws BadRequestException
     *             if it can't stream the file
     * @throws ResourceNotFoundException
     *             if the requested file could not be found
     */
    @RequestMapping(value = "/pawsey/requests/{requestId}/{filename:.+}", method = RequestMethod.GET, produces = {
            MediaType.APPLICATION_OCTET_STREAM_VALUE, MediaType.TEXT_PLAIN_VALUE, "text/csv", "application/x-tar" })
    public void downloadFilePawsey(@PathVariable() String requestId, @PathVariable() String filename,
            HttpServletResponse response) throws IOException, BadRequestException, ResourceNotFoundException
    {
        downloadFile(requestId, filename, response, EnumSet.of(CasdaDownloadMode.PAWSEY_HTTP));
    }

    private void downloadFile(String requestId, String filename, HttpServletResponse response,
            EnumSet<CasdaDownloadMode> permittedDownloadModes) throws ResourceNotFoundException, IOException
    {
        DataAccessJob dataAccessJob = dataAccessJobRepository.findByRequestId(requestId);

        logger.info("{}", CasdaDataAccessEvents.E040.messageBuilder().add(requestId).add(filename));

        if (dataAccessJob == null || !permittedDownloadModes.contains(dataAccessJob.getDownloadMode()))
        {
            throw new ResourceNotFoundException("No valid request found with request id " + requestId);
        }

        if (dataAccessJob.isExpired())
        {
            switch (dataAccessJob.getDownloadMode())
            {
            case WEB:
            case PAWSEY_HTTP:
                response.sendRedirect("/requests/" + dataAccessJob.getRequestId());
                break;
            case SIAP_ASYNC:
                response.sendRedirect(
                        AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + dataAccessJob.getRequestId());
                break;
            case SIAP_SYNC:
            default:
                throw new BadRequestException("Unsupported download mode " + dataAccessJob.getDownloadMode());
            }
            return;
        }

        this.dataAccessService.downloadFile(dataAccessJob, filename, response, false);
    }

}
