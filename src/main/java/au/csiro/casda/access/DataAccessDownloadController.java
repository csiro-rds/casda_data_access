package au.csiro.casda.access;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import au.csiro.casda.access.jpa.DataAccessJobRepository;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.soda.AccessDataController;
import au.csiro.casda.access.soda.RequestToken;
import au.csiro.casda.access.util.Utils;
import au.csiro.casda.access.uws.AccessJobManager;
import au.csiro.casda.access.uws.AccessJobManager.ScheduleJobException;
import au.csiro.casda.entity.dataaccess.CasdaDownloadMode;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.logging.CasdaLogMessageBuilderFactory;
import au.csiro.casda.logging.CasdaMessageBuilder;
import au.csiro.casda.logging.LogEvent;

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
    /** Constant for a preview link type */
    protected static final String LINK_TYPE_PREVIEW = "preview";
    /** Constant for a moc link type */
    protected static final String LINK_TYPE_MOC = "moc";
    /** Constant for a hips link type */
    protected static final String LINK_TYPE_HIPS = "hips";
    private static final String PLACEHOLDER_HIPS_PATH = "<hips_path>";
    private static final String PLACEHOLDER_PROJECT = "<project>";
    private static final String PLACEHOLDER_FILE = "<file>";

    private final DataAccessJobRepository dataAccessJobRepository;

    private final DataAccessService dataAccessService;

    private AccessJobManager accessJobManager;
    
    private final String  projectCoverageUrl;
    private final String  projectCoverageDir;
    private final String  projectCoverageMocFile;
    private final String  projectCoveragePreviewFile;
    private final String catalogueHipsUrl;
    private final String catalogueHipsDir;
    private final String dataLinkAccessSecretKey;

	/**
	 * Constructor
	 * @param dataAccessJobRepository the data access job repository
	 * @param dataAccessService the data access service
     * @param accessJobManager the manager of data access jobs
	 * @param projectCoverageUrl the url template for coverage artefacts links
	 * @param projectCoverageDir the physical directory path for coverage artefacts
	 * @param projectCoverageMocFile the standard moc filename
	 * @param projectCoveragePreviewFile the standard preview filename
     * @param catalogueHipsUrl access url for HiPS Catalogue.
     * @param catalogueHipsDir phsyical server location of HiPS catalogue data
     * @param dataLinkAccessSecretKey
     *            the AES secret key used to decrypt an authorised ID token
	 */
    @Autowired
    public DataAccessDownloadController(DataAccessJobRepository dataAccessJobRepository,
            DataAccessService dataAccessService, AccessJobManager accessJobManager,
            @Value("${project.coverage.url}") String projectCoverageUrl,
            @Value("${project.coverage.dir}") String projectCoverageDir,
            @Value("${project.coverage.moc.file}") String projectCoverageMocFile, 
            @Value("${project.coverage.preview.file}") String projectCoveragePreviewFile, 
            @Value("${catalogue.hips.url}") String catalogueHipsUrl,
            @Value("${catalogue.hips.dir}") String catalogueHipsDir,
            @Value("${siap.shared.secret.key}") String dataLinkAccessSecretKey)

    {
        this.dataAccessJobRepository = dataAccessJobRepository;
        this.dataAccessService = dataAccessService;
        this.accessJobManager = accessJobManager;
        this.projectCoverageUrl = projectCoverageUrl;
        this.projectCoverageDir = projectCoverageDir;
        this.projectCoverageMocFile = projectCoverageMocFile;
        this.projectCoveragePreviewFile = projectCoveragePreviewFile;
        this.catalogueHipsUrl = catalogueHipsUrl;
        this.catalogueHipsDir = catalogueHipsDir;
        this.dataLinkAccessSecretKey = dataLinkAccessSecretKey;
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
        downloadFile(requestId, filename, response, Utils.WEB_DOWNLOADS);
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
        downloadFile(requestId, filename, response, Utils.PAWSEY_DOWNLOADS);
    }
    
    /**
     * Download the requested thumbnail - stream it directly to the requester.
     * 
     * @param fileId
     *            the NGAS file id of the thumbnail
     * @param response
     *            the response
     * @throws IOException
     *             if it can't redirect to the expired page
     * @throws BadRequestException
     *             if it can't stream the file
     * @throws ResourceNotFoundException
     *             if the requested file could not be found
     */
    @RequestMapping(value = "/pawsey/thumbnail/{fileId:.+}", method = RequestMethod.GET)
    public void downloadThumbnailPawsey(@PathVariable() String fileId, 
            HttpServletResponse response) throws IOException, BadRequestException, ResourceNotFoundException
    {
        if (StringUtils.isBlank(fileId))
        {
            response.sendError(HttpStatus.SC_NOT_FOUND);
        }
        if (!fileId.contains("-thumbnail-"))
        {
            response.sendError(HttpStatus.SC_FORBIDDEN);
        }
        dataAccessService.downloadThumbnailFromNgas(fileId, response);
    }

    /**
     * Download an undeposited level 7 thumbnail for approval screen
     * @param dapCollectionId the dap collection id
     * @param filename the filename including path to file from this particular L7 collection's base directory
     * @return the byte array of the thumbnail
     * @throws IOException if the file cannot be read
     * @throws BadRequestException if it can't stream the file
     * @throws ResourceNotFoundException if the requested file could not be found
     */
    @RequestMapping(value = "/pawsey/thumbnail/preapproval/{dapCollectionId}", 
    		method = RequestMethod.GET, produces = {MediaType.IMAGE_PNG_VALUE, MediaType.IMAGE_JPEG_VALUE})
    public byte[] downloadUndepositedThumbnailPawsey(@PathVariable() String dapCollectionId, 
    		@RequestParam String filename) throws IOException, BadRequestException, ResourceNotFoundException
    {
    	return dataAccessService.retrieveUndepositedThumbnail(dapCollectionId, filename);
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
            case SODA_ASYNC_WEB:
            case SODA_ASYNC_PAWSEY:
                response.sendRedirect(
                        AccessDataController.ACCESS_DATA_ASYNC_BASE_PATH + "/" + dataAccessJob.getRequestId());
                break;
            case SODA_SYNC_WEB:
            case SODA_SYNC_PAWSEY:
            default:
                throw new BadRequestException("Unsupported download mode " + dataAccessJob.getDownloadMode());
            }
            return;
        }

        this.dataAccessService.downloadFile(dataAccessJob, filename, response, false, false);
    }
    
    /**
     * @param projectCode the opal code of the project to search for
     * @param linkType the type of link to return, e.g. preview image, moc or hips directory
     * @return the link pointing to the desired content
     * @throws BadRequestException if the linktype or project code is invalid
     * @throws ResourceNotFoundException if the project exists but the generated content does not exist yet.
     */
    @RequestMapping(value = "/coverage/{projectCode}/{linkType}", method = RequestMethod.GET, 
    		produces = MediaType.TEXT_PLAIN_VALUE)
    public @ResponseBody String getCoverageLink(@PathVariable("projectCode") String projectCode,
    		@PathVariable("linkType") String linkType) throws BadRequestException, ResourceNotFoundException
    {
    	if(!dataAccessService.isProjectExist(projectCode))
    	{
    		throw new BadRequestException(String.format("Unrecognised project code : %s", projectCode));
    	}
    	String link = projectCoverageUrl.replace(PLACEHOLDER_PROJECT, projectCode);
    	switch(linkType)
    	{
    		case LINK_TYPE_PREVIEW:
    			link = link.replace(PLACEHOLDER_FILE, projectCoveragePreviewFile);
    			break;
    		case LINK_TYPE_MOC:
    			link = link.replace(PLACEHOLDER_FILE, projectCoverageMocFile);
    			break;
    		case LINK_TYPE_HIPS:
    			link = link.replace(PLACEHOLDER_FILE, "");//no filename wanted here. just the link to the directory
    			break;
    		default:
    			throw new BadRequestException(String.format("Unrecognised link type : %s", linkType));
    	}
    	
    	if(!Files.exists(Paths.get(projectCoverageDir, projectCode)))
    	{
    		throw new ResourceNotFoundException(
    				String.format("Project %s currently does not have generated coverage maps", projectCode));
    	}
    	return link;
    } 
    
    /**
     * Generate url that points to a HiPS catalogue.
     * 
     * @param hipsPath
     *            Path to HiPS catalogue folder. This is the folder that contains the HiPS catalogue data.
     * @return HiPS catalogue url.
     * @throws ResourceNotFoundException
     *             if the path to the HiPS Catalogue does not exist.
     */
    @RequestMapping(value = "/catalogue/hips/{hipsPath}", method = RequestMethod.GET,
            produces = MediaType.TEXT_PLAIN_VALUE)
    public @ResponseBody String getHipsCatalogueLink(@PathVariable("hipsPath") String hipsPath)
            throws ResourceNotFoundException
    {
        String link = catalogueHipsUrl.replace(PLACEHOLDER_HIPS_PATH, hipsPath);

        if (!Files.exists(Paths.get(catalogueHipsDir, hipsPath)))
        {
            throw new ResourceNotFoundException(
                    String.format("Generated HiPS catalogue for path %s does not exist.", hipsPath));
        }
        return link;
    }
    
    
    
    /**
     * Download a square preview image of the catalogue entry.
     * 
     * @param imageCubeId
     *            The id of the image cube that is being previewed.
     * @param ra
     *            The right ascension of the central point of the preview.
     * @param dec
     *            The declination of the central point of the preview.
     * @param radius
     *            The distance from centre to either edge of the preview cutout
     * @param response
     *            The http response object, used to send back the response code and data.
     * @throws IOException
     *             If a problem occurs sending the response back.
     */
    @RequestMapping(value = "preview/image/{id}", method = RequestMethod.GET)
    public void downloadCatalogueObjectPreview(@PathVariable("id") long imageCubeId, @RequestParam("ra") double ra,
            @RequestParam("dec") double dec, @RequestParam("radius") String radius, HttpServletResponse response)
            throws IOException
    {
        double radiusDbl = Double.parseDouble(radius);
        // Get existing preview 
        if (!dataAccessService.downloadCutoutPreview(imageCubeId, ra, dec, radiusDbl, response))
        {
            // No existing preview, so build and start a job to produce it
            RequestToken idToken =
                    new RequestToken("cube-" + imageCubeId, "Anon", "None", new Date(), dataLinkAccessSecretKey);
            idToken.setDownloadMode(RequestToken.CUTOUT);
            Map<String, String[]> params = new HashMap<>();
            params.put(ParamMap.ParamKeyWhitelist.ID.toString(), new String[] { idToken.toEncryptedString() });
            params.put(ParamMap.ParamKeyWhitelist.FORMAT.toString(), new String[] { "png" });
            params.put(ParamMap.ParamKeyWhitelist.CIRCLE.toString(),
                    new String[] { String.format("%f %f %s", ra, dec, radius) });
            JobDto jobDetails = new JobDto();
            jobDetails.setIds(new String[] { "cube-" + imageCubeId });
            jobDetails.setDownloadMode(CasdaDownloadMode.SODA_SYNC_WEB);
            jobDetails.setDownloadFormat("png");
            jobDetails.setParams(params);
            jobDetails.setJobType(RequestToken.CUTOUT);
            DataAccessJob dataAccessJob = accessJobManager.createDataAccessJob(jobDetails, null, true);
            if (dataAccessJob.getImageCutouts().isEmpty())
            {
                dataAccessJobRepository.delete(dataAccessJob);
                response.sendError(HttpStatus.SC_NOT_FOUND);
                return;
            }
            try
            {
                accessJobManager.scheduleJob(dataAccessJob.getRequestId());
            }
            catch (ResourceNotFoundException | ResourceIllegalStateException | ScheduleJobException e)
            {
                // suppress exception that this couldn't be scheduled because the job is created and we assume the user
                // or admin will follow it up.
                CasdaMessageBuilder<?> builder =
                        CasdaLogMessageBuilderFactory.getCasdaMessageBuilder(LogEvent.UNKNOWN_EVENT);
                builder.add("Unable to schedule job " + dataAccessJob);
                logger.error(builder.toString(), e);
            }
            response.sendError(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
