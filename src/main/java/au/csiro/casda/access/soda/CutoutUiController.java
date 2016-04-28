package au.csiro.casda.access.soda;

import java.net.URI;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestClientException;
import org.springframework.web.servlet.ModelAndView;

import au.csiro.casda.access.BadRequestException;
import au.csiro.casda.access.DataAccessDataProduct;
import au.csiro.casda.access.DataAccessDataProduct.DataAccessProductType;
import au.csiro.casda.access.ResourceNotFoundException;
import au.csiro.casda.access.security.SecuredRestTemplate;
import au.csiro.casda.access.services.DataAccessService;
import au.csiro.casda.access.siap2.RequestToken;
import au.csiro.casda.entity.observation.ImageCube;

/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2010 - 2015 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * Controller for the cutout form.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Controller
public class CutoutUiController
{

    /** The request type parameter */
    protected static final String ID_PARAM = "id";

    private final String dataLinkAccessSecretKey;

    private String applicationBaseUrl;

    private DataAccessService dataAccessService;

    private SecuredRestTemplate restTemplate;

    /**
     * Create a new CutoutUiController instance.
     * 
     * @param dataAccessService
     *            The service instance for accessing the data.
     * @param applicationBaseUrl
     *            the application base URL
     * @param dataLinkAccessSecretKey
     *            the AES secret key used to decrypt an authorised ID token
     */
    @Autowired
    public CutoutUiController(DataAccessService dataAccessService,
            @Value("${application.base.url}") String applicationBaseUrl,
            @Value("${siap.shared.secret.key}") String dataLinkAccessSecretKey)
    {
        this.dataAccessService = dataAccessService;
        this.applicationBaseUrl = applicationBaseUrl;
        this.dataLinkAccessSecretKey = dataLinkAccessSecretKey;
        restTemplate = new SecuredRestTemplate("", "", false);
    }

    /**
     * Prepare and display a cutout form prepopulated with the information about the target image cube.
     * 
     * @param authenticatedIdToken
     *            The encrypted authorised id token.
     * @return A ModelAndView prepared for displaying the form.
     * @throws ResourceNotFoundException
     *             if the original image could not be found
     */
    @RequestMapping(value = "/cutoutui", method = RequestMethod.GET, produces = "text/html")
    public ModelAndView displayCutoutForm(@RequestParam(value = ID_PARAM, required = true) String authenticatedIdToken)
            throws ResourceNotFoundException
    {
        ModelAndView result = new ModelAndView("cutout");

        return buildModelForImage(authenticatedIdToken, result);
    }

    private ModelAndView buildModelForImage(String authenticatedIdToken, ModelAndView result)
            throws ResourceNotFoundException
    {
        // Decode selected id
        DataAccessDataProduct dataAccessDataProduct;
        RequestToken token;
        try
        {
            token = new RequestToken(authenticatedIdToken, dataLinkAccessSecretKey);
        }
        catch (IllegalArgumentException ex)
        {
            throw new BadRequestException("id '" + authenticatedIdToken + "' is not valid");
            //token = new RequestToken("cube-474", "42", "opal", new Date(), dataLinkAccessSecretKey);
            //authenticatedIdToken = token.toEncryptedString();
        }
        dataAccessDataProduct = new DataAccessDataProduct(token.getId());

        if (DataAccessProductType.cube != dataAccessDataProduct.getDataAccessProductType())
        {
            throw new BadRequestException(
                    "Unsupported data access product type " + dataAccessDataProduct.getDataAccessProductType());
        }

        // Retrieve object
        ImageCube dataProduct = (ImageCube) dataAccessService.findDataProduct(dataAccessDataProduct);
        if (dataProduct == null)
        {
            throw new ResourceNotFoundException("No resource found matching id '" + authenticatedIdToken + "'");
        }

        // Populate data
        result.addObject("id", authenticatedIdToken);
        result.addObject("sbid", dataProduct.getParent().getSbid());
        result.addObject("imageName", dataProduct.getFilename());
        result.addObject("centreRa", dataProduct.getRaDeg());
        result.addObject("centreDec", dataProduct.getDecDeg());

        return result;
    }

    /**
     * Process a form submission, requesting the production of a cutout from the image cube.
     * 
     * @param request
     *            The web request containing the cutout criteria.
     * @return A ModelAndView redirecting the browser to the request page.
     * @throws ResourceNotFoundException
     *             if the original image could not be found
     */
    @RequestMapping(value = "/cutoutui", method = RequestMethod.POST)
    public ModelAndView processCutoutForm(HttpServletRequest request) throws ResourceNotFoundException
    {
        // Extract the cutout criteria
        String id = request.getParameter("ID");
        String ra = request.getParameter("ra");
        String dec = request.getParameter("dec");
        String radius = request.getParameter("radius");

        // Build async uri with a POS query
        //String queryParams = String.format("ID=%s&POS=CIRCLE %s %s %s&BAND=1e-9 2", id, ra, dec, radius);
        String queryParams = String.format("ID=%s&POS=CIRCLE %s %s %s", id, ra, dec, radius);
        String url = applicationBaseUrl + "/data/async?" + queryParams;

        // Create an async cutout job
        String requestId;
        URI location;
        try
        {
            location = restTemplate.postForLocation(url, null);
            String[] pathSegments = location.getPath().split("/");
            requestId = pathSegments[pathSegments.length - 1];
        }
        catch (RestClientException e)
        {
            ModelAndView mav = new ModelAndView("cutout");
            mav.addObject("error", "Invalid parameters provided");
            return buildModelForImage(id, mav);
        }

        // Start the job
        String phaseRunUri = location.toString() + "/phase?phase=RUN";
        restTemplate.postForEntity(phaseRunUri, null, String.class);

        String requestPageUrl = applicationBaseUrl + "/requests/" + requestId;
        return new ModelAndView("redirect:" + requestPageUrl);
    }
}
