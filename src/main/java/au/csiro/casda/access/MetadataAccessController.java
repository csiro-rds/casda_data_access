package au.csiro.casda.access;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.ContinuumComponentRepository;
import au.csiro.casda.access.jpa.ValidationNoteRepository;

/**
 * RESTful web service controller. Endpoint fopr queries to database. used mainly for DAP ui metadata requests
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 *
 */
@RestController
public class MetadataAccessController
{
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");
    
    private static Logger logger = LoggerFactory.getLogger(MetadataAccessController.class);
    
    private ValidationNoteRepository validationNoteRepository;
    private ContinuumComponentRepository continuumComponentRepository;
    private DataAccessJdbcRepository dataAccessJdbcRepository;
    
    /**
     * Constructor
     * @param validationNoteRepository the validation notice repository
     * @param continuumComponentRepository the continuum Component Repository
     * @param dataAccessJdbcRepository the dataAccessJdbcRepository for straight sql queries
     */
    @Autowired
    public MetadataAccessController(ValidationNoteRepository validationNoteRepository, 
    		ContinuumComponentRepository continuumComponentRepository, 
    		DataAccessJdbcRepository dataAccessJdbcRepository)
    {
        this.validationNoteRepository = validationNoteRepository;
        this.continuumComponentRepository = continuumComponentRepository;
        this.dataAccessJdbcRepository = dataAccessJdbcRepository;
    }
   
    /**
     * @param sbids an array of sbids to query the database for
     * @param project the opal code of the project to query the database for
     * @return the validation notes, in a map organised by sbid
     */
    @RequestMapping(value = "/metadata/validationNotes", method = RequestMethod.GET, 
    		produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<Integer, List<Map<String, Object>>> getValidationNotes(
    		@RequestParam("sbids") Integer[] sbids, @RequestParam("project") String project)
    {
    	logger.info("Validation notes requested for SBIDs: {}", Arrays.toString(sbids));
    	List<Map<String, Object>> notes = validationNoteRepository.getValidationNotesForSbids(sbids, project);
    	Map<Integer, List<Map<String, Object>>> map = new LinkedHashMap<Integer, List<Map<String, Object>>>();
    	
    	for(Map<String, Object> valNote : notes)
    	{
    		valNote.put("created", DATE_FORMAT.print(((DateTime)valNote.get("created"))));
    		if(!map.containsKey(valNote.get("sbid")))
    		{
    			map.put((Integer)valNote.get("sbid"), new ArrayList<Map<String, Object>>());
    		}
    		map.get((Integer)valNote.get("sbid")).add(valNote);
    	}
    	
    	logger.info("Validation notes returned for SBIDs: {}", Arrays.toString(map.keySet().toArray()));
    	return map;
    }
    
    
    /**
     * Queries the continuum components table in the database for the coordinates matching the supplied object name
     * @param objectName the name of the object to check the database for
     * @return the coordinates of the object in question if it exists in our database, return null if nothing found
     */
    @RequestMapping(value = "/metadata/coordinates", method = RequestMethod.GET, 
    		produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Double> resolveObjectName(@RequestParam("objectName") String objectName)
    {
    	logger.info("Coordinates requested for object name: {}", objectName);
    	List<Map<String, Double>> coords = continuumComponentRepository.resolveObjectName(objectName);
    	
    	logger.info("Coordinates returned for object name: {}", objectName);
    	return coords.isEmpty() ? new HashMap<String, Double>() : coords.get(0);
    }
    
	/**
	 * @param sbid the primary sbid of the observation
     * @param projectCode the opal code of the project to query the database for
	 * @return a list of encapsulation file ids
	 */
    @RequestMapping(value = "/metadata/evaluationEncapsulation", method = RequestMethod.GET, 
    		produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getEvaluationFilesForObservationAndProject(@RequestParam("sbid") Integer sbid, 
    		@RequestParam("projectCode") String projectCode)
    {
    	logger.info("Evaluation files requested for observation: {}, project {}", sbid, projectCode);
    	List<String> ids;
    	try
    	{

        	ids = dataAccessJdbcRepository.getEvaluationIds(sbid, projectCode);
    	}
    	catch(Exception e)
    	{
    		ids = new ArrayList<String>();
    	}
    	
    	logger.info("Evaluation file ids returned for observation: {}", sbid);
    	return ids;
    }
    
	/**
	 * @param sbid the primary sbid of the observation
     * @param projectCode the opal code of the project to query the database for
	 * @return a list of metrics structured as a Map of name &amp; values
	 */
    @RequestMapping(value = "/metadata/validationmetrics", method = RequestMethod.GET, 
    		produces = MediaType.APPLICATION_JSON_VALUE)
    public List<Map<String, Object>> getValidationMetricsForObservationAndProject(@RequestParam("sbid") Integer sbid, 
    		@RequestParam("projectCode") String projectCode)
    {
    	logger.info("Validation Metrics requested for observation: {}, project {}", sbid, projectCode);
    	List<Map<String, Object>> metrics;
    	try
    	{
    		metrics = dataAccessJdbcRepository.getValidationMetricsForSbidAndProject(sbid, projectCode);
    	}
    	catch(Exception e)
    	{
    		//everything gets caught and logger here (this should be a black box to dap)
    		metrics = new ArrayList<Map<String, Object>>();
    		logger.warn("Retrieval of validation metrics for sbid {} & project {} failed due to : {}", 
    				sbid, projectCode, e.getMessage());
    	}
    	
    	logger.info("Validation Metrics returned for observation: {}, project {}", sbid, projectCode);
    	return metrics;
    } 
}
