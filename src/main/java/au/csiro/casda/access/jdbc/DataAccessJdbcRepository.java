package au.csiro.casda.access.jdbc;

import java.util.HashMap;

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


import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.dataaccess.CachedFile.FileType;


/**
 * 
 * Simple JDBC repository for executing arbitrary SQL statements
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Repository
public class DataAccessJdbcRepository
{

    private JdbcTemplate jdbcTemplate;
    
    @Autowired
    public void setDataSource(DataSource dataSource) 
    {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }
    
    /**
     * Execute any SQL statement without a return type
     * @param statement
     *          SQL String
     */
    public void executeStatement(String statement)
    {
        this.jdbcTemplate.execute(statement);
    }
    
    /**
     * @param requestId the dta access job request id
     * @param fileType the type of file to search for ( used to identify table)
     * @param page the paging details
     * @return matching records up to the limit
     */
    public List<Map<String, Object>> getPageOfCatalogues(String requestId, String fileType, Integer[] page)
    {
    	String query = "SELECT cat.id AS id, cat.catalogue_type AS catalogueType, cat.entries_table_name AS " +
    				"tableName, cat.filename AS fileName, proj.opal_code as projectId, obs.sbid AS obsId, "
    				+ "l7.dap_collection_id AS l7Id " +
    				"FROM casda.catalogue cat " + 
    				"LEFT JOIN casda.data_access_job_catalogue jt ON cat.id = jt.catalogue_id " +
    				"LEFT JOIN casda.data_access_job daj ON jt.data_access_job_id = daj.id " +
    				"LEFT JOIN casda.project proj ON cat.project_id = proj.id " +
    				"LEFT JOIN casda.observation obs ON cat.observation_id = obs.id " +
    				"LEFT JOIN casda.level7_collection l7 ON cat.level7_collection_id = l7.id " +
    				"WHERE request_id = ? " +
    				" LIMIT ? "+
    				" OFFSET ?";

    	return jdbcTemplate.queryForList(query, requestId, (page[1]-page[0]+1), (page[0]-1));
    }
    
    
    /**
     * @param requestId the dta access job request id
     * @param fileType the type of file to search for ( used to identify table)
     * @param page the paging details
     * @return matching records up to the limit
     */
    public List<Map<String, Object>> getPageOfGeneratedFiles(String requestId, String fileType, Integer[] page)
    {
    	String query = "SELECT gen.id as id, ic.id as imageCubeId, gen.filesize as fileSize, ic.filesize as " +
    					"imageCubeSize, obs.sbid AS obsId, l7.dap_collection_id AS l7Id, ic.filename as filename," +
    					"daj.download_format as format " +
						"FROM casda.data_access_job daj " +
						"LEFT JOIN casda.generated_image gen ON daj.id = gen.data_access_job_id " +
						"LEFT JOIN casda.image_cube ic ON gen.image_cube_id = ic.id " +
						"LEFT JOIN casda.observation obs ON ic.observation_id = obs.id " +
						"LEFT JOIN casda.level7_collection l7 ON ic.level7_collection_id = l7.id " +
						"WHERE request_id = ? " +
						"AND gen.generated_file_type = ? " +
						"ORDER BY gen.id " +
						" LIMIT ? " +
						" OFFSET ?";
    	
    	return jdbcTemplate.queryForList(query ,requestId, fileType, (page[1]-page[0]+1), (page[0]-1));
    }
    
    /**
     * @param requestId the dta access job request id
     * @param type the type of file to search for ( used to identify table)
     * @param page the paging details
     * @return matching records up to the limit
     */
    public List<Map<String, Object>> getPageOfErrorFiles(String requestId, String type, Integer[] page)
    {
    	String query = 	"SELECT error.id AS id, error.error_message AS message " +
						"FROM casda.data_access_job_error error, casda.data_access_job daj " +
						"WHERE error.data_access_job_id = daj.id " +
						"AND daj.request_id = ? " +
						"ORDER BY error.id" +
						" LIMIT ? " + 
						" OFFSET ?";
    	
    	return jdbcTemplate.queryForList(query, requestId, (page[1]-page[0]+1), (page[0]-1));
    }

    /**
     * @param type the type/table to search on
     * @param requestId the request id of the data access job
     * @param page the page bounds
     * @param encapsulated true if this data product type is encapsulated on deposit
     * @return a list containing the details of each file
     */
    public List<Map<String, Object>> getPageOfDownloadFiles(String type, String requestId, Integer[] page, 
    		boolean encapsulated)
    {
    	String query = "SELECT artefact.id AS id, artefact.filesize AS fileSize, artefact.filename AS filename, " +
				"obs.sbid AS obsId ";
        if(! (type.equals(FileType.MEASUREMENT_SET.name()) || type.equals(FileType.EVALUATION_FILE.name())))
		{
			query += ", l7.dap_collection_id AS l7Id ";
		}
		if(encapsulated)
		{
			query +=", encap.filename AS encapsulationFilename, encap.filesize AS encapsulationFileSize, encap.id "
					+ "AS encapsulationId ";
		}
		query +=	"FROM casda." + type + " artefact " +
					"LEFT JOIN casda.data_access_job_" + type + " jt ON artefact.id = jt." + type + "_id " + 
					"LEFT JOIN casda.data_access_job daj ON jt.data_access_job_id = daj.id " +
					"LEFT JOIN casda.observation obs ON artefact.observation_id = obs.id ";
		if(! (type.equals(FileType.MEASUREMENT_SET.name()) || type.equals(FileType.EVALUATION_FILE.name())))
		{
			query += "LEFT JOIN casda.level7_collection l7 ON artefact.level7_collection_id = l7.id ";
		}
		if(encapsulated)
		{
			query +="LEFT JOIN casda.encapsulation_file encap ON artefact.encapsulation_file_id = encap.id ";  
		}
		
		query += "WHERE request_id = ? " +
				"ORDER BY filename" +
				" OFFSET ? " +
				" LIMIT ?";
		
    	return jdbcTemplate.queryForList(query, requestId, (page[0]-1), (page[1]-page[0]+1));
    }
    
    /**
     * @param id the of the data access job
     * @return true is any image cubes or measurement sets exist
     */
	public boolean isImageCubesAndMeasurementSetsExist(Long id) 
	{
		String query =  "SELECT SUM(total) >= 1 "
					+ "FROM (SELECT COUNT (*) AS total "
					+ "FROM casda.data_access_job_image_cube ic, casda.data_access_job daj "
					+ "WHERE ic.data_access_job_id = daj.id "
					+ "AND daj.request_id = ? "
					+ "UNION ALL SELECT COUNT(*) AS total "
					+ "FROM casda.data_access_job_measurement_set ms, casda.data_access_job daj "
					+ "WHERE ms.data_access_job_id = daj.id "
					+ "AND daj.request_id = ?) AS finalTotal";
		return jdbcTemplate.queryForObject(query, new Object[]{id.toString(), id.toString()}, Boolean.class);
	}	

    /**
	 * @param requestId the request id of the data access job
	 * @return the map of counts for each file type
	 */
	public Map<String, Object> countFilesForJob(String requestId)
	{
		String query = 	"SELECT  'IMAGE_CUBE' as type, count(dajic) " +
						"FROM casda.data_access_job daj " +
						"LEFT JOIN casda.data_access_job_image_cube dajic ON dajic.data_access_job_id = daj.id " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'MEASUREMENT_SET' as type, count(dajms) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_measurement_set dajms ON dajms.data_access_job_id = daj.id " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'ENCAPSULATION_FILE' as type, count(dajef) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_encapsulation_file dajef ON dajef.data_access_job_id = daj.id " +
						"WHERE request_id = ? " +
						"UNION " + 
						"SELECT  'EVALUATION_FILE' as type, count(dajevf) " +
                        "FROM casda.data_access_job daj  " +
                        "LEFT JOIN casda.data_access_job_evaluation_file dajevf ON dajevf.data_access_job_id = daj.id " +
                        "WHERE request_id = ? " +
						"UNION " +
						"SELECT  'SPECTRUM' as type, count(dajspec) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_spectrum dajspec ON dajspec.data_access_job_id = daj.id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'MOMENT_MAP' as type, count(dajmm) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_moment_map dajmm ON dajmm.data_access_job_id = daj.id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'CUBELET' as type, count(dajcu) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_cubelet dajcu ON dajcu.data_access_job_id = daj.id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'CATALOGUE' as type, count(dajcat) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_catalogue dajcat ON dajcat.data_access_job_id = daj.id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'IMAGE_CUTOUT' as type, count(cutout) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN ( " +
						"select * from casda.generated_image where generated_file_type = 'IMAGE_CUTOUT' " +
						") AS cutout ON daj.id = cutout.data_access_job_id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'GENERATED_SPECTRUM' as type, count(generated_spectrum) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN ( " +
						"select * from casda.generated_image where generated_file_type = 'GENERATED_SPECTRUM' " +
						") AS generated_spectrum ON daj.id = generated_spectrum.data_access_job_id  " +
						"WHERE request_id = ? " +
						"UNION " +
						"SELECT  'ERROR' as type, count(dajerr) " +
						"FROM casda.data_access_job daj  " +
						"LEFT JOIN casda.data_access_job_error dajerr ON dajerr.data_access_job_id = daj.id " +
						"WHERE request_id = ?";
		List<Map<String, Object>> result = jdbcTemplate.queryForList(query, requestId, requestId, requestId, requestId,
				requestId, requestId, requestId, requestId, requestId, requestId, requestId);
		Map<String, Object> counts = new HashMap<String, Object>();
		for(Map<String, Object> temp : result)
		{
			counts.put((String)temp.get("type"), temp.get("count"));
		}
		
		return counts;
	}
	
	/**
	 * @param sbid the primary sbid of the observation
	 * @param projectCode the opal code of the project to match
	 * @return a list of encapsulation file ids
	 */
	public List<String> getEvaluationIds(Integer sbid, String projectCode) 
	{
		String query = 	"SELECT 'encap-'::text || identifier " +
						"FROM ( " +
						"SELECT distinct encaps.id as identifier " +
						"FROM casda.observation obs " +
						"LEFT JOIN casda.evaluation_file eval ON eval.observation_id = obs.id " +
						"LEFT JOIN casda.encapsulation_file encaps ON encaps.id = eval.encapsulation_file_id " +
						"LEFT JOIN casda.project proj ON proj.id = eval.project_id " +
						"WHERE obs.sbid = ? " +
						"AND proj.opal_code = ? ) AS subquery";
		return jdbcTemplate.queryForList(query, String.class, sbid, projectCode);
	}

	/**
	 * @param sbid the primary sbid for the observation in question
	 * @param projectCode the opal code for the project in question
	 * @return a list of metrics structured as a Map of name &amp; values
	 */
	public List<Map<String, Object>> getValidationMetricsForSbidAndProject(Integer sbid, String projectCode) 
	{
		String query =  "SELECT metric.metric_name AS name, metric.description AS description, " +
				 		"metricValue.status AS status, metricValue.metric_value AS value " +
						"FROM casda.validation_metric metric " +
						"LEFT JOIN casda.validation_metric_value metricValue " +
						"ON metricValue.metric_id = metric.id " +
						"LEFT JOIN casda.observation obs ON obs.id = metricValue.observation_id " +
						"LEFT JOIN casda.project proj ON proj.id = metricValue.project_id " +
						"WHERE obs.sbid = ? " +
						"AND proj.opal_code = ? " +
						"ORDER BY metricValue.id";
		return jdbcTemplate.queryForList(query, sbid, projectCode);
	}
	
	/**
	 * @param projectCode the project opal code to check against the database
	 * @return true if this project exists
	 */
	public boolean projectExists(String projectCode)
	{
		String query = "select EXISTS(select id from casda.project where opal_code = ?)";
		return jdbcTemplate.queryForObject(query, new Object[]{projectCode}, Boolean.class);
	}
}
