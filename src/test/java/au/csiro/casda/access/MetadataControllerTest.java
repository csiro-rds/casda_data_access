package au.csiro.casda.access;

import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import au.csiro.casda.access.jdbc.DataAccessJdbcRepository;
import au.csiro.casda.access.jpa.ContinuumComponentRepository;
import au.csiro.casda.access.jpa.ValidationNoteRepository;

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
 * Tests the Data Access UI Controller.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
public class MetadataControllerTest 
{
	private DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MM/yyyy");
	
	private MetadataAccessController controller;
	
	@Mock
	private ValidationNoteRepository validationNoteRepository;
	
	@Mock
	private ContinuumComponentRepository continuumComponentRepository;
	
	@Mock
	private DataAccessJdbcRepository dataAccessJdbcRepository;

    /**
     * Set up the ui controller before each test.
     * 
     * @throws Exception
     *             any exception thrown during set up
     */
    @Before
    public void setUp() throws Exception
    {
        MockitoAnnotations.initMocks(this);
        controller = new MetadataAccessController(
        		validationNoteRepository, continuumComponentRepository, dataAccessJdbcRepository);
    }
    
    @Test
    public void testGetValidationNotes()
    {
    	when(validationNoteRepository.getValidationNotesForSbids(any(), any())).thenReturn(createMockResults());	
    	
    	Map<Integer, List<Map<String, Object>>> results = 
    			controller.getValidationNotes(new Integer[]{1234,5678}, "AS031");
    	
    	//map keys
    	assertEquals(2, results.size());
    	assertTrue(results.containsKey(1234));
    	assertTrue(results.containsKey(5678));
    	
    	//first map entry
    	assertEquals(3, results.get(1234).size());
    	//contents
    		//first map
	    	assertEquals("Marigold Smith", results.get(1234).get(0).get("personName"));
	    	assertEquals("2016-12-04 00:00", results.get(1234).get(0).get("created"));
	    	assertEquals("This is a comment", results.get(1234).get(0).get("content"));
	    	assertEquals(1234, results.get(1234).get(0).get("sbid"));
    		//second map
	    	assertEquals("Archibald Brown", results.get(1234).get(1).get("personName"));
	    	assertEquals("2016-12-06 00:00", results.get(1234).get(1).get("created"));
	    	assertEquals("This is a response", results.get(1234).get(1).get("content"));
	    	assertEquals(1234, results.get(1234).get(1).get("sbid"));
    		//third map
	    	assertEquals("Benjamin Israeli", results.get(1234).get(2).get("personName"));
	    	assertEquals("2016-12-12 00:00", results.get(1234).get(2).get("created"));
	    	assertEquals("This is a somthing else", results.get(1234).get(2).get("content"));
	    	assertEquals(1234, results.get(1234).get(2).get("sbid"));

    	//second map entry
    	assertEquals(1, results.get(5678).size());
    	//contents
	    	assertEquals("Marigold Smith", results.get(5678).get(0).get("personName"));
	    	assertEquals("2016-12-18 00:00", results.get(5678).get(0).get("created"));
	    	assertEquals("I like cheese!", results.get(5678).get(0).get("content"));
	    	assertEquals(5678, results.get(5678).get(0).get("sbid"));
    }
    
    
    private List<Map<String, Object>> createMockResults()
    {
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    	Map<String, Object> map1 = new HashMap<String, Object>();
    	map1.put("personName", "Marigold Smith");
    	map1.put("created", formatter.parseDateTime("04/12/2016"));
    	map1.put("content", "This is a comment");
    	map1.put("sbid", 1234);
    	results.add(map1);
    	
    	Map<String, Object> map2 = new HashMap<String, Object>();
    	map2.put("personName", "Archibald Brown");
    	map2.put("created", formatter.parseDateTime("06/12/2016"));
    	map2.put("content", "This is a response");
    	map2.put("sbid", 1234);
    	results.add(map2);
    	
    	Map<String, Object> map3 = new HashMap<String, Object>();
    	map3.put("personName", "Benjamin Israeli");
    	map3.put("created", formatter.parseDateTime("12/12/2016"));
    	map3.put("content", "This is a somthing else");
    	map3.put("sbid", 1234);
    	results.add(map3);
    	
    	Map<String, Object> map4 = new HashMap<String, Object>();
    	map4.put("personName", "Marigold Smith");
    	map4.put("created", formatter.parseDateTime("18/12/2016"));
    	map4.put("content", "I like cheese!");
    	map4.put("sbid", 5678);
    	results.add(map4);
    	
    	return results;
    }
}
