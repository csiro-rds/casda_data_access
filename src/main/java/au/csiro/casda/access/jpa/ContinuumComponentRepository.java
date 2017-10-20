package au.csiro.casda.access.jpa;

import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.sourcedetect.ContinuumComponent;

/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2017 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */
/**
 * JPA Repository declaration. Copyright 2017, CSIRO Australia All rights reserved.
 */
@Repository
public interface ContinuumComponentRepository extends CrudRepository<ContinuumComponent, Long>
{
	/**
	 * @param objectName the opbject name (component_id) to search for
	 * @return a list containing maps containing the ra &amp; dec in degrees for this object name (only first in needed)
	 */
	@Query("SELECT new map(cc.raDegCont as ra, cc.decDegCont as dec) "
			+ "FROM ContinuumComponent cc WHERE cc.componentName = :objectName")
	public List<Map<String, Double>> resolveObjectName(@Param("objectName") String objectName);
}
