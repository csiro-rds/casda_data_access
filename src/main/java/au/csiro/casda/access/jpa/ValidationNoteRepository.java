package au.csiro.casda.access.jpa;

import java.util.List;
import java.util.Map;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.ValidationNote;

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
public interface ValidationNoteRepository extends CrudRepository<ValidationNote, Long>
{
	/**
	 * @param sbid the array of sbis to search for
	 * @param project the opal code of the project
	 * @return a list of mapped values, containing the comment, the author and the date
	 */
	@Query("SELECT new map(v.personName as personName, v.created as created, v.content as content, v.sbid as sbid) "
			+ "FROM ValidationNote v WHERE v.sbid in :sbids and v.project.opalCode = :project order by v.created")
	public List<Map<String, Object>> getValidationNotesForSbids(
			@Param("sbids")Integer[] sbid, @Param("project") String project);
}
