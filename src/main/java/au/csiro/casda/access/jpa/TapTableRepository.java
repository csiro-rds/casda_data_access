package au.csiro.casda.access.jpa;

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

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.TapTableExtract;

/**
 * JPA Repository declaration.
 * <p>
 * Copyright 2015, CSIRO Australia All rights reserved.
 */
@Repository
public interface TapTableRepository extends CrudRepository<TapTableExtract, String>
{
    /**
     * Finds the Tap Table record matching the given database schema and table name
     * 
     * @param dbSchemaName
     *            the database schema name
     * @param dbTableName
     *            the database table name
     * @return the Tap Table record
     */
    @Query("SELECT tt FROM TapTableExtract tt WHERE tt.dbSchemaName = :dbSchemaName AND tt.dbTableName = :dbTableName")
    public TapTableExtract findByTapTable(@Param("dbSchemaName") String dbSchemaName,
            @Param("dbTableName") String dbTableName);
}