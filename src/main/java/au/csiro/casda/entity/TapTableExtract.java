package au.csiro.casda.entity;

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

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Extract of VO TAP table information from the database. Used to map the database tables to the table exposed by VO
 * TAP.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Entity
@Table(name = "tap_tables", schema = "casda")
public class TapTableExtract
{

    @Id
    @Column
    private String tableName;

    @Column
    private String schemaName;

    @Column
    private String dbSchemaName;

    @Column
    private String dbTableName;

    /**
     * Default constructor.
     */
    public TapTableExtract()
    {
    }

    /**
     * Constructor with args.
     * 
     * @param schemaName
     *            the TAP schema name
     * @param tableName
     *            the TAP table name (we store the qualified TAP table name)
     * @param dbSchemaName
     *            the database schema name
     * @param dbTableName
     *            the database table name
     */
    public TapTableExtract(String schemaName, String tableName, String dbSchemaName, String dbTableName)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.dbSchemaName = dbSchemaName;
        this.dbTableName = dbTableName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getDbSchemaName()
    {
        return dbSchemaName;
    }

    public String getDbTableName()
    {
        return dbTableName;
    }

}
