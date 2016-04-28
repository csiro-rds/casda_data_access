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

import static org.junit.Assert.assertEquals;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import javax.transaction.Transactional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.jpa.repository.support.JpaRepositoryFactory;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

import au.csiro.casda.access.BaseTest;
import au.csiro.casda.entity.TapTableExtract;

/**
 * Tests for the repository using in memory GeoDb instance.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 */
public class TapTableRepositoryTest extends BaseTest
{
    @PersistenceUnit
    private EntityManagerFactory emf;

    private TapTableRepository repository;

    private EntityManager entityManager;

    public TapTableRepositoryTest() throws Exception
    {
        super();
    }

    @Before
    @Transactional
    public void setup()
    {
        entityManager = emf.createEntityManager();

        RepositoryFactorySupport rfs = new JpaRepositoryFactory(entityManager);
        repository = rfs.getRepository(TapTableRepository.class);

        repository.deleteAll();

        entityManager.getTransaction().begin();

        TapTableExtract tapTableExtractOne = new TapTableExtract("schema1", "schema1.table1", "casda", "table_one");
        TapTableExtract tapTableExtractTwo = new TapTableExtract("schema2", "schema2.table2", "casda", "table_two");
        repository.save(tapTableExtractOne);
        repository.save(tapTableExtractTwo);
    }

    @After
    public void tearDown()
    {
        entityManager.getTransaction().rollback();
    }

    @Test
    public void testFindTapTableExtract()
    {
        TapTableExtract result = repository.findByTapTable("casda", "table_two");
        assertEquals("schema2.table2", result.getTableName());

        result = repository.findByTapTable("casda", "table_one");
        assertEquals("schema1.table1", result.getTableName());
    }

}
