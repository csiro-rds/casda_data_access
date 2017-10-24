package au.csiro.casda.access.jpa;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.observation.Thumbnail;

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
 * JPA Repository declaration.
 * <p>
 * Copyright 2016, CSIRO Australia. All rights reserved.
 */
@Repository
public interface ThumbnailRepository extends CrudRepository<Thumbnail, Long>
{
	/**
	 * retrieves a thumbnail based on the filename and the observation
	 * @param fileName the filename of the thumbnail
	 * @param sbid the observation's sbid
	 * @return the matching thumbnail
	 */
    @Query("SELECT thumb FROM Thumbnail thumb WHERE filename = :fileName and thumb.observation.sbid = :sbid")
    public Thumbnail findThumbnail(@Param("fileName") String fileName, @Param("sbid") Integer sbid);

    /**
     * Retrieves a thumbnail based on the filename and the level 7 collection id
     * @param fileName the filename of the thumbnail
     * @param dataCollectionId the dap collection id of the parent level 7 collection
     * @return the matching thumbnail
     */
    @Query("SELECT thumb FROM Thumbnail thumb WHERE filename = :fileName and"
            + " thumb.level7Collection.dapCollectionId = :dcid")
    public Thumbnail findLevel7Thumbnail(@Param("fileName") String fileName, @Param("dcid") long dataCollectionId);
}
