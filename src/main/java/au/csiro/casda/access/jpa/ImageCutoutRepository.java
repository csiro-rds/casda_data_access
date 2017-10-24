package au.csiro.casda.access.jpa;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import au.csiro.casda.entity.dataaccess.ImageCutout;

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
 * Copyright 2014, CSIRO Australia. All rights reserved.
 */
@Repository
public interface ImageCutoutRepository extends CrudRepository<ImageCutout, Long>
{
    /**
     * Retrieve a list of imageCutout objects which match the parent image and bounds but have not yet been expired.
     * 
     * @param imageCubeId
     *            The id of the parent image cube that is being subsetted.
     * @param bounds
     *            The bounds of the cutout
     * @param downloadFormat
     *            The format of the output.
     * @return A list of matching image cutout objects
     */
    @Query("SELECT cutout FROM ImageCutout cutout WHERE cutout.imageCube.id = :imageCubeId "
            + "AND cutout.bounds like :bounds% AND cutout.dataAccessJob.status in ('PREPARING', 'READY') "
            + "AND cutout.dataAccessJob.downloadFormat = :downloadFormat "
            + "ORDER BY cutout.dataAccessJob.lastModified DESC")
    public List<ImageCutout> findByImageCubeIdBoundsAndDownloadFormat(@Param("imageCubeId") long imageCubeId,
            @Param("bounds") String bounds, @Param("downloadFormat") String downloadFormat);
}
