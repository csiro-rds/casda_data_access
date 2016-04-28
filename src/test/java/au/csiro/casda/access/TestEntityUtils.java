package au.csiro.casda.access;

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


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import au.csiro.casda.entity.observation.Catalogue;
import au.csiro.casda.entity.observation.CatalogueType;
import au.csiro.casda.entity.observation.ImageCube;
import au.csiro.casda.entity.observation.MeasurementSet;
import au.csiro.casda.entity.observation.Observation;
import au.csiro.casda.entity.observation.Project;
import au.csiro.casda.entity.observation.Scan;
import au.csiro.casda.entity.sourcedetect.ContinuumComponent;
import au.csiro.casda.entity.sourcedetect.ContinuumIsland;
import au.csiro.casda.entity.sourcedetect.PolarisationComponent;

/**
 * Creates sample entities with mandatory and unique fields.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 * 
 */
public class TestEntityUtils
{

    public static Catalogue createCatalogue(Observation observation, long projectCodeNum, long catalogueId,
            CatalogueType catalogueType, int idOffset)
    {
        Project project = createProject(projectCodeNum, idOffset);

        Catalogue catalogue = new Catalogue();
        catalogue.setCatalogueType(catalogueType);
        catalogue.setFilename("filename" + (catalogueId + idOffset));
        catalogue.setFormat("format");
        catalogue.setProject(project);
        observation.addCatalogue(catalogue);
        return catalogue;
    }

    public static Project createProject(long projectCodeNum, int idOffset)
    {
        Project project = new Project();
        project.setOpalCode("ABC" + (projectCodeNum + idOffset));
        project.setShortName("shortName");
        return project;
    }

    public static Observation createObservation(int sbid, int idOffset)
    {
        Observation observation = new Observation();
        observation.setTelescope("telescope");
        observation.setObsStartMjd(123456.1);
        observation.setObsStart(DateTime.now(DateTimeZone.UTC));
        observation.setObsEndMjd(123556.2);
        observation.setObsEnd(DateTime.now(DateTimeZone.UTC));
        observation.setObsProgram("obsProgram");
        observation.setSbid(sbid + idOffset);
        return observation;
    }

    public static ImageCube createImageCube(long filenameId, long projectCodeNum, Observation observation, int idOffset)
    {
        Project project = TestEntityUtils.createProject(projectCodeNum, idOffset);

        ImageCube imageCube = new ImageCube();
        imageCube.setFilename("file_name" + (filenameId + idOffset) + ".fits");
        imageCube.setFormat("fits");
        imageCube.setProject(project);
        imageCube.setType("Restored");
        observation.addImageCube(imageCube);
        return imageCube;
    }

    public static MeasurementSet createMeasurementSet(long filenameId, long projectCodeNum, Observation observation,
            int idOffset)
    {
        Project project = TestEntityUtils.createProject(projectCodeNum, idOffset);

        MeasurementSet measurementSet = new MeasurementSet();
        measurementSet.setFilename("measurement.set." + (filenameId + idOffset));
        measurementSet.setFormat("xml");
        measurementSet.setProject(project);
        observation.addMeasurementSet(measurementSet);
        return measurementSet;
    }

    public static Scan createScan(long scanId, MeasurementSet measurementSet, int idOffset)
    {
        Scan scan = new Scan();
        scan.setFieldName("fieldName" + (scanId + idOffset));
        scan.setCoordSystem("coordSystem");
        scan.setScanStart(DateTime.now(DateTimeZone.UTC));
        scan.setScanEnd(DateTime.now(DateTimeZone.UTC));
        scan.setCentreFrequency(12.19);
        scan.setPolarisations("[XX]");
        scan.setScanId((int) scanId);
        scan.setNumChannels(5);
        scan.setFieldCentreX(15.1);
        scan.setFieldCentreY(17.9);
        scan.setChannelWidth(12.1);
        measurementSet.addScan(scan);

        return scan;
    }

    public static ContinuumComponent createContinuumComponent(String componentName, Catalogue catalogue)
    {
        ContinuumComponent continuumComponent = new ContinuumComponent();
        continuumComponent.setComponentName(componentName);
        continuumComponent.setCatalogue(catalogue);
        return continuumComponent;
    }

    public static ContinuumIsland createContinuumIsland(String componentName, Catalogue catalogue)
    {
        ContinuumIsland continuumIsland = new ContinuumIsland();
        continuumIsland.setIslandName(componentName);
        continuumIsland.setCatalogue(catalogue);
        return continuumIsland;
    }

    public static PolarisationComponent createPolarisationComponent(String componentName, Catalogue catalogue)
    {
        PolarisationComponent polarisationComponent = new PolarisationComponent();
        polarisationComponent.setComponentName(componentName);
        polarisationComponent.setCatalogue(catalogue);
        return polarisationComponent;
    }

}
