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

/**
 * Data Access Data Product represents a data product requested for download by the user.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class DataAccessDataProduct
{
    /**
     * Data product types available for VO data access. These match the data product types in obscore records.
     * <p>
     * Copyright 2015, CSIRO Australia. All rights reserved.
     */
    public enum DataAccessProductType
    {

        /** visibility records in obscore map to records in our measurement_set database table */
        visibility,
        /** cube records in obscore map to records in our image_cube database table */
        cube,
        /** records in catalogue database table */
        catalogue;
    }

    private final DataAccessProductType dataAccessProductType;
    private final Long id;
    private final String dataProductId;

    /**
     * Constructor from data product id
     * 
     * @param dataProductId
     *            the data product id (format: type-id)
     */
    public DataAccessDataProduct(String dataProductId)
    {
        if (dataProductId == null)
        {
            throw new IllegalArgumentException("Invalid data product id: " + dataProductId);
        }
        String[] dataProductIdSplit = dataProductId.split("-");
        if (dataProductIdSplit.length != 2)
        {
            throw new IllegalArgumentException("Invalid data product id: " + dataProductId);
        }
        this.dataAccessProductType = DataAccessProductType.valueOf(dataProductIdSplit[0]);
        this.id = Long.valueOf(dataProductIdSplit[1]);
        this.dataProductId = dataProductId;
    }

    /**
     * Constructor
     * 
     * @param dataAccessProductType
     *            the data access product type
     * @param id
     *            the data product id
     */
    public DataAccessDataProduct(DataAccessProductType dataAccessProductType, Long id)
    {
        this.dataAccessProductType = dataAccessProductType;
        this.id = id;
        this.dataProductId = dataAccessProductType.name() + "-" + id;
    }

    public DataAccessProductType getDataAccessProductType()
    {
        return dataAccessProductType;
    }

    public Long getId()
    {
        return id;
    }

    public String getDataProductId()
    {
        return dataProductId;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((dataAccessProductType == null) ? 0 : dataAccessProductType.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        DataAccessDataProduct other = (DataAccessDataProduct) obj;
        if (dataAccessProductType != other.dataAccessProductType)
        {
            return false;
        }
        if (id == null)
        {
            if (other.id != null)
            {
                return false;
            }
        }
        else if (!id.equals(other.id))
        {
            return false;
        }
        return true;
    }

}
