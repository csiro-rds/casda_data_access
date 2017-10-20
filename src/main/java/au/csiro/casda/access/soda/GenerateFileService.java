package au.csiro.casda.access.soda;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.ImageCube;

/**
 * A service class for producing generated files such as generated files or spectra from image cubes.
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
@Service
public class GenerateFileService
{
    private static Logger logger = LoggerFactory.getLogger(GenerateFileService.class);

    private String sregionContainsQuery;

    private JdbcTemplate jdbcTemplate;
    
    /**
     * Create a new instance of CutoutService
     *   
     * @param sregionContainsQuery The sql query to check if the generated file area is within the image cube. 
     * @param jdbcTemplate The template to be used for database queries.
     */
    @Autowired
    public GenerateFileService(@Value("${sregion.contains.query}") 
    	String sregionContainsQuery, JdbcTemplate jdbcTemplate)
    {
        this.sregionContainsQuery = sregionContainsQuery;
        this.jdbcTemplate = jdbcTemplate;
    }
    
    /**
     * Calculate a set of bounds, one for each pos parameter in the supplied map.
     * 
     * @param dataAccessJobParams
     *            The map of input parameters, all keys in lower case.
     * @param imageCube
     *            The image cube being processed.
     * @param dataAccessProductId
     *            The id of the image cube 
     * @param paramsWithoutCutouts 
     * 			  The list of params which will generate no result
     * @return A list of bounds values, in the same order as the supplied parameter array.
     */
    public List<GeneratedFileBounds> calcGeneratedFileBounds(ParamMap dataAccessJobParams, ImageCube imageCube,
            String dataAccessProductId, List<String> paramsWithoutCutouts)
    {
        List<GeneratedFileBounds> posBoundsList = buildBoundsList(dataAccessJobParams, imageCube);
        
        if ((dataAccessJobParams.get("pos") != null || dataAccessJobParams.get("circle") != null || 
                dataAccessJobParams.get("polygon") != null) && posBoundsList.isEmpty())
        {
            paramsWithoutCutouts.addAll(listUnsatisfiedParams(dataAccessJobParams, posBoundsList, dataAccessProductId));
            // The spatial bounds do not overlap this image, so no generated file can be generated from it
            return new ArrayList<GeneratedFileBounds>();
        }

        // calculate non spatial axis ranges for each image
        List<ImageCubeAxis> axisList = getAxisList(imageCube);
        List<GeneratedFileBounds> dimensionSets =
        		getDimensionSets(dataAccessJobParams, axisList); //, paramsWithoutCutouts); 

        // Set a default bounds if we have generated file params, but no POS
        if (posBoundsList.isEmpty() && hasDimensionCriteria(dataAccessJobParams))
        {
            final double largestXSize = 360.0;
            GeneratedFileBounds bounds = new GeneratedFileBounds(String.valueOf(imageCube.getRaDeg()),
                    String.valueOf(imageCube.getDecDeg()), largestXSize);
            posBoundsList.add(bounds);
        }

        // Cross multiply pos and plane criteria
        List<GeneratedFileBounds> multiDimBoundsList = new ArrayList<>();
        for (GeneratedFileBounds planeRange : dimensionSets)
        {
            String[] dimBounds = planeRange.getDimBounds();
            for (GeneratedFileBounds posBounds : posBoundsList)
            {
                GeneratedFileBounds bounds = new GeneratedFileBounds(posBounds);
                int index = 0;
                for (String dimRange : dimBounds)
                {
                    // We need to filter out bounds which are for axes representing single planes
                    if (dimRange != null
                            && (axisList.get(index).getSize() > 1 || axisList.get(index).getPlaneSpan() > 1))
                    {
                        bounds.setDimBounds(index, dimRange);
                    }
                    index++;
                }
                bounds.setNumPlanes(planeRange.getNumPlanes());
                String params = "";
                if (StringUtils.isNotBlank(posBounds.getParams()))
                {
                    params = posBounds.getParams();
                }
                if (StringUtils.isNotBlank(planeRange.getParams()))
                {
                    if (params.length() > 0)
                    {
                        params += ",";
                    }
                    params += planeRange.getParams();
                }
                bounds.setParams(params);
                multiDimBoundsList.add(bounds);
            }
        }

        paramsWithoutCutouts.addAll(listUnsatisfiedParams(dataAccessJobParams, multiDimBoundsList, dataAccessProductId));
        
        logger.info(String.format("Applied %s to image cube-%s and produced bounds of %s.", dataAccessJobParams,
                imageCube.getId(), multiDimBoundsList));
        return multiDimBoundsList;
    }
    
    private Collection<String> listUnsatisfiedParams(ParamMap dataAccessJobParams,
            List<GeneratedFileBounds> boundsList, String dataAccessProductId)
    {
        // build list of all param combos
        List<String> paramCombos = buildParamCombos(dataAccessJobParams);
        
        
        // Remove those in the generated files
        for (GeneratedFileBounds bounds : boundsList)
        {
            
            if (!paramCombos.remove(bounds.getParams()))
            {
                logger.warn("Unable to find bounds " + bounds.getParams() + " in " + paramCombos);
            }
        }
        
        for (int i = 0; i < paramCombos.size(); i++)
        {
            paramCombos.set(i, paramCombos.get(i)+",ID="+dataAccessProductId);
        }

        return paramCombos;
    }

    /**
     * Build up a list of all the combinations of parameter values. Each combination would be expected to result in a
     * generated file. Note: This does not combine COORD with the matching standard params as that would require
     * knowledge of the axes being processed.
     * 
     * @param dataAccessJobParams
     *            The parameters for the generated file.
     * @return The list of parameter combinations.
     */
    List<String> buildParamCombos(ParamMap dataAccessJobParams)
    {
        List<String> paramCombos = new ArrayList<>();
        
        // First we treat all spatial params as a single parameter list 
        EnumSet<ParamKeyWhitelist> spatialAxisParamKeys =
                EnumSet.of(ParamKeyWhitelist.POS, ParamKeyWhitelist.CIRCLE, ParamKeyWhitelist.POLYGON);
        Map<ParamKeyWhitelist, String[]> paramVals = new  TreeMap<>();
        for (ParamKeyWhitelist paramKey : spatialAxisParamKeys)
        {
            String[] values = dataAccessJobParams.get(paramKey.name().toLowerCase());
            paramVals.put(paramKey, values);
            paramCombos.addAll(populateCombos(new ArrayList<String>(), paramKey, values));
        }

        // Now deal with the other entries (do band, pol, any others). Note ID is handled elsewhere 
        EnumSet<ParamKeyWhitelist> paramKeys =
                EnumSet.complementOf(EnumSet.of(ParamKeyWhitelist.ID, ParamKeyWhitelist.FORMAT));
        paramKeys.removeAll(spatialAxisParamKeys);
        List<ParamKeyWhitelist> paramKeyOrderedList = new ArrayList<>();
        paramKeyOrderedList.add(ParamKeyWhitelist.BAND);
        paramKeyOrderedList.add(ParamKeyWhitelist.POL);
        paramKeys.removeAll(paramKeyOrderedList);
        paramKeyOrderedList.addAll(paramKeys);
        
        for (ParamKeyWhitelist paramKey : paramKeyOrderedList)
        {
            String[] values = dataAccessJobParams.get(paramKey.name().toLowerCase());
            paramCombos = populateCombos(paramCombos, paramKey, values);
        }
        
        return paramCombos;
    }

    private List<String> populateCombos(List<String> existingCombos, ParamKeyWhitelist paramKey, String[] values)
    {
        if (ArrayUtils.isEmpty(values))
        {
            return existingCombos;
        }
        
        List<String> newParamCombos = new ArrayList<>();
        List<String> valueList = Arrays.asList(values);
        if (paramKey == ParamKeyWhitelist.POL)
        {
            valueList = PolParamProcessor.getPolParams(values);
        }
        for (String val : valueList)
        {
            String param = (paramKey != ParamKeyWhitelist.POL ? paramKey.name() + "=" : "") + val;
            for (String existingCombo : existingCombos)
            {
                newParamCombos.add(existingCombo+","+param);
            }
            if (existingCombos.isEmpty())
            {
                newParamCombos.add(param);
            }
        }

        return newParamCombos;
    }

    private List<GeneratedFileBounds> buildBoundsList(ParamMap dataAccessJobParams, ImageCube imageCube)
    {
        List<GeneratedFileBounds> boundsList = new ArrayList<>();
        // Could have multiple, each entry produces a separate image cube
        if (dataAccessJobParams.get("pos") != null)
        {
            for (String posParam : dataAccessJobParams.get("pos"))
            {
                GeneratedFileBounds bounds = calcPosGeneratedFileBounds(posParam);
                if (bounds != null && overlapsImage(bounds, imageCube))
                {
                    bounds.setParams("POS="+posParam);
                    boundsList.add(bounds);
                }
            }
            
        }
        
        if (dataAccessJobParams.get("circle") != null)
        {
            for (String circParam : dataAccessJobParams.get("circle"))
            {
                GeneratedFileBounds bounds = calcCircleBounds(circParam);
                if (bounds != null && overlapsImage(bounds, imageCube))
                {
                    bounds.setParams("CIRCLE="+circParam);
                    boundsList.add(bounds);
                }
            } 
        }
        
        if (dataAccessJobParams.get("polygon") != null)
        {
            for (String polyParam : dataAccessJobParams.get("polygon"))
            {
                GeneratedFileBounds bounds = calcPolygonBounds(polyParam);
                if (bounds != null && overlapsImage(bounds, imageCube))
                {
                    bounds.setParams("POLYGON="+polyParam);
                    boundsList.add(bounds);
                }
            } 
        }
        return boundsList;
    }

    /**
     * Calculate the bounds of a generated file which would enclose the supplied POS value. Note: this does not 
     * currently cope with polygons which overlap the celestial pole.
     * 
     * @param posParam
     *            The already validated pos string defining the region required in the generated file.
     * @return The bounds value which will enclose the region.
     */
    GeneratedFileBounds calcPosGeneratedFileBounds(String posParam)
    {
        if (posParam.startsWith("CIRCLE"))
        {
            return calcCircleBounds(posParam.replace("CIRCLE ", ""));
        }
        else if (posParam.startsWith("RANGE"))
        {
            return calcRangeBounds(posParam);
        }
        else if (posParam.startsWith("POLYGON"))
        {
            return calcPolygonBounds(posParam.replace("POLYGON ", ""));
        }

        return null;
    }

    /**
     * Calculate the bounds of a generated file which would enclose the supplied Circle value. 
     * 
     * @param circParam
     *            The already validated circle string defining the region required in the generated file.
     * @return The bounds value which will enclose the region.
     */
    GeneratedFileBounds calcCircleBounds(String circParam)
    {
        final int raIndex = 0;
        final int decIndex = 1;
        final int radiusIndex = 2;
        String[] criterionParts = circParam.split(" +");

        double radius = Double.parseDouble(criterionParts[radiusIndex]);
        double length = radius * 2d;
        GeneratedFileBounds bounds = new GeneratedFileBounds(criterionParts[raIndex], criterionParts[decIndex], length);
        return bounds;
    }

    private GeneratedFileBounds calcRangeBounds(String posParam)
    {
        final int firstRaIndex = 1;
        final int firstDecIndex = 3;
        String[] criterionParts = posParam.split(" +");

        // Need to replace missing values with the default bounds
        String[] defaultCoords = new String[] { "0", "360.0", "-90", "+90.0" };
        for (int i = 0; i < defaultCoords.length; i++)
        {
            if (PositionParamProcessor.NEGATIVE_INFINTY.equals(criterionParts[i + 1])
                    || PositionParamProcessor.POSITIVE_INFINTY.equals(criterionParts[i + 1]))
            {
                criterionParts[i + 1] = defaultCoords[i];
            }
        }
        BigDecimal firstRa = new BigDecimal(criterionParts[firstRaIndex]);
        BigDecimal secondRa = new BigDecimal(criterionParts[firstRaIndex + 1]);
        BigDecimal firstDec = new BigDecimal(criterionParts[firstDecIndex]);
        BigDecimal secondDec = new BigDecimal(criterionParts[firstDecIndex + 1]);

        return calcBoundsForRange(firstRa, secondRa, firstDec, secondDec);
    }

    private GeneratedFileBounds calcBoundsForRange(BigDecimal firstRa, BigDecimal secondRa, BigDecimal firstDec,
            BigDecimal secondDec)
    {
        final int halfDivisor = 2;
        final int coordPrecision = 6;
        BigDecimal centreRa =
                firstRa.add(secondRa).divide(new BigDecimal(halfDivisor), coordPrecision, RoundingMode.HALF_UP);
        BigDecimal widthRa = firstRa.subtract(secondRa).abs();

        BigDecimal centreDec =
                firstDec.add(secondDec).divide(new BigDecimal(halfDivisor), coordPrecision, RoundingMode.HALF_UP);
        BigDecimal heightDec = firstDec.subtract(secondDec).abs();
        GeneratedFileBounds bounds = new GeneratedFileBounds(centreRa, centreDec, widthRa, heightDec);
        return bounds;
    }

    /**
     * Calculate the bounds of a generated file which would enclose the supplied Polygon value. 
     * 
     * @param polyParam
     *            The already validated polygon string defining the region required in the generated file.
     * @return The bounds value which will enclose the region.
     */
    GeneratedFileBounds calcPolygonBounds(String polyParam)
    {
        String[] criterionParts = polyParam.split(" +");
        // Calculate extremes of polygon and treat as range
        BigDecimal firstRa = new BigDecimal("1000");
        BigDecimal secondRa = new BigDecimal("-1000");
        BigDecimal firstDec = new BigDecimal("1000");
        BigDecimal secondDec = new BigDecimal("-1000");

        for (int i = 0; i < criterionParts.length; i += 2)
        {
            BigDecimal pointRa = new BigDecimal(criterionParts[i]);
            BigDecimal pointDec = new BigDecimal(criterionParts[i + 1]);

            firstRa = pointRa.min(firstRa);
            secondRa = pointRa.max(secondRa);
            firstDec = pointDec.min(firstDec);
            secondDec = pointDec.max(secondDec);
        }

        return calcBoundsForRange(firstRa, secondRa, firstDec, secondDec);
    }

    /**
     * Retrieve a list of non-spatial axes from the provided image cube. If the image is two dimensional, an empty list
     * is returned.
     * 
     * @param image
     *            The image cube to be examined.
     * @return A list of axes.
     */
    public List<ImageCubeAxis> getAxisList(ImageCube image)
    {
        List<ImageCubeAxis> axisList = new ArrayList<>();
        try
        {
            if (image.getDimensionsJson() == null)
            {
                return axisList;
            }

            JsonNode axesNode = image.getDimensionsJson().get("axes");
            for (int i = 0; i < axesNode.size(); i++)
            {
                JsonNode axisNode = axesNode.get(i);
                String name = axisNode.get("name").asText();
                if (name != null && !name.startsWith("RA") && !name.startsWith("DEC"))
                {
                    ImageCubeAxis icAxis = new ImageCubeAxis(i + 1, name, axisNode.get("numPixels").asInt(),
                            axisNode.get("min").asDouble(), axisNode.get("max").asDouble(),
                            axisNode.get("pixelSize").asDouble());
                    axisList.add(icAxis);
                }
            }

            // Build up the number of planes each step of each axis spans.
            // We have to work from the inner (i.e. later) axis outwards
            int planeSpan = 1;
            for (int i = axisList.size() - 1; i >= 0; i--)
            {
                ImageCubeAxis icAxis = axisList.get(i);
                icAxis.setPlaneSpan(planeSpan);
                planeSpan *= icAxis.getSize();
            }

        }
        catch (IOException e)
        {
            throw new IllegalStateException(String.format("Invalid json dimension string for image %s", image), e);
        }

        return axisList;
    }

    private List<int[]> convertValuesToAxisPixelRange(ImageCubeAxis axis, List<ValueRange> axisCriteria,
            List<String> orderedParams, List<String> pixelRangeAxisCriteria)
    {
        List<int[]> pixelRanges = new ArrayList<>();
        int i = 0;
        for (ValueRange valueRange : axisCriteria)
        {
            int[] axisPixelRange = calculateAxisOverlap(axis, valueRange);
            if (axisPixelRange != null)
            {
                pixelRanges.add(axisPixelRange);
                pixelRangeAxisCriteria.add(orderedParams.get(i));
            }
            i++;
        }
        if (axisCriteria.isEmpty())
        {
            pixelRanges.add(new int[]{1, axis.getSize()});
        }
        return pixelRanges;
    }
    
    /**
     * Retrieve a list of plane ranges (min and max planes on each axis) which describe the planes which should be
     * extracted from an image cube with the supplied axisList to satisfy the criteria specified in the supplied
     * paramMap.
     * 
     * @param paramMap
     *            The map of generated file criteria.
     * @param nonSpatialAxisList
     *            The list of non-spatial axes in the image cube.
     * @return A list of range to be retrieved, each described in a GeneratedFileBounds object
     */
    List<GeneratedFileBounds> getDimensionSets(ParamMap paramMap, List<ImageCubeAxis> nonSpatialAxisList)
    {
        // Single empty bounds indicates all of the image - no generated files of extra dimensions, only ra/dec
        List<GeneratedFileBounds> planeBounds = new ArrayList<>();
        if (CollectionUtils.isEmpty(nonSpatialAxisList))
        {
            if (!hasDimensionCriteria(paramMap))
            {
                planeBounds.add(new GeneratedFileBounds());
            }
            return planeBounds;
        }

        if (!hasDimensionCriteria(paramMap))
        {
            return createAllPlaneBounds(nonSpatialAxisList);
        }

        // Build up list of ranges
        List<List<int[]>> axisRangeList = new ArrayList<>();
        List<List<String>> axisRangeParamList = new ArrayList<>();
        for (ImageCubeAxis axis : nonSpatialAxisList)
        {
            List<String> orderedParams = new ArrayList<>();
            List<ValueRange> axisCriteria = getAxisCriteria(axis, paramMap, orderedParams);
            List<String> pixelRangeAxisParam = new ArrayList<>();
            List<int[]> axisPixelRanges =
                    convertValuesToAxisPixelRange(axis, axisCriteria, orderedParams, pixelRangeAxisParam);
            axisRangeList.add(axisPixelRanges);
            axisRangeParamList.add(pixelRangeAxisParam);
        }
        
        // For each range combo, create bounds, set num planes
        List<List<String>> rangeComboParams = new ArrayList<>();
        List<List<int[]>> rangeCombos = buildRangeCombos(axisRangeList, axisRangeParamList, rangeComboParams); 
        for (int i = 0; i < rangeCombos.size(); i++)
        {
            List<int[]> combo = rangeCombos.get(i);
            List<String> comboParams = rangeComboParams.get(i);

            int index = 0;
            int totalPlanes = 1;
            GeneratedFileBounds bounds = new GeneratedFileBounds();
            for (Iterator<int[]> iterator = combo.iterator(); iterator.hasNext();)
            {
                int[] is = iterator.next();
                bounds.setDimBounds(index, getRange(is));
                if (StringUtils.isNotEmpty(comboParams.get(index)))
                {
                    String prefix =  StringUtils.isEmpty(bounds.getParams()) ? "" : bounds.getParams() + ",";
                    bounds.setParams(prefix + comboParams.get(index));
                }
                totalPlanes *= (is[1] - is[0] + 1);
                index++;
            }
            bounds.setNumPlanes(totalPlanes);
            planeBounds.add(bounds);
        }

        return planeBounds;
    }
    
    private List<List<int[]>> buildRangeCombos(List<List<int[]>> axisRangeList, List<List<String>> axisRangeParamList,
            List<List<String>> rangeComboParams)
    {
        return buildRangeCombos(axisRangeList, axisRangeParamList, rangeComboParams, 0);
    }

    private List<List<int[]>> buildRangeCombos(List<List<int[]>> axisRangeList, List<List<String>> axisRangeParamList,
            List<List<String>> rangeComboParams, int depth)
    {
        List<List<int[]>> combos = new ArrayList<>();
        
        // Deal with the innermost axis - just build lists of the ranges and parameter values 
        if (depth == axisRangeList.size() - 1)
        {
            for (int i = 0; i < axisRangeList.get(depth).size(); i++)
            {
                int[] range = axisRangeList.get(depth).get(i);
                List<String> paramList = axisRangeParamList.get(depth);
                String param = paramList.size() > i ? paramList.get(i) : "";

                List<int[]> newCombo = new ArrayList<>();
                newCombo.add(range);
                combos.add(newCombo);

                List<String> newComboParam = new ArrayList<>();
                newComboParam.add(param);
                rangeComboParams.add(newComboParam);
            }
            return combos;
        }

        // For outer axes we need to recursively grab the ranges and parameters for the inner axes 
        List<List<String>> innerParams = new ArrayList<>();
        List<List<int[]>> innerCombos = buildRangeCombos(axisRangeList, axisRangeParamList, innerParams, depth + 1);

        // Then add the ranges and parameters for this axis at the front of the respective lists 
        for (int i = 0; i < axisRangeList.get(depth).size(); i++)
        {
            int[] range = axisRangeList.get(depth).get(i);
            List<String> paramList = axisRangeParamList.get(depth);
            String rangeParam = paramList.size() > i ? paramList.get(i) : "";

            for (int j = 0; j < innerCombos.size(); j++)
            {
                List<int[]> inner = innerCombos.get(j);
                List<String> innerParam = innerParams.get(j);

                List<int[]> newCombo = new ArrayList<>();
                newCombo.add(range);
                newCombo.addAll(inner);
                combos.add(newCombo);

                List<String> newComboParam = new ArrayList<>();
                newComboParam.add(rangeParam);
                newComboParam.addAll(innerParam);
                rangeComboParams.add(newComboParam);
            }
        }
        return combos;
    }

    private String getRange(int[] is)
    {
        String range;
        if (is[0] == is[1])
        {
            range = String.valueOf(is[0]);
        }
        else
        {
            range = String.valueOf(is[0])+":"+String.valueOf(is[1]);
        }
        return range;
    }

    private List<GeneratedFileBounds> createAllPlaneBounds(List<ImageCubeAxis> nonSpatialAxisList)
    {
        List<GeneratedFileBounds> planeBounds = new ArrayList<>();
        ImageCubeAxis firstAxis = nonSpatialAxisList.get(0);
        GeneratedFileBounds bounds = new GeneratedFileBounds();
        bounds.setNumPlanes(firstAxis.getSize() * firstAxis.getPlaneSpan());
        planeBounds.add(bounds);
        return planeBounds;
    }

    
    /**
     * Calculate the pixel range from the axis that is covered by the range.
     * 
     * @param axis
     *            The axis definition.
     * @param valueRange
     *            The range of values to be matched on the axis.
     * @return Either null if there is no overlap, or an int array containing two values, the minimum and maximum pixels
     *         of the overlapping range.
     */
    int[] calculateAxisOverlap(ImageCubeAxis axis, ValueRange valueRange)
    {
        // note: FITS pixels are 1 indexed.
        BigDecimal axisMinValue = new BigDecimal(axis.getMinVal());
        BigDecimal axisDelta = new BigDecimal(axis.getDelta());
        BigDecimal minPixelCalc =
                (new BigDecimal(valueRange.getMinValue()).subtract(axisMinValue)).divide(axisDelta, RoundingMode.FLOOR);
        BigDecimal maxPixelCalc =
                (new BigDecimal(valueRange.getMaxValue()).subtract(axisMinValue)).divide(axisDelta, RoundingMode.FLOOR);
        int minPixel = 1 + Math.max(0, minPixelCalc.intValue());
        int maxPixel = axis.getSize();
        if (maxPixelCalc.compareTo(new BigDecimal(axis.getSize()-1)) <= 0)
        {
            maxPixel = 1 + maxPixelCalc.intValue();    
        }
        
        // a criteria that does not overlap the axis will have a max < min
        if (maxPixel >= minPixel)
        {
            return new int[] { minPixel, maxPixel };
        }
        return null;
    }

    /**
     * Extract the criteria that apply to the provided axis.
     * 
     * @param imageCubeAxis
     *            The axis to be matched
     * @param paramMap
     *            The full map of parameters.
     * @param orderedParams
     *            List to be populated with the full parameters in the order of the returned criteria 
     * @return A list of criteria that match.
     */
    private List<ValueRange> getAxisCriteria(ImageCubeAxis imageCubeAxis, ParamMap paramMap, List<String> orderedParams)
    {
        List<ValueRange> criteria = new ArrayList<>();
        switch (imageCubeAxis.getName().toLowerCase())
        {
        case "freq":
            String[] bandParams = paramMap.get(ParamKeyWhitelist.BAND.name());
            if (ArrayUtils.isNotEmpty(bandParams))
            {
                for (String value : bandParams)
                {
                    orderedParams.add("BAND="+value);
                }
                criteria.addAll(BandParamProcessor.getFreqRanges(bandParams));
            }
            break;

        case "stokes":
            String[] polParams = paramMap.get(ParamKeyWhitelist.POL.name());
            if (ArrayUtils.isNotEmpty(polParams))
            {
                criteria.addAll(PolParamProcessor.getPolRanges(polParams));
                orderedParams.addAll(PolParamProcessor.getPolParams(polParams));
            }
            break;

        default:
            break;
        }

        // Check for matching coord
        List<ValueRange> rangesForAxis = CoordParamProcessor.getRangesForAxis(paramMap.get(ParamKeyWhitelist.COORD.name()),
                imageCubeAxis.getName(), orderedParams);
        criteria.addAll(rangesForAxis);
        return criteria;
    }

    private boolean hasDimensionCriteria(ParamMap paramMap)
    {
        for (ParamKeyWhitelist entry : EnumSet.complementOf(EnumSet.of(ParamMap.ParamKeyWhitelist.POS,
                ParamMap.ParamKeyWhitelist.ID, ParamMap.ParamKeyWhitelist.FORMAT)))
        {
            String[] paramArray = paramMap.get(entry.name());
            if (paramArray != null)
            {
                for (String param : paramArray)
                {
                    if (StringUtils.isNotBlank(param))
                    {
                        return true;
                    }
                    
                }
            }
        }

        return false;
    }

    private boolean overlapsImage(GeneratedFileBounds bounds, ImageCube imageCube)
    {
        String query = sregionContainsQuery.replace("<centre_ra_rad>", radianString(bounds.getRa()))
                .replace("<centre_dec_rad>", radianString(bounds.getDec()))
                .replace("<image_cube_id>", String.valueOf(imageCube.getId()));
        
       String result = jdbcTemplate.queryForObject(query, String.class);
       return "t".equalsIgnoreCase(result);
    }

    private String radianString(String degreesValue)
    {
        return String.valueOf(Math.toRadians(Double.parseDouble(degreesValue)));
    }
}
