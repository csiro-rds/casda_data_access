package au.csiro.casda.access.siap2;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

import au.csiro.casda.access.soda.ImageCubeAxis;
import au.csiro.casda.access.soda.ValueRange;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.dataaccess.ParamMap.ParamKeyWhitelist;
import au.csiro.casda.entity.observation.ImageCube;

/**
 * A service class for producing cut-outs from image cubes.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@Service
public class CutoutService
{
    private static Logger logger = LoggerFactory.getLogger(CutoutService.class);

    private String sregionContainsQuery;

    private JdbcTemplate jdbcTemplate;
    
    /**
     * Create a new instance of CutoutService
     *   
     * @param sregionContainsQuery The sql query to check if the cutout area is within the image cube. 
     * @param jdbcTemplate The template to be used for database queries.
     */
    @Autowired
    public CutoutService(@Value("${sregion.contains.query}") String sregionContainsQuery, JdbcTemplate jdbcTemplate)
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
     * @return A list of bounds values, in the same order as the supplied parameter array.
     */
    public List<CutoutBounds> calcCutoutBounds(ParamMap dataAccessJobParams, ImageCube imageCube)
    {
        List<CutoutBounds> posBoundsList = new ArrayList<>();

        // Could have multiple, each entry produces a separate image cube
        if (dataAccessJobParams.get("pos") != null)
        {
            for (String posParam : dataAccessJobParams.get("pos"))
            {
                CutoutBounds bounds = calcPosCutoutBounds(posParam);
                if (bounds != null && overlapsImage(bounds, imageCube))
                {
                    posBoundsList.add(bounds);
                }
            }
            
            if (posBoundsList.isEmpty())
            {
                // The spatial bounds do not overlap this image, so no cutout can be generated from it
                return new ArrayList<CutoutBounds>();
            }
        }

        // calculate non spatial axis ranges for each image
        List<ImageCubeAxis> axisList = getAxisList(imageCube);
        List<CutoutBounds> dimensionSets = getDimensionSets(dataAccessJobParams, axisList);

        // Set a default bounds if we have cutout params, but no POS
        if (posBoundsList.isEmpty() && hasDimensionCriteria(dataAccessJobParams))
        {
            final double largestXSize = 360.0;
            CutoutBounds bounds = new CutoutBounds(String.valueOf(imageCube.getRaDeg()),
                    String.valueOf(imageCube.getDecDeg()), largestXSize);
            posBoundsList.add(bounds);
        }

        // Cross multiply pos and plane criteria
        List<CutoutBounds> multiDimBoundsList = new ArrayList<>();
        for (CutoutBounds planeRange : dimensionSets)
        {
            String[] dimBounds = planeRange.getDimBounds();
            for (CutoutBounds posBounds : posBoundsList)
            {
                CutoutBounds bounds = new CutoutBounds(posBounds);
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
                multiDimBoundsList.add(bounds);
            }
        }

        logger.info(String.format("Applied %s to image cube-%s and produced bounds of %s.", dataAccessJobParams,
                imageCube.getId(), multiDimBoundsList));
        return multiDimBoundsList;
    }

    /**
     * Calculate the bounds of a cutout which would enclose the supplied POS value. Note: this does not currently cope
     * with polygons which overlap the celestial pole.
     * 
     * @param posParam
     *            The already validated pos string defining the region required in the cutout.
     * @return The bounds value which will enclose the region.
     */
    CutoutBounds calcPosCutoutBounds(String posParam)
    {
        if (posParam.startsWith("CIRCLE"))
        {
            return calcCircleBounds(posParam);
        }
        else if (posParam.startsWith("RANGE"))
        {
            return calcRangeBounds(posParam);
        }
        else if (posParam.startsWith("POLYGON"))
        {
            return calcPolygonBounds(posParam);
        }

        return null;
    }

    private CutoutBounds calcCircleBounds(String posParam)
    {
        final int raIndex = 1;
        final int decIndex = 2;
        final int radiusIndex = 3;
        String[] criterionParts = posParam.split(" +");

        double radius = Double.parseDouble(criterionParts[radiusIndex]);
        double length = radius * 2d;
        CutoutBounds bounds = new CutoutBounds(criterionParts[raIndex], criterionParts[decIndex], length);
        return bounds;
    }

    private CutoutBounds calcRangeBounds(String posParam)
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

    private CutoutBounds calcBoundsForRange(BigDecimal firstRa, BigDecimal secondRa, BigDecimal firstDec,
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
        CutoutBounds bounds = new CutoutBounds(centreRa, centreDec, widthRa, heightDec);
        return bounds;
    }

    private CutoutBounds calcPolygonBounds(String posParam)
    {
        String[] criterionParts = posParam.split(" +");
        // Calculate extremes of polygon and treat as range
        BigDecimal firstRa = new BigDecimal("1000");
        BigDecimal secondRa = new BigDecimal("-1000");
        BigDecimal firstDec = new BigDecimal("1000");
        BigDecimal secondDec = new BigDecimal("-1000");

        for (int i = 1; i < criterionParts.length; i += 2)
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

    private List<int[]> convertValuesToAxisPixelRange(ImageCubeAxis axis, List<ValueRange> axisCriteria)
    {
        List<int[]> pixelRanges = new ArrayList<>();
        for (ValueRange valueRange : axisCriteria)
        {
            int[] axisPixelRange = calculateAxisOverlap(axis, valueRange);
            if (axisPixelRange != null)
            {
                pixelRanges.add(axisPixelRange);
            }
        }
        if (axisCriteria.isEmpty())
        {
            pixelRanges.add(new int[]{1, axis.getSize()});
        }
        return pixelRanges;
    }
    
    /**
     * Retrieve a list of plane ranges (min and max planes on each axis) which describe the planes which should be extracted
     * from an image cube with the supplied axisList to satisfy the criteria specified in the supplied paramMap.
     * 
     * @param paramMap
     *            The map of cutout criteria.
     * @param nonSpatialAxisList
     *            The list of non-spatial axes in the image cube.
     * @return A list of range to be retrieved, each described in a CutoutBounds object 
     */
    List<CutoutBounds> getDimensionSets(ParamMap paramMap, List<ImageCubeAxis> nonSpatialAxisList)
    {
        // Single empty bounds indicates all of the image - no cutouts of extra dimensions, only ra/dec
        List<CutoutBounds> planeBounds = new ArrayList<>();
        if (CollectionUtils.isEmpty(nonSpatialAxisList))
        {
            if (!hasDimensionCriteria(paramMap))
            {
                planeBounds.add(new CutoutBounds());
            }
            return planeBounds;
        }

        if (!hasDimensionCriteria(paramMap))
        {
            return createAllPlaneBounds(nonSpatialAxisList);
        }

        // Build up list of ranges
        List<List<int[]>> axisRangeList = new ArrayList<>();
        for (ImageCubeAxis axis : nonSpatialAxisList)
        {
            List<ValueRange> axisCriteria = getAxisCriteria(axis, paramMap);
            List<int[]> axisPixelRanges = convertValuesToAxisPixelRange(axis, axisCriteria);
            axisRangeList.add(axisPixelRanges);
        }
        
        // For each range combo, create bounds, set num planes
        List<List<int[]>> rangeCombos = buildRangeCombos(axisRangeList);
        for (List<int[]> combo : rangeCombos)
        {
            int index = 0;
            int totalPlanes = 1;
            CutoutBounds bounds = new CutoutBounds();
            for (Iterator<int[]> iterator = combo.iterator(); iterator.hasNext();)
            {
                int[] is = iterator.next();
                bounds.setDimBounds(index, getRange(is));
                totalPlanes *= (is[1] - is[0] + 1);
                index++;
            }
            bounds.setNumPlanes(totalPlanes);
            planeBounds.add(bounds);
        }

        return planeBounds;
    }
    
    private List<List<int[]>> buildRangeCombos(List<List<int[]>> axisRangeList)
    {
        return buildRangeCombos(axisRangeList, 0);
    }
    
    private List<List<int[]>> buildRangeCombos(List<List<int[]>> axisRangeList, int depth)
    {
        List<List<int[]>> combos = new ArrayList<>();
        if (depth == axisRangeList.size() - 1)
        {
            for (int[] range : axisRangeList.get(depth))
            {
                List<int[]> newCombo = new ArrayList<>();
                newCombo.add(range);
                combos.add(newCombo);
            }
            return combos;
        }
        
        List<List<int[]>> innerCombos = buildRangeCombos(axisRangeList, depth+1);

        for (int[] range : axisRangeList.get(depth))
        {
            for (List<int[]> inner : innerCombos)
            {
                List<int[]> newCombo = new ArrayList<>();
                newCombo.add(range);
                newCombo.addAll(inner);
                combos.add(newCombo);
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

    private List<CutoutBounds> createAllPlaneBounds(List<ImageCubeAxis> nonSpatialAxisList)
    {
        List<CutoutBounds> planeBounds = new ArrayList<>();
        ImageCubeAxis firstAxis = nonSpatialAxisList.get(0);
        CutoutBounds bounds = new CutoutBounds();
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
     * @return A list of criteria that match.
     */
    private List<ValueRange> getAxisCriteria(ImageCubeAxis imageCubeAxis, ParamMap paramMap)
    {
        List<ValueRange> criteria = new ArrayList<>();
        switch (imageCubeAxis.getName().toLowerCase())
        {
        case "freq":
            criteria.addAll(BandParamProcessor.getFreqRanges(paramMap.get(ParamKeyWhitelist.BAND.name())));
            break;

        case "stokes":
            criteria.addAll(PolParamProcessor.getPolRanges(paramMap.get(ParamKeyWhitelist.POL.name())));
            break;

        default:
            break;
        }

        // Check for matching coord
        criteria.addAll(CoordParamProcessor.getRangesForAxis(paramMap.get(ParamKeyWhitelist.COORD.name()),
                imageCubeAxis.getName()));
        return criteria;
    }

    private boolean hasDimensionCriteria(ParamMap paramMap)
    {
        for (ParamKeyWhitelist entry : EnumSet
                .complementOf(EnumSet.of(ParamMap.ParamKeyWhitelist.POS, ParamMap.ParamKeyWhitelist.ID)))
        {
            if (ArrayUtils.isNotEmpty(paramMap.get(entry.name())))
            {
                return true;
            }
        }

        return false;
    }

    private boolean overlapsImage(CutoutBounds bounds, ImageCube imageCube)
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
