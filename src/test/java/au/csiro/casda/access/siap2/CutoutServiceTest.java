package au.csiro.casda.access.siap2;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import au.csiro.casda.access.soda.ImageCubeAxis;
import au.csiro.casda.access.soda.ValueRange;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.observation.ImageCube;

/**
 * Verify the functionality of the CutoutService class.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class CutoutServiceTest
{

    /**
     * Check the calcCutoutBounds method's handling of various POS values.
     */
    @RunWith(Parameterized.class)
    public static class CalcCutoutBoundsTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays
                    .asList(new Object[][] { { "CIRCLE 12.0 34.0 0.5", new CutoutBounds("12.0", "34.0", "1.0", null) },
                            { "CIRCLE +182.5 -34.0 7.5", new CutoutBounds("+182.5", "-34.0", "15.0", null) },
                            { "RANGE 12.0 12.5 34.0 36.0", new CutoutBounds("12.250000", "35.000000", "0.5", "2.0") },
                            { "RANGE 0 360.0 -2.0 2.0", new CutoutBounds("180.000000", "0.000000", "360.0", "4.0") },
                            { "RANGE 0 360.0 89.0 +Inf", new CutoutBounds("180.000000", "89.500000", "360.0", "1.0") },
                            { "RANGE -Inf +Inf -Inf +Inf", new CutoutBounds("180.000000", "0.000000", "360.0", "180.0") },
                            { "POLYGON 12.0 34.0 14.0 35.0 14.0 36.0 12.0 35.0",
                                    new CutoutBounds("13.000000", "35.000000", "2.0", "2.0") },
                    { "POLYGON 112.0 34.0 118.0 36 118.0 -10.0 112.0 -10.0 89.0 0",
                            new CutoutBounds("103.500000", "13.000000", "29.0", "46.0") } });
        }

        private CutoutService cutoutService;
        private String posValue;
        private CutoutBounds bounds;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcCutoutBoundsTest(String posValue, CutoutBounds bounds) throws Exception
        {
            this.posValue = posValue;
            this.bounds = bounds;
            cutoutService = new CutoutService("", jdbcTemplate);
        }

        @Test
        public void testCalcCutoutBounds()
        {
            assertThat("Incorrect bounds for '" + posValue + "'.", cutoutService.calcPosCutoutBounds(posValue),
                    is(bounds));
        }
    }

    /**
     * Check the calcCutoutBounds method's handling of multiple POS values.
     */
    public static class CalcCutoutBoundsMultipleTest
    {

        private CutoutService cutoutService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcCutoutBoundsMultipleTest() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            cutoutService = new CutoutService("", jdbcTemplate);
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcCutoutBounds2d() throws IOException
        {
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");
            String[] values = { "CIRCLE 12.0 34.0 0.5", "CIRCLE 0.0 -90.0 4.2", "RANGE 12.0 12.5 34.0 36.0" };
            CutoutBounds[] expected = new CutoutBounds[] { new CutoutBounds("12.0", "34.0", "1.0", null),
                    new CutoutBounds("0.0", "-90.0", "8.4", null),
                    new CutoutBounds("12.250000", "35.000000", "0.5", "2.0") };

            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("pos", values);
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat("Incorrect bounds for '" + StringUtils.arrayToCommaDelimitedString(values) + "'.",
                    calcCutoutBounds, containsInAnyOrder(expected));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcCutoutBounds2dNoOverlap() throws IOException
        {
            // Force all overlap queries to return no overlap
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("f");
            String[] values = { "CIRCLE 12.0 34.0 0.5", "CIRCLE 0.0 -90.0 4.2", "RANGE 12.0 12.5 34.0 36.0" };
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("pos", values);
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds, is(empty()));
        }

        @Test
        public void testCalcCutoutBounds2dNoPosNoDim() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds, is(empty()));
        }

        @Test
        public void testCalcCutoutBoundsNoPosWithDimNoAxes() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds, is(empty()));
        }

        @Test
        public void testCalcCutoutBoundsNoPosWithDim() throws IOException
        {
            CutoutBounds expected = new CutoutBounds("0.5", "0.5", 360.0);
            expected.setMinPlane(5);
            expected.setMaxPlane(12);
            expected.setDimBounds(0, "2:3");
            expected.setDimBounds(1, "1:4");
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds.get(0), is(expected));
            assertThat(calcCutoutBounds.size(), is(1));
        }

        @Test
        public void testCalcCutoutBoundsNoPosWithPol() throws IOException
        {
            CutoutBounds expected = new CutoutBounds("0.5", "0.5", 360.0);
            expected.setDimBounds(0, "1:3");
            expected.setDimBounds(1, "1");
            expected.setNumPlanes(3);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds.get(0), is(expected));
            assertThat(calcCutoutBounds.size(), is(1));
        }

        @Test
        public void testCalcCutoutBoundsSinglePlaneWithPol() throws IOException
        {
            CutoutBounds expected = new CutoutBounds("0.5", "0.5", 360.0);
            expected.setNumPlanes(1);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_plane.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds.get(0), is(expected));
            assertThat(calcCutoutBounds.size(), is(1));
        }

        @Test
        public void testCalcCutoutBoundsSingleFreqMultiPol() throws IOException
        {
            CutoutBounds expected = new CutoutBounds("0.5", "0.5", 360.0);
            expected.setDimBounds(0, "1");
            expected.setDimBounds(1, "1");
            expected.setNumPlanes(1);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_freq.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<CutoutBounds> calcCutoutBounds = cutoutService.calcCutoutBounds(paramMap, image);
            assertThat(calcCutoutBounds.get(0), is(expected));
            assertThat(calcCutoutBounds.size(), is(1));
        }
        
        
    }

    /**
     * Check the getAxisList method.
     */
    public static class GetAxisListTest
    {

        private CutoutService cutoutService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public GetAxisListTest() throws Exception
        {
            cutoutService = new CutoutService("", jdbcTemplate);
        }

        @Test
        public void testGetAxisList() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");

            List<ImageCubeAxis> axisList = cutoutService.getAxisList(image);

            ImageCubeAxis axis = axisList.get(0);
            assertThat(axis.getName(), is("FREQ"));
            assertThat(axis.getDelta(), closeTo(5.0e7, 1));
            assertThat(axis.getMinVal(), closeTo(7.75e8, 1));
            assertThat(axis.getMaxVal(), closeTo(9.25e8, 1));
            assertThat(axis.getPlaneSpan(), is(4));
            assertThat(axis.getSize(), is(3));

            axis = axisList.get(1);
            assertThat(axis.getName(), is("STOKES"));
            assertThat(axis.getDelta(), closeTo(1, 1E-6));
            assertThat(axis.getMinVal(), closeTo(0.5, 1E-6));
            assertThat(axis.getMaxVal(), closeTo(4.5, 1E-6));
            assertThat(axis.getPlaneSpan(), is(1));
            assertThat(axis.getSize(), is(4));

            assertThat(axisList.size(), is(2));
        }

    }

    /**
     * Check the getDimensionSets method.
     */
    public static class GetDimensionSetsTest
    {

        private CutoutService cutoutService;
        private ImageCube fourDimImage;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public GetDimensionSetsTest() throws Exception
        {
            cutoutService = new CutoutService("", jdbcTemplate);
            fourDimImage = new ImageCube();
            String sampleWcsLibOutputJson =
                    FileUtils.readFileToString(new File("src/test/resources/soda/image_geometry.freq_stokes.json"));
            fourDimImage.setDimensions(sampleWcsLibOutputJson);
        }

        @Test
        public void testGetAllDimensions() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("POS", "CIRCLE 1 2 3");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is(nullValue()));
            assertThat(cutoutBounds.getDimBounds(1), is(nullValue()));
            assertThat(cutoutBounds.getNumPlanes(), is(12));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetAllDimensionsWithCriteria() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.375"); // 800 - 900 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("1:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(cutoutBounds.getNumPlanes(), is(12));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSingleRange() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSinglePixel() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529 0.3529"); // 850 - 850 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSinglePixelOneVal() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsOpenRange() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "-Inf 0.3529"); // 850 + MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsOpenRangeMax() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529 +Inf"); // 0-850 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("1:2"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultipleRanges() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            paramMap.add("BAND", "0.375 0.375"); // 800 - 800 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("1"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            cutoutBounds = dimSets.get(1);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(2));
        }

        @Test
        public void testGetDimensionsNoMatch() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3 0.31579"); // 950 - 1000 MHz
            paramMap.add("BAND", "0.4"); // 750 - 750 MHz
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            assertThat(dimSets, is(empty()));
        }

        @Test
        public void testGetDimensionsMultiAxisOneBandOnePol() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            paramMap.add("POL", "Q");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("2"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultiAxisOneBandTwoPol() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            paramMap.add("POL", "Q");
            paramMap.add("POL", "U");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("2:3"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetPlaneSetsMultiAxisOneBandTwoPolSep() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            paramMap.add("POL", "I");
            paramMap.add("POL", "U");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("1"));
            cutoutBounds = dimSets.get(1);
            assertThat(cutoutBounds.getDimBounds(0), is("2"));
            assertThat(cutoutBounds.getDimBounds(1), is("3"));
            assertThat(dimSets.size(), is(2));
        }

        @Test
        public void testGetDimensionsMultiAxisTwoBandAllPol() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            paramMap.add("POL", "Q");
            paramMap.add("POL", "U");
            paramMap.add("POL", "I");
            paramMap.add("POL", "V");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultiAxisTwoBandTwoPolSep() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            paramMap.add("POL", "I");
            paramMap.add("POL", "V");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1"));
            cutoutBounds = dimSets.get(1);
            assertThat(cutoutBounds.getDimBounds(0), is("2:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("4"));
            assertThat(dimSets.size(), is(2));
        }

        @Test
        public void testGetDimensionsMultiAxisInnerCriteriaOnly() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("POL", "Q");
            paramMap.add("POL", "U");
            paramMap.add("POL", "I");
            paramMap.add("POL", "V");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("1:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("1:4"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultiAxisPartialInnerCriteriaOnly() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("POL", "Q");
            paramMap.add("POL", "U");
            List<ImageCubeAxis> axisList = cutoutService.getAxisList(fourDimImage);

            List<CutoutBounds> dimSets = cutoutService.getDimensionSets(paramMap, axisList);
            CutoutBounds cutoutBounds = dimSets.get(0);
            assertThat(cutoutBounds.getDimBounds(0), is("1:3"));
            assertThat(cutoutBounds.getDimBounds(1), is("2:3"));
            assertThat(dimSets.size(), is(1));
        }
    }

    /**
     * Validation of the calculateAxisOverlap method, mostly boundary conditions.
     */
    public static class CalculateAxisOverlapTest
    {
        private CutoutService cutoutService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalculateAxisOverlapTest() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            cutoutService = new CutoutService("", jdbcTemplate);
        }

        @Test
        public void testSinglePixelIntegerAxis()
        {
            ImageCubeAxis singlePixelIntegerAxis = new ImageCubeAxis(0, "FREQ", 1, 100, 110, 10);
            int[] axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 100));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 100));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 104));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 105));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 106));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(106, 109));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));
        }

        @Test
        public void testMultiPixelIntegerAxis()
        {
            ImageCubeAxis multiPixelIntegerAxis = new ImageCubeAxis(0, "FREQ", 2, 100, 120, 10);
            int[] axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 104));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 105));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 106));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(105, 109));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(105, 110));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(109, 111));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(110, 114));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(118, 125));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));
        }

        @Test
        public void testMultiPixelDoubleAxis()
        {
            ImageCubeAxis multiPixelDoubleAxis = new ImageCubeAxis(0, "FREQ", 2, 100.1, 100.3, 0.1);
            int[] axisOverlap =
                    cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.1));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.14));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.15));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.16));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.19));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.19, 100.20));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.21, 100.24));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.21, 100.27));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.27, 100.29));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.29, 100.31));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.31, 100.34));
            assertThat(axisOverlap, is(nullValue()));
        }

        @Test
        public void testMultiPixelDoubleAxisOpen()
        {
            ImageCubeAxis multiPixelDoubleAxis = new ImageCubeAxis(0, "FREQ", 2, 100.1, 100.3, 0.1);

            int[] axisOverlap =
                    cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.20, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap =
                    cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap =
                    cutoutService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.31, Double.MAX_VALUE));
            assertThat(axisOverlap, is(nullValue()));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 100.19));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 100.3));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 99));
            assertThat(axisOverlap, is(nullValue()));

            axisOverlap = cutoutService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));
        }
    }

    static ImageCube createImageCubeFromJson(String jsonFileName) throws IOException
    {
        ImageCube image = new ImageCube();
        String sampleWcsLibOutputJson = FileUtils.readFileToString(new File(jsonFileName));
        image.setDimensions(sampleWcsLibOutputJson);
        return image;
    }
}
