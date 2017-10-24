package au.csiro.casda.access.soda;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.ParamMap;
import au.csiro.casda.entity.observation.ImageCube;

/**
 * Verify the functionality of the GenerateFileService class.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
@RunWith(Enclosed.class)
public class GenerateFileServiceTest
{

    /**
     * Check the calcGeneratedFileBounds method's handling of various POS values.
     */
    @RunWith(Parameterized.class)
    public static class CalcGeneratedFileBoundsTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays
                    .asList(new Object[][] {
                            { "CIRCLE 12.0 34.0 0.5", new GeneratedFileBounds("12.0", "34.0", "1.000000", null) }, {
                                    "CIRCLE +182.5 -34.0 7.5",
                                    new GeneratedFileBounds("+182.5", "-34.0", "15.000000", null) },
                            { "RANGE 12.0 12.5 34.0 36.0",
                                    new GeneratedFileBounds("12.250000", "35.000000", "0.5", "2.0") },
                            { "RANGE 0 360.0 -2.0 2.0",
                                    new GeneratedFileBounds("180.000000", "0.000000", "360.0", "4.0") },
                            { "RANGE 0 360.0 89.0 +Inf",
                                    new GeneratedFileBounds("180.000000", "89.500000", "360.0", "1.0") },
                            { "RANGE -Inf +Inf -Inf +Inf",
                                    new GeneratedFileBounds("180.000000", "0.000000", "360.0", "180.0") },
                            { "POLYGON 12.0 34.0 14.0 35.0 14.0 36.0 12.0 35.0",
                                    new GeneratedFileBounds("13.000000", "35.000000", "2.0", "2.0") },
                            { "POLYGON 112.0 34.0 118.0 36 118.0 -10.0 112.0 -10.0 89.0 0",
                                    new GeneratedFileBounds("103.500000", "13.000000", "29.0", "46.0") } });
        }

        private GenerateFileService generateFileService;
        private String posValue;
        private GeneratedFileBounds bounds;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcGeneratedFileBoundsTest(String posValue, GeneratedFileBounds bounds) throws Exception
        {
            this.posValue = posValue;
            this.bounds = bounds;
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testCalcGeneratedFileBounds()
        {
            assertThat("Incorrect bounds for '" + posValue + "'.", generateFileService.calcPosGeneratedFileBounds(posValue),
                    is(bounds));
        }
    }
    
    /**
     * Check the calcGeneratedFileBounds method's handling of various POS values.
     */
    @RunWith(Parameterized.class)
    public static class CalcCircleGeneratedFileBoundsTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays
                    .asList(new Object[][] { { "12.0 34.0 0.5", new GeneratedFileBounds("12.0", "34.0", "1.000000", null) },
                            { "+182.5 -34.0 7.5", new GeneratedFileBounds("+182.5", "-34.0", "15.000000", null) }});
        }

        private GenerateFileService generateFileService;
        private String posValue;
        private GeneratedFileBounds bounds;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcCircleGeneratedFileBoundsTest(String posValue, GeneratedFileBounds bounds) throws Exception
        {
            this.posValue = posValue;
            this.bounds = bounds;
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testCalcGeneratedFileBounds()
        {
            assertThat("Incorrect bounds for '" + posValue + "'.", generateFileService.calcCircleBounds(posValue),
                    is(bounds));
        }
    }
    
    /**
     * Check the calcGeneratedFileBounds method's handling of various POS values.
     */
    @RunWith(Parameterized.class)
    public static class CalcPolygonGeneratedFileBoundsTest
    {

        @Parameters
        public static Collection<Object[]> data()
        {
            // Param values (may be multiple)
            return Arrays
                    .asList(new Object[][] {
                            { "12.0 34.0 14.0 35.0 14.0 36.0 12.0 35.0",
                                    new GeneratedFileBounds("13.000000", "35.000000", "2.0", "2.0") },
                    { "112.0 34.0 118.0 36 118.0 -10.0 112.0 -10.0 89.0 0",
                            new GeneratedFileBounds("103.500000", "13.000000", "29.0", "46.0") } });
        }

        private GenerateFileService generateFileService;
        private String posValue;
        private GeneratedFileBounds bounds;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcPolygonGeneratedFileBoundsTest(String posValue, GeneratedFileBounds bounds) throws Exception
        {
            this.posValue = posValue;
            this.bounds = bounds;
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testCalcGeneratedFileBounds()
        {
            assertThat("Incorrect bounds for '" + posValue + "'.", generateFileService.calcPolygonBounds(posValue),
                    is(bounds));
        }
    }

    /**
     * Check the calcGeneratedFileBounds method's handling of multiple POS values.
     */
    public static class CalcGeneratedFileBoundsMultipleTest
    {

        private GenerateFileService generateFileService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalcGeneratedFileBoundsMultipleTest() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcGeneratedFileBounds2d() throws IOException
        {
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");
            String[] values = { "CIRCLE 12.0 34.0 0.5", "CIRCLE 0.0 -90.0 4.2", "RANGE 12.0 12.5 34.0 36.0" };
            GeneratedFileBounds[] expected = new GeneratedFileBounds[] { 
            		new GeneratedFileBounds("12.0", "34.0", "1.000000", null),
                    new GeneratedFileBounds("0.0", "-90.0", "8.400000", null),
                    new GeneratedFileBounds("12.250000", "35.000000", "0.5", "2.0") };

            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("pos", values);
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat("Incorrect bounds for '" + StringUtils.arrayToCommaDelimitedString(values) + "'.",
                    calcGeneratedFileBounds, contains(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POS=CIRCLE 12.0 34.0 0.5"));
            assertThat(calcGeneratedFileBounds.get(1).getParams(), is("POS=CIRCLE 0.0 -90.0 4.2"));
            assertThat(calcGeneratedFileBounds.get(2).getParams(), is("POS=RANGE 12.0 12.5 34.0 36.0"));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcGeneratedFileBounds2dNoOverlap() throws IOException
        {
            // Force all overlap queries to return no overlap
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("f");
            String[] values = { "CIRCLE 12.0 34.0 0.5", "CIRCLE 0.0 -90.0 4.2", "RANGE 12.0 12.5 34.0 36.0" };
            List<String> posParams = new ArrayList<>();
            for (String val : values)
            {
                posParams.add("POS="+val);
            }
            
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("pos", values);
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, "cube-27", errorList);
            assertThat(calcGeneratedFileBounds, is(empty()));
            assertThat(errorList, not(empty()));
            for (String errorBounds : errorList)
            {
                assertThat(errorBounds, not(nullValue()));
                for (String param : errorBounds.split(","))
                {
                    assertThat(param, not(nullValue()));
                    if (param.startsWith("POS="))
                    {
                        assertThat(param, is(in(posParams)));
                    }
                    else
                    {
                        assertThat(param, is("ID=cube-27"));
                    }
                }
            }
            
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcGeneratedFileBounds2dEmptyPol() throws IOException
        {
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");
            String[] values = { "CIRCLE 12.0 34.0 0.5", "CIRCLE 0.0 -90.0 4.2", "RANGE 12.0 12.5 34.0 36.0" };
            GeneratedFileBounds[] expected = new GeneratedFileBounds[] { 
            		new GeneratedFileBounds("12.0", "34.0", "1.000000", null),
                    new GeneratedFileBounds("0.0", "-90.0", "8.400000", null),
                    new GeneratedFileBounds("12.250000", "35.000000", "0.5", "2.0") };

            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("pos", values);
            paramMap.add("pol", "");
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat("Incorrect bounds for '" + StringUtils.arrayToCommaDelimitedString(values) + "'.",
                    calcGeneratedFileBounds, contains(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POS=CIRCLE 12.0 34.0 0.5"));
            assertThat(calcGeneratedFileBounds.get(1).getParams(), is("POS=CIRCLE 0.0 -90.0 4.2"));
            assertThat(calcGeneratedFileBounds.get(2).getParams(), is("POS=RANGE 12.0 12.5 34.0 36.0"));
        }

        @Test
        public void testCalcGeneratedFileBounds2dNoPosNoDim() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds, is(empty()));
        }

        @Test
        public void testCalcGeneratedFileBoundsNoPosWithDimNoAxes() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.2d.json");
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds, is(empty()));
        }

        @Test
        public void testCalcGeneratedFileBoundsNoPosWithDim() throws IOException
        {
            GeneratedFileBounds expected = new GeneratedFileBounds("0.5", "0.5", 360.0);
            expected.setMinPlane(5);
            expected.setMaxPlane(12);
            expected.setDimBounds(0, "2:3");
            expected.setDimBounds(1, "1:4");
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds.get(0), is(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("BAND=0.3333 0.3529"));
            assertThat(calcGeneratedFileBounds.size(), is(1));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcGeneratedFileBoundsWithPosWithDim() throws IOException
        {
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("t");
            
            GeneratedFileBounds expected = new GeneratedFileBounds("320.0", "54.0", 1.0);
            expected.setMinPlane(5);
            expected.setMaxPlane(12);
            expected.setDimBounds(0, "1");
            expected.setDimBounds(1, "2");
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_freq.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "Q"); 
            paramMap.add("POS", "CIRCLE 320.0 54.0 0.5");
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds.get(0), is(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POS=CIRCLE 320.0 54.0 0.5,POL=Q"));
            assertThat(calcGeneratedFileBounds.size(), is(1));
        }

        @SuppressWarnings("unchecked")
        @Test
        public void testCalcGeneratedFileBoundsWithPosNoOverlapWithDim() throws IOException
        {
            // Configure a no overlap condition
            when(jdbcTemplate.queryForObject(anyString(), any(Class.class))).thenReturn("f");
            
            GeneratedFileBounds expected = new GeneratedFileBounds("320.0", "54.0", 1.0);
            expected.setMinPlane(5);
            expected.setMaxPlane(12);
            expected.setDimBounds(0, "1");
            expected.setDimBounds(1, "2");
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_freq.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "Q"); 
            paramMap.add("POS", "CIRCLE 320.0 54.0 0.5");
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, "cube-42", errorList);
            assertThat(calcGeneratedFileBounds, is(empty()));
            assertThat(errorList, not(empty()));
            assertThat(errorList.get(0), is("POS=CIRCLE 320.0 54.0 0.5,POL=Q,ID=cube-42"));
        }

        @Test
        public void testCalcGeneratedFileBoundsNoPosWithPol() throws IOException
        {
            GeneratedFileBounds expected = new GeneratedFileBounds("0.5", "0.5", 360.0);
            expected.setDimBounds(0, "1:3");
            expected.setDimBounds(1, "1");
            expected.setNumPlanes(3);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds.get(0), is(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POL=I"));
            assertThat(calcGeneratedFileBounds.size(), is(1));
        }

        @Test
        public void testCalcGeneratedFileBoundsSinglePlaneWithPol() throws IOException
        {
            GeneratedFileBounds expected = new GeneratedFileBounds("0.5", "0.5", 360.0);
            expected.setNumPlanes(1);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_plane.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds.get(0), is(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POL=I"));
            assertThat(calcGeneratedFileBounds.size(), is(1));
        }

        @Test
        public void testCalcGeneratedFileBoundsSingleFreqMultiPol() throws IOException
        {
            GeneratedFileBounds expected = new GeneratedFileBounds("0.5", "0.5", 360.0);
            expected.setDimBounds(0, "1");
            expected.setDimBounds(1, "1");
            expected.setNumPlanes(1);
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.single_freq.json");
            image.setRaDeg(0.5d);
            image.setDecDeg(0.5d);
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I"); 
            List<String> errorList = new ArrayList<>();
            List<GeneratedFileBounds> calcGeneratedFileBounds = 
            		generateFileService.calcGeneratedFileBounds(paramMap, image, null, errorList);
            assertThat(calcGeneratedFileBounds.get(0), is(expected));
            assertThat(calcGeneratedFileBounds.get(0).getParams(), is("POL=I"));
            assertThat(calcGeneratedFileBounds.size(), is(1));
        }
        
        
    }

    /**
     * Check the getAxisList method.
     */
    public static class GetAxisListTest
    {

        private GenerateFileService generateFileService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public GetAxisListTest() throws Exception
        {
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testGetAxisList() throws IOException
        {
            ImageCube image = createImageCubeFromJson("src/test/resources/soda/image_geometry.freq_stokes.json");

            List<ImageCubeAxis> axisList = generateFileService.getAxisList(image);

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

        private GenerateFileService generateFileService;
        private ImageCube fourDimImage;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public GetDimensionSetsTest() throws Exception
        {
            generateFileService = new GenerateFileService("", jdbcTemplate);
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is(nullValue()));
            assertThat(generatedFileBounds.getDimBounds(1), is(nullValue()));
            assertThat(generatedFileBounds.getNumPlanes(), is(12));
            assertThat(generatedFileBounds.getParams(), is(nullValue()));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetAllDimensionsWithCriteria() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.375"); // 800 - 900 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("1:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getNumPlanes(), is(12));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.375"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSingleRange() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.3529"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSinglePixel() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529 0.3529"); // 850 - 850 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529 0.3529"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsSinglePixelOneVal() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsOpenRange() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "-Inf 0.3529"); // 850 + MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=-Inf 0.3529"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsOpenRangeMax() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529 +Inf"); // 0-850 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("1:2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529 +Inf"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultipleRanges() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3333 0.3529"); // 850 - 900 MHz
            paramMap.add("BAND", "0.375 0.375"); // 800 - 800 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.3529"));
            generatedFileBounds = dimSets.get(1);
            assertThat(generatedFileBounds.getDimBounds(0), is("1"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.375 0.375"));
            assertThat(dimSets.size(), is(2));
        }

        @Test
        public void testGetDimensionsNoMatch() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3 0.31579"); // 950 - 1000 MHz
            paramMap.add("BAND", "0.4"); // 750 - 750 MHz
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            assertThat(dimSets, is(empty()));
        }

        @Test
        public void testGetDimensionsMultiAxisOneBandOnePol() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("BAND", "0.3529"); // 850 MHz
            paramMap.add("POL", "Q");
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("2"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529,POL=Q"));
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("2:3"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529,POL=Q,POL=U"));
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529,POL=I"));
            generatedFileBounds = dimSets.get(1);
            assertThat(generatedFileBounds.getDimBounds(0), is("2"));
            assertThat(generatedFileBounds.getDimBounds(1), is("3"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3529,POL=U"));
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.3529,POL=I,POL=Q,POL=U,POL=V"));
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.3529,POL=I"));
            generatedFileBounds = dimSets.get(1);
            assertThat(generatedFileBounds.getDimBounds(0), is("2:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("4"));
            assertThat(generatedFileBounds.getParams(), is("BAND=0.3333 0.3529,POL=V"));
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
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("1:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("1:4"));
            assertThat(generatedFileBounds.getParams(), is("POL=I,POL=Q,POL=U,POL=V"));
            assertThat(dimSets.size(), is(1));
        }

        @Test
        public void testGetDimensionsMultiAxisPartialInnerCriteriaOnly() throws IOException
        {
            DataAccessJob job = new DataAccessJob();
            ParamMap paramMap = new ParamMap("", job);
            paramMap.add("POL", "Q");
            paramMap.add("POL", "U");
            List<ImageCubeAxis> axisList = generateFileService.getAxisList(fourDimImage);

            List<GeneratedFileBounds> dimSets = generateFileService.getDimensionSets(paramMap, axisList);
            GeneratedFileBounds generatedFileBounds = dimSets.get(0);
            assertThat(generatedFileBounds.getDimBounds(0), is("1:3"));
            assertThat(generatedFileBounds.getDimBounds(1), is("2:3"));
            assertThat(generatedFileBounds.getParams(), is("POL=Q,POL=U"));
            assertThat(dimSets.size(), is(1));
        }
    }

    /**
     * Validation of the calculateAxisOverlap method, mostly boundary conditions.
     */
    public static class CalculateAxisOverlapTest
    {
        private GenerateFileService generateFileService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public CalculateAxisOverlapTest() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testSinglePixelIntegerAxis()
        {
            ImageCubeAxis singlePixelIntegerAxis = new ImageCubeAxis(0, "FREQ", 1, 100, 110, 10);
            int[] axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 100));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 100));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 104));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 105));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(99, 106));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(singlePixelIntegerAxis, new ValueRange(106, 109));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));
        }

        @Test
        public void testMultiPixelIntegerAxis()
        {
            ImageCubeAxis multiPixelIntegerAxis = new ImageCubeAxis(0, "FREQ", 2, 100, 120, 10);
            int[] axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 104));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 105));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(99, 106));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(105, 109));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(105, 110));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(109, 111));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(110, 114));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelIntegerAxis, new ValueRange(118, 125));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));
        }

        @Test
        public void testMultiPixelDoubleAxis()
        {
            ImageCubeAxis multiPixelDoubleAxis = new ImageCubeAxis(0, "FREQ", 2, 100.1, 100.3, 0.1);
            int[] axisOverlap =
                    generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.1));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.14));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.15));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.16));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, 100.19));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.19, 100.20));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.21, 100.24));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.21, 100.27));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.27, 100.29));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.29, 100.31));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.31, 100.34));
            assertThat(axisOverlap, is(nullValue()));
        }

        @Test
        public void testMultiPixelDoubleAxisOpen()
        {
            ImageCubeAxis multiPixelDoubleAxis = new ImageCubeAxis(0, "FREQ", 2, 100.1, 100.3, 0.1);

            int[] axisOverlap =
                    generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.20, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 2, 2 }));

            axisOverlap =
                    generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.09, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap =
                    generateFileService.calculateAxisOverlap(multiPixelDoubleAxis, new ValueRange(100.31, Double.MAX_VALUE));
            assertThat(axisOverlap, is(nullValue()));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 100.19));
            assertThat(axisOverlap, is(new int[] { 1, 1 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 100.3));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, 99));
            assertThat(axisOverlap, is(nullValue()));

            axisOverlap = generateFileService.calculateAxisOverlap(multiPixelDoubleAxis,
                    new ValueRange(BandParamProcessor.MIN_FREQ_VALUE, Double.MAX_VALUE));
            assertThat(axisOverlap, is(new int[] { 1, 2 }));
        }
    }


    /**
     * Validation of the buildParamCombos method.
     */
    public static class BuildParamCombosTest
    {
        private GenerateFileService generateFileService;
        
        @Mock
        private JdbcTemplate jdbcTemplate;

        public BuildParamCombosTest() throws Exception
        {
            MockitoAnnotations.initMocks(this);
            generateFileService = new GenerateFileService("", jdbcTemplate);
        }

        @Test
        public void testNoParams()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, is(empty()));
        }

        @Test
        public void testSingleParam()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("POL=I"));
        }

        @Test
        public void testSingleParamInterval()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.20 0.22");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("BAND=0.20 0.22"));
        }

        @Test
        public void testSingleParamMultipleValues()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("BAND", "0.20 0.22", "0.25 0.28");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("BAND=0.20 0.22", "BAND=0.25 0.28"));
        }

        @Test
        public void testMultipleParam()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POS", "CIRCLE 1 2 3");
            paramMap.add("BAND", "0.20 0.22");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("POS=CIRCLE 1 2 3,BAND=0.20 0.22"));
        }

        @Test
        public void testPolCombine()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POL", "I", "V", "Q");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("POL=I,POL=Q","POL=V"));
        }

        @Test
        public void testPositionalCombine()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POS", "CIRCLE 1 2 3");
            paramMap.add("POLYGON", "1 2 2 2 2 3 1 3");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos, contains("POS=CIRCLE 1 2 3","POLYGON=1 2 2 2 2 3 1 3"));
        }

        @Test
        public void testPositionalCombineMultipleParams()
        {
            ParamMap paramMap = new ParamMap("", new DataAccessJob());
            paramMap.add("POS", "CIRCLE 1 2 3");
            paramMap.add("POLYGON", "1 2 2 2 2 3 1 3");
            paramMap.add("BAND", "0.20 0.22", "0.25 0.28");
            paramMap.add("POL", "I");
            List<String> paramCombos = generateFileService.buildParamCombos(paramMap);
            assertThat(paramCombos,
                    contains("POS=CIRCLE 1 2 3,BAND=0.20 0.22,POL=I", "POLYGON=1 2 2 2 2 3 1 3,BAND=0.20 0.22,POL=I",
                            "POS=CIRCLE 1 2 3,BAND=0.25 0.28,POL=I", "POLYGON=1 2 2 2 2 3 1 3,BAND=0.25 0.28,POL=I"));
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
