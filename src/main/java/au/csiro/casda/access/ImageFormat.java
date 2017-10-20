package au.csiro.casda.access;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Possible image formats which may be output by the SODA implementation.
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
public enum ImageFormat
{
    /** The binary astronomy image format: FITS */
    FITS("fits", "application/fits", "image/fits"),

    /** Displayable image format: PNG*/
    PNG("png", "image/png");
    
    private List<String> identifiers;

    private String fileExtension;

    /**
     * Enum constructor
     * 
     * @param fileExtension
     *            the file extension (eg xml)
     * @param formats
     *            list of valid formats for this OuptutFormat 
     */
    ImageFormat(String fileExtension, String... formats)
    {
        this.fileExtension = fileExtension;
        this.identifiers = new ArrayList<>(Arrays.asList(formats));
        this.identifiers.add(fileExtension);
    }

    /**
     * @return a list of valid mime-types/short-formats that can be used to refer to this OutputFormat. @see
     *         #findMatchingFormat
     */
    public List<String> getIdentifiers()
    {
        return identifiers;
    }

    /**
     * Find a format based on its mime type or short format name.
     *
     * @param formatStr
     *            The requested format.
     * @return The matching format, or null if none match.
     */
    public static ImageFormat findMatchingFormat(String formatStr)
    {
        // case insensitive
        String formatStrLower = formatStr.toLowerCase();
        return Arrays.asList(ImageFormat.values()).stream().filter(outputFormat -> {
            return outputFormat.identifiers.contains(formatStrLower);
        }).findFirst().orElse(null);
    }

    /**
     * @return The default MIME type for the format.
     */
    public String getDefaultContentType()
    {
        return identifiers.get(0);
    }

    /**
     * @return The file extension to use for this output type.
     */
    public String getFileExtension()
    {
        return this.fileExtension;
    }
    
}