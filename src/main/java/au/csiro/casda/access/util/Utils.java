package au.csiro.casda.access.util;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

/**
 * Utilities class for Casda Data Access.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class Utils
{
    /*
     * AES is supposedly equivalent to "AES/ECB/PKCS5Padding".
     *
     * Warning: if you change to a more explicit definition of algorithm/block-mode/padding then you will at least need
     * to creation of the SecretKeySpec below to to use just the algorithm. Also, you will probably have to deal with
     * padding issues during decryption.
     */
    private static final String CRYPTO_CIPHER = "AES";
    private static final String CRYPTO_BYTE_ENCODING = "UTF-8";

    /**
     * Encrypts the given text with the provided secret
     * 
     * @param text
     *            String to be encrypted
     * @param secret
     *            The secret key to encrypt with
     * @return encrypted String
     */
    public static String encryptAesUrlSafe(String text, String secret)
    {
        if (StringUtils.isBlank(text) || StringUtils.isBlank(secret))
        {
            return "";
        }

        // Create key and cipher
        Cipher cipher;
        try
        {
            cipher = Cipher.getInstance(CRYPTO_CIPHER);
            SecretKeySpec secretKeySpec = new SecretKeySpec(secret.getBytes(CRYPTO_BYTE_ENCODING), CRYPTO_CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec);
            byte[] result = cipher.doFinal(text.getBytes(CRYPTO_BYTE_ENCODING));
            return Base64.encodeBase64URLSafeString(result);
        }
        catch (UnsupportedEncodingException e)
        {
            /*
             * CRYPTO_BYTE_ENCODING no longer supported. What the...?
             */
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            /*
             * The crypto providers (or whatever ones have been configured) do not support CRYPTO_CIPHER and its
             * required padding
             */
            throw new RuntimeException(e);
        }
        catch (InvalidKeyException e)
        {
            throw new IllegalArgumentException(
                    "Provided key is not suitable for " + CRYPTO_CIPHER + " encyption/decryption");
        }
        catch (IllegalBlockSizeException e)
        {
            /*
             * Indicates that we've not specified a padding mode in CRYPTO_CIPHER
             */
            throw new RuntimeException(e);
        }
        catch (BadPaddingException e) // also cover AEADBadTagException
        {
            /*
             * Shouldn't happen during encryption.
             */
            throw new RuntimeException(e);
        }
    }

    /**
     * Tries to decrypt the given encrypted text with the provided secret
     * 
     * @param encryptedString
     *            the encrypted String
     * @param secret
     *            The secret key to decrypt with
     * @return decrypted Strings
     */
    public static String decryptAesUrlSafe(String encryptedString, String secret)
    {
        if (StringUtils.isBlank(encryptedString) || StringUtils.isBlank(secret))
        {
            return "";
        }
        try
        {
            byte[] keyBytes = secret.getBytes(CRYPTO_BYTE_ENCODING);
            Cipher cipher = Cipher.getInstance(CRYPTO_CIPHER);
            SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, CRYPTO_CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec);
            byte[] dataBytes = Base64.decodeBase64(encryptedString);
            byte[] result = cipher.doFinal(dataBytes);
            return new String(result, CRYPTO_BYTE_ENCODING);
        }
        catch (UnsupportedEncodingException e)
        {
            /*
             * CRYPTO_BYTE_ENCODING no longer supported. What the...?
             */
            throw new RuntimeException(e);
        }
        catch (NoSuchAlgorithmException | NoSuchPaddingException e)
        {
            /*
             * The crypto providers (or whatever ones have been configured) do not support CRYPTO_CIPHER and its
             * required padding
             */
            throw new RuntimeException(e);
        }
        catch (InvalidKeyException e)
        {
            throw new IllegalArgumentException(
                    "Provided key is not suitable for " + CRYPTO_CIPHER + " encyption/decryption");
        }
        catch (IllegalBlockSizeException e)
        {
            /*
             * Based on our crypto mode, this indicates the input data is unsuitable for decrypting (ie: it wasn't
             * produced with our crypto function).
             */
            throw new IllegalArgumentException("Could not decrypt '" + encryptedString + "'");
        }
        catch (BadPaddingException e) // also covers AEADBadTagException
        {
            /*
             * Indicates we've configured the crypto to use unpadding during decryption and the data hasn't been
             * properly padded.
             */
            throw new RuntimeException(e);
        }

    }
    
    /**
     * Returns ISO8601 representation of the date
     * 
     * @param date
     *            The date to be formated
     * @return The formated String representation of the date.
     */
    public static String formatDateToISO8601(Date date)
    {
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(date.getTime()));
    }

}
