package au.csiro.casda.access.util;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.nio.charset.Charset;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Test cases for Utils.
 * <p>
 * Copyright 2015, CSIRO Australia. All rights reserved.
 */
public class UtilsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private String key = RandomStringUtils.randomAscii(16);

    @Test
    public void roundTripTest()
    {
        String data = "a";
        String encrypted = Utils.encryptAesUrlSafe(data, key);
        String decrypted = Utils.decryptAesUrlSafe(encrypted, key);
        assertThat(decrypted, equalTo(data));
    }

    @Test
    public void testEncryption()
    {
        String data = "a";
        String encrypted = Utils.encryptAesUrlSafe(data, "$C</V=aFA$o=LV46");
        assertThat(encrypted, equalTo("bsCQbncdn7XU5VFnJKRRJQ"));
    }

    @Test
    public void testDecryption()
    {
        String encrypted = "bsCQbncdn7XU5VFnJKRRJQ";
        String decrypted = Utils.decryptAesUrlSafe(encrypted, "$C</V=aFA$o=LV46");
        assertThat(decrypted, equalTo("a"));
    }

    @Test
    public void testDecryptionWithPlus8PaddedValue()
    {
        String encrypted = "bsCQbncdn7XU5VFnJKRRJY"; // same as bsCQbncdn7XU5VFnJKRRJQ + 8
        String decrypted = Utils.decryptAesUrlSafe(encrypted, "$C</V=aFA$o=LV46");
        assertThat(decrypted, equalTo("a"));
    }

    @Test
    public void testDecryptionForBogusInput()
    {
        String encrypted = Base64.encodeBase64String("foobar12".getBytes(Charset.forName("UTF-8")));

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Could not decrypt '" + encrypted + "'");
        Utils.decryptAesUrlSafe(encrypted, key);
    }
}
