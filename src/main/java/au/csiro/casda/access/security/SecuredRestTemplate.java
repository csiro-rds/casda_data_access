package au.csiro.casda.access.security;
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


import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.InterceptingClientHttpRequestFactory;
import org.springframework.security.crypto.codec.Base64;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;


/**
 * 
 * RestTemplate to access secured rest services (SSL enabled)
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
@Component
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE, proxyMode = ScopedProxyMode.TARGET_CLASS)
public class SecuredRestTemplate extends RestTemplate
{
    private HttpComponentsClientHttpRequestFactory requestFactory;

    /**
     * Default Constructor
     */    
    public SecuredRestTemplate()
    {
        this("", "", true);
    }

    /**
     * Constructor
     * 
     * @param userName
     *            The username to be used for basic authentication.
     * @param password
     *            The password to be used for basic authentication.
     * @param buffer
     *            The BufferRequestBody switch to turn on/off.
     * 
     */
    public SecuredRestTemplate(String userName, String password, boolean buffer)
    {
        requestFactory = new HttpComponentsClientHttpRequestFactory();
        requestFactory.setBufferRequestBody(buffer);

        // Create https capable custom HttpClient
        CloseableHttpClient httpClient = HttpClients.custom()
                // Hostname verification is turned off in NoopHostnameVerifier so this can work on all our environments
                .setSSLHostnameVerifier(new NoopHostnameVerifier())
                // Disable auto redirect handling as we rely on redirect 'location' in various places
                .disableRedirectHandling().build();
        
        requestFactory.setHttpClient(httpClient);
        
        //If basic auth credentials are not provided don't intercept to add Authorization header
        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password))
        {
            setRequestFactory(requestFactory);
        }
        else
        {
            List<ClientHttpRequestInterceptor> interceptors = Collections
                    .<ClientHttpRequestInterceptor> singletonList(new BasicAuthorizationInterceptor(userName, password));
            setRequestFactory(new InterceptingClientHttpRequestFactory(requestFactory, interceptors));
        }  
    }

    /**
     * Set connection timeout and readTimeout
     * 
     * @param connectionTimeout
     *            The connection timeout in milliseconds.
     * @param readTimeout
     *            The read timeout in milliseconds.
     */
    public void setConnectionAndReadTimeout(int connectionTimeout, int readTimeout)
    {
        setConnectiontimeout(connectionTimeout);
        if (readTimeout > -1)
        {
            requestFactory.setReadTimeout(readTimeout);
        }
    }

    private void setConnectiontimeout(int connectionTimeout)
    {
        if (connectionTimeout > -1)
        {
            requestFactory.setConnectTimeout(connectionTimeout);
        }
    }
    
    /**
     * 
     * Intercepter class that intercepts outgoing rest calls and add Authorization header
     * 
     * <p>
     * Copyright 2015, CSIRO Australia All rights reserved.
     *
     */
    private static class BasicAuthorizationInterceptor implements ClientHttpRequestInterceptor
    {
        private final String username;
        private final String password;

        public BasicAuthorizationInterceptor(String username, String password)
        {
            this.username = username;
            this.password = password;
        }

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException
        {
            byte[] token = Base64.encode((this.username + ":" + this.password).getBytes(Charsets.UTF_8));           
            request.getHeaders().add("Authorization", "Basic " + new String(token, Charsets.UTF_8));
            return execution.execute(request, body);
        }
    }
}
