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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mangofactory.swagger.configuration.SpringSwaggerConfig;
import com.mangofactory.swagger.models.dto.ApiInfo;
import com.mangofactory.swagger.plugin.EnableSwagger;
import com.mangofactory.swagger.plugin.SwaggerSpringMvcPlugin;

/**
 * Configuration for our Swagger based api docs.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 */

@Configuration
@EnableSwagger
@EnableAutoConfiguration
public class SwaggerConfig
{

    private SpringSwaggerConfig springSwaggerConfig;

    @Autowired
    public void setSpringSwaggerConfig(SpringSwaggerConfig springSwaggerConfig)
    {
        this.springSwaggerConfig = springSwaggerConfig;
    }

    /**
     * @return The swagger bean to be used to describe the API
     */
    @Bean
    public SwaggerSpringMvcPlugin customImplementation()
    {
        return new SwaggerSpringMvcPlugin(this.springSwaggerConfig)
                // This info will be used in Swagger. See realisation of ApiInfo for more details.
                .apiInfo(
                        new ApiInfo(
                                "CASDA Data Access API",
                                "This is an API for submitting and tracking data access requests.",
                                null, null, null, null))
                // Here we disable auto generating of responses for REST-endpoints
                .useDefaultResponseMessages(false)
                // Here we specify URI patterns which will be included in Swagger docs. Use regex for this purpose.
                .includePatterns("/requests.*", "/queues.*", "/.*filepath", "/data/async.*", "/metadata.*", "/pawsey.*"
                		, "/coverage.*");
    }

}
