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


import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import au.csiro.casda.access.jpa.CachedFileRepository;

/**
 * Spring Configuration for unit tests. To use the class, add the following line to the top of your test:
 * <p>
 * <code>
 * <pre>
 * @ContextConfiguration(classes = { TestAppConfig.class }) 
 * </pre>
 * </code>
 * <p>
 * This class ensures that all components under the au.csiro.casda.datadeposit.observation
 * package are autowired. It also ensures that the standard application.properties files
 * are read in the order defined below.
 * 
 * Note: If it turns out that some components should not be included then an exclusion
 * filter should be added to the <pre>@ComponentScan</pre> annotation.
 * 
 * Copyright 2013, CSIRO Australia All rights reserved.
 */
@PropertySource("classpath:/application.properties")
@PropertySource("classpath:/config/application-casda_data_access.properties")
@PropertySource("classpath:/test_config/application.properties")
@EnableJpaRepositories(basePackageClasses = { CachedFileRepository.class })
@EntityScan("au.csiro.casda.entity")
@EnableAutoConfiguration(exclude = JpaRepositoriesAutoConfiguration.class)
/*
 * --------------------------------------------------------------------------------------------------------------------
 * 
 * WARNING: Do NOT declare this class as an @Configuration or it will be automatically loaded when you run the command
 * line applications through Eclipse. See comment above about how to configure test classes to pick up this class as a
 * configuration.
 * 
 * --------------------------------------------------------------------------------------------------------------------
 */
public class TestAppConfig
{
    /**
     * Required to configure the PropertySource(s) (see https://jira.spring.io/browse/SPR-8539)
     * 
     * @return a PropertySourcesPlaceholderConfigurer
     */
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer()
    {
        return new PropertySourcesPlaceholderConfigurer();
    }

}
