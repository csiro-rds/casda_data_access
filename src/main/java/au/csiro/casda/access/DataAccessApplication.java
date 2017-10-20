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

/*
 * CSIRO Data Access Portal
 * 
 * Copyright (C) 2010 - 2012 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * 
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * 
 */

import java.io.File;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.boot.context.web.ErrorPageFilter;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.boot.orm.jpa.EntityScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.scheduling.annotation.EnableScheduling;

import au.csiro.casda.Utils;
import au.csiro.casda.access.jpa.CachedFileRepository;
import au.csiro.casda.access.uws.AccessUwsFactory;
import au.csiro.casda.deposit.jobqueue.QueuedJobManager;
import au.csiro.casda.jobmanager.AsynchronousJobManager;
import au.csiro.casda.jobmanager.CommandRunnerServiceProcessJobFactory;
import au.csiro.casda.jobmanager.JavaProcessJobFactory;
import au.csiro.casda.jobmanager.JobManager;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import au.csiro.casda.jobmanager.SlurmJobManager;
import au.csiro.casda.logging.CasdaLoggingSettings;
import au.csiro.spring.notification.MailService;
import freemarker.template.TemplateExceptionHandler;
import uws.UWSException;
import uws.service.UWSFactory;
import uws.service.file.LocalUWSFileManager;
import uws.service.file.UWSFileManager;

/**
 * Initialises spring boot application.
 * 
 * Copyright 2014, CSIRO Australia All rights reserved.
 * 
 *
 */
@Configuration
@EntityScan("au.csiro.casda.entity")
@EnableAutoConfiguration
@ComponentScan
@EnableJpaRepositories(basePackageClasses = { CachedFileRepository.class })
@EnableScheduling
public class DataAccessApplication extends SpringBootServletInitializer
{
    private static Logger logger = LoggerFactory.getLogger(DataAccessApplication.class);

    /**
     * Application name used in logging.
     */
    public static final String APPLICATION_NAME = "CasdaDataAccess";

    private static final String CONFIG_FOLDER = "config";

    @Autowired
    private ApplicationContext context;

    private Class<JobManager> unthrottledJobManagerClass;

    private HashMap<String, Integer> jobManagerThrottlingMap;

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application)
    {

        File userDir = new File(System.getProperty("user.dir"));
        File configDir = new File(userDir, CONFIG_FOLDER);

        CasdaLoggingSettings loggingSettings = new CasdaLoggingSettings(APPLICATION_NAME, null);

        loggingSettings.addGeneralLoggingSettings();

        logger.info("Config being read from {} and {}", configDir.getAbsolutePath(), userDir.getAbsolutePath());

        SpringApplicationBuilder app = application.sources(DataAccessApplication.class);
        app.profiles("casda_data_access");
        return app;
    }

    /**
     * Configures the throttling behaviour (how many jobs of a particular type can run concurrently) of the throttled
     * JobManager. The string must be in the format of a Spring EL list, where the elements of the list are name/value
     * pairs, eg: {&quot;stage_artefact&quot;, &quot;1&quot;, &quot;register_artefact&quot;, &quot;4&quot; }
     * 
     * @param jobManagerThrottlingConfig
     *            the configuration String
     */
    @Autowired
    @Value("${job.manager.throttled.config}")
    public void setJobManagerThrottlingConfig(String jobManagerThrottlingConfig)
    {
        Map<String, String> map = Utils.elStringToMap(jobManagerThrottlingConfig);
        this.jobManagerThrottlingMap = new HashMap<>();
        for (String key : map.keySet())
        {
            this.jobManagerThrottlingMap.put(key, Integer.parseInt(map.get(key)));
        }
    }

    /**
     * Sets the class name for the non-throttled JobManager class to be used to run Jobs.
     * 
     * @param className
     *            the name of a class that implements the JobManager interface
     */
    @SuppressWarnings("unchecked")
    @Autowired
    @Value("${job.manager.class.name}")
    public void setUnthrottledJobManagerClassName(String className)
    {
        try
        {
            unthrottledJobManagerClass = (Class<JobManager>) Class.forName(className);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a new SlurmJobManager instance for use by this application. Called by Spring on startup.
     * 
     * @param processJobFactory
     *            the factory to be used to create job processes.
     * @param slurmJobStatusSeparator
     *            the String used to separate the Slurm status elements
     * @param runningJobsCountCommandAndArgs
     *            the command and args used to find out how many jobs are running for a particular job type
     * @param jobStatusCommandAndArgs
     *            the command and args used to find out a job's status
     * @param startJobCommandAndArgsPrologue
     *            the command and args used to start a job
     * @param cancelJobCommandAndArgs
     *            the command and args used to cancel a job
     * @return A new SlurmJobManager instance.
     */
    @Bean
    public SlurmJobManager slurmJobManager(ProcessJobFactory processJobFactory,
            @Value("${slurm.job.status.separator}") String slurmJobStatusSeparator,
            @Value("${slurm.jobs.running.count.command}") String runningJobsCountCommandAndArgs,
            @Value("${slurm.job.status.command}") String jobStatusCommandAndArgs,
            @Value("${slurm.job.start.command.prologue}") String startJobCommandAndArgsPrologue,
            @Value("${slurm.job.cancel.command}") String cancelJobCommandAndArgs)
    {
        return new SlurmJobManager(processJobFactory, slurmJobStatusSeparator, runningJobsCountCommandAndArgs,
                jobStatusCommandAndArgs, startJobCommandAndArgsPrologue, cancelJobCommandAndArgs);
    }

    /**
     * Create a new AsynchronousJobManager instance for use by this application. Called by Spring on startup.
     * 
     * @return The AsynchronousJobManager instance.
     */
    @Bean
    public AsynchronousJobManager asynchronousJobManager()
    {
        return new AsynchronousJobManager();
    }

    /**
     * @return the JobManager bean to be used throughout the application. This JobManager will be throttled.
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public JobManager jobManager()
    {
        return new QueuedJobManager(getUnthrottledJobManager(), this.jobManagerThrottlingMap);
    }

    /**
     * Create an instance of the base job manager implementation as specified in the job.manager.unthrottled.class.name
     * config value. It should be wrapped in a throttling manager before use.
     * 
     * @return the unthrottled JobManager bean.
     */
    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public JobManager getUnthrottledJobManager()
    {
        if (unthrottledJobManagerClass.isAssignableFrom(SlurmJobManager.class))
        {
            return (JobManager) context.getBean("slurmJobManager");
        }

        return context.getAutowireCapableBeanFactory().createBean(this.unthrottledJobManagerClass);
    }

    /**
     * Used by Spring Boot to configure a Filter that intercepts exceptions and shows a (customizable) error page. We
     * have defined our own version of this bean to disable the behaviour (see #disableSpringBootErrorFilter).
     * 
     * @return an ErrorPageFilter
     */
    @Bean
    public ErrorPageFilter errorPageFilter()
    {
        return new ErrorPageFilter();
    }

    /**
     * Overrides the default FilterRegistrationBean so that our ErrorPageFilter is disabled.
     * <p>
     * The reason we want to disable that page is because our ExceptionHandlers class now deals with all exceptions and
     * having the ErrorPageFilter in the call chain causes some interesting double-handling of exceptions. Also requires
     * the definition of an ErrorPageFilter bean (which can be Spring Boot's default one).
     * 
     * @param filter
     *            an ErrorPageFilter
     * @return a FilterRegistrationBean
     */
    @Bean
    public FilterRegistrationBean disableSpringBootErrorFilter(ErrorPageFilter filter)
    {
        FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(filter);
        filterRegistrationBean.setEnabled(false);
        return filterRegistrationBean;
    }

    /**
     * Return the factory which will create ProcessJob instance for us to run commands.
     * 
     * @param cmdWebServiceUrl
     *            The URL of the command runner web service.
     * @param factoryName
     *            The name of the factory to use, if not using the default.
     * @return The ProcessJobFactory instance
     */
    @Bean
    public ProcessJobFactory getProcessJobFactory(@Value("${command.webservice.url:}") String cmdWebServiceUrl,
            @Value("${command.process.job.factory:}") String factoryName)
    {
        if ("CommandRunnerServiceProcessJobFactory".equalsIgnoreCase(factoryName))
        {
            if (StringUtils.isBlank(cmdWebServiceUrl))
            {
                throw new IllegalArgumentException(
                        "command.webservice.url must be configured to use CommandRunnerServiceProcessJobFactory");
            }
            return new CommandRunnerServiceProcessJobFactory(cmdWebServiceUrl, APPLICATION_NAME);
        }

        return new JavaProcessJobFactory();
    }

    /**
     * @return a UWSFactory instance
     */
    @Bean
    public UWSFactory getUwsFactory()
    {
        return context.getAutowireCapableBeanFactory().createBean(AccessUwsFactory.class);
    }

    /**
     * @param uwsDirectoryName
     *            the name of the directory used to persist UWS jobs
     * @return a UWSFileManager instance
     * @throws UWSException
     *             if there was a problem instantiating the UWSFileManager
     */
    @Bean
    public UWSFileManager getUwsFileManager(@Value("${uws.directory}") String uwsDirectoryName) throws UWSException
    {
        return new LocalUWSFileManager(new File(uwsDirectoryName), false, false);
    }
    
    /**
     * Creates and populates the MailService bean
     * @param templates the path where the freemarker templates are stored
     * @param safeAddress the emailaddress which will override the 'to address' in non-prod environments
     * @param host the smtp server
     * @param port the port for accessing the smtp server
     * @return the MailService
     */
    @Bean
    public MailService getMailService(@Value("${freemarker.template.path}") String templates, 
    								  @Value("${email.safe.address}") String safeAddress,
    								  @Value("${email.host}") String host,
    								  @Value("${email.port}") int port)
    {
    	//freemarker configuration
    	freemarker.template.Configuration config = new freemarker.template.Configuration();
    	config.setClassForTemplateLoading(Utils.class, templates);
    	config.setDefaultEncoding("UTF-8");
    	config.setLocale(Locale.ENGLISH);
    	config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    	
    	//Mail sender configuration
    	JavaMailSenderImpl sender = new JavaMailSenderImpl();
    	sender.setHost(host);
    	sender.setPort(port);
    	
    	return new MailService(sender, config, safeAddress);
    }
}
