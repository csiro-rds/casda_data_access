package au.csiro.casda.access.services;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import au.csiro.casda.access.InlineScriptException;
import au.csiro.casda.jobmanager.ProcessJob;
import au.csiro.casda.jobmanager.ProcessJobBuilder.ProcessJobFactory;
import au.csiro.casda.jobmanager.SimpleToolProcessJobBuilder;
import au.csiro.casda.jobmanager.SingleJobMonitor;

/**
 * Service for running scripts inline.
 * 
 * Copyright 2015, CSIRO Australia All rights reserved.
 *
 */
@Service
public class InlineScriptService
{

    private ProcessJobFactory processJobFactory;
    
    /**
     * Create a new instance of the InlineScriptService.
     * 
     * @param processJobFactory
     *            the factory to be used to create job processes.
     */
    @Autowired
    public InlineScriptService(ProcessJobFactory processJobFactory)
    {
        this.processJobFactory = processJobFactory;
    }
    
    /**
     * Calls a unix/batch script inline to interact.
     * 
     * @param args
     *            the script arguments
     * @return the String output from the script
     * @throws InlineScriptException
     *             if there is a problem interacting with the Pawsey HPC, or if the script doesn't execute successfully
     */
    public String callScriptInline(String... args) throws InlineScriptException
    {
        SingleJobMonitor jobMonitor = createInlineJobMonitor(args);

        if (!jobMonitor.isJobCreated())
        {
            throw new InlineScriptException("Error executing the command with args " + StringUtils.join(args, " ") + "\n"
                    + jobMonitor.getJobOutput());
        }

        if (jobMonitor.isJobFailed())
        {
            throw new InlineScriptException("Error executing the command with args " + StringUtils.join(args)
                    + " response: " + jobMonitor.getJobOutput());
        }

        return jobMonitor.getJobOutput();

    }

    /**
     * Creates the process and job monitor to run a script inline.
     * 
     * @param args
     *            the command and arguments
     * 
     * @return the Job Monitor
     */
    public SingleJobMonitor createInlineJobMonitor(String... args)
    {
        SimpleToolProcessJobBuilder processBuilder = new SimpleToolProcessJobBuilder(processJobFactory, args);
        ProcessJob inlineJob = processBuilder.createJob(StringUtils.join(args, "-"), "inline");
        SingleJobMonitor jobMonitor = new SingleJobMonitor();
        inlineJob.run(jobMonitor);
        return jobMonitor;
    }

}
