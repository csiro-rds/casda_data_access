package au.csiro.casda.access.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.spring.notification.MailService;
/*
 * #%L
 * CSIRO ASKAP Science Data Archive
 * %%
 * Copyright (C) 2017 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 * %%
 * Licensed under the CSIRO Open Source License Agreement (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License in the LICENSE file.
 * #L%
 */

/**
 * Mail service for notifying users of events which may affect them, e.g. files ready for download, or link expiration
 * <p>
 * Copyright 2017, CSIRO Australia. All rights reserved.
 */
@Service
public class CasdaMailService
{
    private static Logger logger = LoggerFactory.getLogger(CasdaMailService.class);
    
	/** Constant for the job ready email */
	public static final String READY_EMAIL = "job.ready.email.ftl";
	/** Constant for the job failed email */
	public static final String FAILED_EMAIL = "job.failed.email.ftl";
	/** Constant for the job created email */
	public static final String CREATED_EMAIL = "job.created.email.ftl";
	/** Constant for the job expiring email */
	public static final String EXPIRING_EMAIL = "job.expiring.email.ftl";
	/** Constant for the job expired email */
	public static final String EXPIRED_EMAIL = "job.expired.email.ftl";
	
	/** Constant for the job ready email subject line */
	public static final String READY_EMAIL_SUBJECT = "Data Access Job <ID> is ready for download";
	/** Constant for the job failed email subject line */
	public static final String FAILED_EMAIL_SUBJECT = "Data Access Job <ID> has failed";
	/** Constant for the job created email subject line */
	public static final String CREATED_EMAIL_SUBJECT = "Data Access Job <ID> has been created";
	/** Constant for the job expiring email subject line */
	public static final String EXPIRING_EMAIL_SUBJECT = "Data Access Job <ID> is about to expire";
	/** Constant for the job expired email subject line */
	public static final String EXPIRED_EMAIL_SUBJECT = "Data Access Job <ID> has expired";
	
    private final MailService mailService;
    private final String emailSender;
    private String link;
    
    /**
     * Constructor
     * @param mailService the mail service for sending this email
     * @param emailSender the from address for email from this service
     * @param link the link to be used in the email, this points to the job details page
     */
    @Autowired
    public CasdaMailService(MailService mailService, @Value("${email.sender.address}") String emailSender, 
    		@Value("${email.link.url}") String link)
    {
        this.mailService = mailService;
        this.emailSender = emailSender;
        this.link = link;
    }
    
    /**
     * Sends an email based on the named freemarker template, if possible (user details exist)
     * @param dataAccessJob the data access job in question
     * @param template the name of the template to use for this email
     * @param subject the subject of the email
     */
    public void sendEmail(DataAccessJob dataAccessJob, String template, String subject)
    {
    	try
    	{
        	if(StringUtils.isNotBlank(dataAccessJob.getUserEmail()))
            {
        		Map<String, Object> values = new HashMap<String, Object>();
        		values.put("userName", dataAccessJob.getUserName() != null ? dataAccessJob.getUserName() : "Sir/Madam");
        		values.put("jobID", dataAccessJob.getRequestId());
        		values.put("jobLink", link.replace("<ID>", dataAccessJob.getRequestId()));
        		if(dataAccessJob.getExpiredTimestamp() != null)
        		{
        			Days diffInDays = Days.daysBetween(new DateTime(), dataAccessJob.getExpiredTimestamp());
            		values.put("daysToExpire", diffInDays.getDays());
        		}
        		
            	this.mailService.sendMail(emailSender, dataAccessJob.getUserEmail(), 
            			subject.replace("<ID>", dataAccessJob.getRequestId()), values, template);
                logger.info("Sent email {} for request {}", subject, dataAccessJob.getRequestId());
            }
        	else
        	{
                logger.info("No address so no email sent {} for request {} user i:{} e:{} n:{} l:{}", subject,
                        dataAccessJob.getRequestId(), dataAccessJob.getUserIdent(), dataAccessJob.getUserEmail(),
                        dataAccessJob.getUserName(), dataAccessJob.getUserLoginSystem());
        	}
    	}
    	catch(Exception e)
    	{
            logger.warn("Data Access Job {} notification failed to send", dataAccessJob.getRequestId(), e);
    	}
    }
}
