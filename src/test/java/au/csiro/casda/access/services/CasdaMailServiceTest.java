package au.csiro.casda.access.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.containsString;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

import javax.mail.MessagingException;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.subethamail.wiser.Wiser;
import org.subethamail.wiser.WiserMessage;

import au.csiro.casda.Utils;
import au.csiro.casda.access.TestAppConfig;
import au.csiro.casda.entity.dataaccess.DataAccessJob;
import au.csiro.casda.entity.dataaccess.DataAccessJobStatus;
import au.csiro.spring.notification.MailService;
import freemarker.template.TemplateExceptionHandler;

/**
 * Tests for the casda mail service.
 * 
 * Copyright 2017, CSIRO Australia All rights reserved.
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestAppConfig.class})
public class CasdaMailServiceTest 
{
	CasdaMailService casdaMailService;
	
	MailService mailService;
	
	private Wiser wiser;
	
    @Before
    public void setUp() throws Exception
    {
    	//freemarker configuration
    	freemarker.template.Configuration config = new freemarker.template.Configuration();
    	config.setClassForTemplateLoading(Utils.class, "/freemarker/");
    	config.setDefaultEncoding("UTF-8");
    	config.setLocale(Locale.ENGLISH);
    	config.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
    	
    	//Mail sender configuration
    	JavaMailSenderImpl sender = new JavaMailSenderImpl();
    	sender.setHost("localhost");
    	sender.setPort(2345);
    	
    	mailService = new MailService(sender, config, null);
    	casdaMailService = new CasdaMailService(mailService, "casda@csiro.au", 
    			"https://localhost:8080/link/to/<ID>/page/1");
    	
        wiser = new Wiser();
        wiser.setHostname("localhost");
        wiser.setPort(2345);
        wiser.start();
    }
    
    @After
    public void tearDown() throws Exception 
    {
        wiser.stop();
    }
	
	@Test
	public void testJobCreatedEmailContent() throws IOException, MessagingException
	{
		String email = FileUtils.readFileToString(new File("src/test/resources/email_notification/emailCreated.txt"));
		
		DataAccessJob job = createDataAccessJob(DataAccessJobStatus.PREPARING);
		
		casdaMailService.sendEmail(job, CasdaMailService.CREATED_EMAIL, CasdaMailService.CREATED_EMAIL_SUBJECT);
		
		List<WiserMessage> messages = wiser.getMessages();
		
		assertEquals(1, messages.size());
		assertEquals("steve.stevens@gmail.com", messages.get(0).getEnvelopeReceiver());
		assertEquals("casda@csiro.au", messages.get(0).getEnvelopeSender());
		assertEquals("Data Access Job d6f1a293-630f-4df9-b095-3516f858bbf7 has been created", 
				messages.get(0).getMimeMessage().getSubject());
		
		assertThat(removeNewLine(messages.get(0).toString()), containsString(removeNewLine(email)));
	}
	
	@Test
	public void testJobFailedEmailContent() throws IOException, MessagingException
	{
		String email = FileUtils.readFileToString(new File("src/test/resources/email_notification/emailFailed.txt"));
		
		DataAccessJob job = createDataAccessJob(DataAccessJobStatus.ERROR);
		
		casdaMailService.sendEmail(job, CasdaMailService.FAILED_EMAIL, CasdaMailService.FAILED_EMAIL_SUBJECT);
		
		List<WiserMessage> messages = wiser.getMessages();
		
		assertEquals(1, messages.size());
		assertEquals("steve.stevens@gmail.com", messages.get(0).getEnvelopeReceiver());
		assertEquals("casda@csiro.au", messages.get(0).getEnvelopeSender());
		assertEquals("Data Access Job d6f1a293-630f-4df9-b095-3516f858bbf7 has failed", 
				messages.get(0).getMimeMessage().getSubject());

		assertThat(removeNewLine(messages.get(0).toString()), containsString(removeNewLine(email)));
	}
	
	@Test
	public void testJobReadyEmailContent() throws IOException, MessagingException
	{
		String email = FileUtils.readFileToString(new File("src/test/resources/email_notification/emailReady.txt"));
		
		DataAccessJob job = createDataAccessJob(DataAccessJobStatus.READY);
		
		casdaMailService.sendEmail(job, CasdaMailService.READY_EMAIL, CasdaMailService.READY_EMAIL_SUBJECT);
		
		List<WiserMessage> messages = wiser.getMessages();
		
		assertEquals(1, messages.size());
		assertEquals("steve.stevens@gmail.com", messages.get(0).getEnvelopeReceiver());
		assertEquals("casda@csiro.au", messages.get(0).getEnvelopeSender());
		assertEquals("Data Access Job d6f1a293-630f-4df9-b095-3516f858bbf7 is ready for download", 
				messages.get(0).getMimeMessage().getSubject());

		assertThat(removeNewLine(messages.get(0).toString()), containsString(removeNewLine(email)));
	}
	
	@Test
	public void testJobExpiringEmailContent() throws IOException, MessagingException
	{
		String email = FileUtils.readFileToString(new File("src/test/resources/email_notification/emailExpiring.txt"));
		
		DataAccessJob job = createDataAccessJob(DataAccessJobStatus.READY);
		
		casdaMailService.sendEmail(job, CasdaMailService.EXPIRING_EMAIL, CasdaMailService.EXPIRING_EMAIL_SUBJECT);
		
		List<WiserMessage> messages = wiser.getMessages();
		
		assertEquals(1, messages.size());
		assertEquals("steve.stevens@gmail.com", messages.get(0).getEnvelopeReceiver());
		assertEquals("casda@csiro.au", messages.get(0).getEnvelopeSender());
		assertEquals("Data Access Job d6f1a293-630f-4df9-b095-3516f858bbf7 is about to expire", 
				messages.get(0).getMimeMessage().getSubject());

		assertThat(removeNewLine(messages.get(0).toString()), containsString(removeNewLine(email)));
	}
	
	@Test
	public void testJobExpiredEmailContent() throws IOException, MessagingException
	{
		String email = FileUtils.readFileToString(new File("src/test/resources/email_notification/emailExpired.txt"));
		
		DataAccessJob job = createDataAccessJob(DataAccessJobStatus.EXPIRED);
		
		casdaMailService.sendEmail(job, CasdaMailService.EXPIRED_EMAIL, CasdaMailService.EXPIRED_EMAIL_SUBJECT);
		
		List<WiserMessage> messages = wiser.getMessages();
		
		assertEquals(1, messages.size());
		assertEquals("steve.stevens@gmail.com", messages.get(0).getEnvelopeReceiver());
		assertEquals("casda@csiro.au", messages.get(0).getEnvelopeSender());
		assertEquals("Data Access Job d6f1a293-630f-4df9-b095-3516f858bbf7 has expired",
				messages.get(0).getMimeMessage().getSubject());

		assertThat(removeNewLine(messages.get(0).toString()), containsString(removeNewLine(email)));
	}
	
	private DataAccessJob createDataAccessJob(DataAccessJobStatus status)
	{
		DataAccessJob job = new DataAccessJob();
		job.setUserEmail("steve.stevens@gmail.com");
		job.setUserName("steve stevens");
		job.setRequestId("d6f1a293-630f-4df9-b095-3516f858bbf7");
		DateTime expiredTimestamp = new DateTime();
		job.setExpiredTimestamp(expiredTimestamp.plusDays(3).plusMinutes(1));
		job.setStatus(status);
		return job;
	}
	
	/**
	 * removes newline chars so tests behave same in linux &amp; windows
	 * @param message the message to sanitise
	 * @return the new message (sans newline chars)
	 */
	private String removeNewLine(String message)
	{
		return message.replace("\n", "").replace("\r", "");
	}
}
