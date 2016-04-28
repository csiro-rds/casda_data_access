Casda Data Access
=================

This web application provides end-user access to data products in the archive. Data products are requested in a batch known as a 'job'.

Four sets of publicly-available end-points are provided:
- a REST interface for creating jobs (called in response to users requesting access to data via DAP)
- a web interface that end-users can use to query their job's status and get links to retrieved data products
- a REST interface that can be used to create, query, and interact with jobs using the Universal Work Service end-points, as defined in: http://www.ivoa.net/documents/UWS/20101010/index.html
- a password-protected administration web interface

Download jobs identify the data products that have been requested but must also specify a download 'mode'.  The 'WEB' and 'SIAP ASYNC' download modes restricts the total size of all the products in a job to a maximum size, whereas the 'Pawsey Pull' mode has no such restriction but instead applies its own restriction that data products may only be accessed from within the Pawsey HPC centre.
 
Data access jobs are queued, and managed by UWS (the same worker service used in VO Tools). Assembled files are stored in a file cache so that requests for the same file can be serviced in a more timely manner.  

When UWS picks up a job from the queue, it first 'touches' any of the files it needs that are already in the cache, by updating the expiry date in the database. Second it makes sure there is enough space in the cache (and on the HPC if required) for this job by removing as many of the least recently accessed files (ie those with the earliest expiry date) as it needs to - if there is already plenty of space this step will not remove any files from the cache. If there is not enough space after cleaning up files, the job will fail. Third it writes a record to the Cached File table for all of the files that are not yet in the cache indicating that they need to be downloaded. And finally it waits until all the files have been successfully copied to the cache to complete the job. If any of the file downloads fails, the job will fail. 

When the service has to retrieve image files from the archive, it does this by calling the `data_deposit` tool `ngas_download`.  Catalogue files are not retrieved from the (tape) archive but by calling the `casda_vo_tools` 'tap' end-point to assemble the catalogue (or catalogues if the user has chosen to 'combine' several catalogues).  A scheduled job runs in the background to initiate and keep track of the progress of files that need to be downloaded from NGAS to the cache. 

Multiple data access jobs can be processed concurrently, but the step to reserve space and add files to the Cached File table is synchronized, so we can accurately keep track of how much space is required, so that files are only added once and so that files that are required by one job won't be removed by another.

The CASDA Administrator can log in to administer the job queue using the dedicated casdaadmin user configured in the application properties. The CASDA Admin can view the job queue; view a job's status; retry, pause, cancel or resume jobs as appropriate; reprioritise jobs in the queue; and pause or unpause the entire job queue. 

Setting up
----------

This project assumes that Eclipse / STS is being used.  Having checked out the project from Stash into the standard project location (ie: 'C:\Projects\Casda'), you can import it into Eclipse as a Gradle project.  You will then need to right-click on the project and do a Gradle -> Refresh All.  

Please then follow the instructions at [https://wiki.csiro.au/display/CASDA/CASDA+Application+setup+-+common] (https://wiki.csiro.au/display/CASDA/CASDA+Application+setup+-+common) to ensure that the code templates and code formatting rules are properly configured.

This project relies on the same Postgres instance managed by the `casda_deposit_manager` service.  Please see that project for instructions on setting-up and configuring the database.


Running the Tests Locally
-------------------------

> `gradle clean test`


Running the Tests within Eclipse
--------------------------------

You should just be able to run a test as a JUnit test in Eclipse (ie: there is no special configuration required).


Building and Installing Locally
-------------------------------

> `gradle clean deployToLocal`

This will build and deploy the war to your local tomcat installation (along with an application context file.)


Running Locally
---------------

Normally the endpoints would be called from DAP but you can create a job through the swagger UI available at:
> `http://localhost:8080/casda_data_access/sdoc.jsp`

Most of the fields for creating a job are self-explanatory.  The IDs are the database record IDs of the artefacts you're trying to download.  The download format only pertains to catalogue requests.  By default, the retrieval of files from archive will be attempted using the `data_deposit` `ngas_download` tool.  This will work successfully only when a data product with the same name exists in the dev NGAS instance (see the `data_deposit` project for how to make this happen).  Alternatively, the a 'mock' download service may be called by configuring the following properties into a local application properties file.  If you're running the service in Eclipse, this is best achieved by adding an `application-casda_data_access.properties` file into the root directory (you can copy the one in `src/test/resources/config` or create a new one).  If you're running the service in Tomcat then you will need to edit the `application-casda_data_access.properties` file in `$CATALINA_HOME/config/bin`.  Please note that this file is replaced every time you do a `gradle deployToLocal` so a slightly easier course is to modify the `src/test/resources/config` file directly - but then please do not commit your changes.

    download.command.type: SIMPLE
    download.command: {"/dev/cygwin/bin/bash", "-c", "/usr/bin/touch '<destination>' && /usr/bin/touch '<destination>.checksum' && exit 0"}
    download.args: 

If you're running separate services in Eclipse, then you will probably also want to change these as well:

    uws.baseurl: http://localhost:8030/casda_data_access/uws
    casda_vo_tools.url: http://localhost:8040/casda_vo_tools/

The file cache will default to `/cygdrive/c/temp/casda_cache`.  To clear it you can simply delete it but then you will also need to clear up associated records in the database.  This can be done comprehensively by clearing the data access job tables using this script:

    begin;
    delete from casda.cached_file_data_access_jobs;
    delete from casda.cached_file;
    delete from casda.data_access_job_catalogue;
    delete from casda.data_access_job_image_cube;
    delete from casda.data_access_job_measurement_set;
    delete from casda.data_access_job;
    end;

Running Within Eclipse
----------------------

The following instructions assume you will run a separate server for each service.  This isn't how things run in our server environments but it makes development a little easier because of the ability to startup and shutdown just the services you need.  If you want to combine them you can - just make sure this is running in the same server as the other CASDA services (ie: not the DAP ones).

* In Eclipse, make sure the 'Servers' view is visible somewhere (Window -> Show View -> Servers).  
* In that view, right-click and select New -> Server.
* In the dialog that opens, expand the Apache folder and select Tomcat v7.0 Server, then click Finish.  You should now see a server in the Servers view called "Tomcat v7.0 Server at localhost".  Rename that to CASDA Data Access and also make sure that a 1.8 JDK (not JRE) is used as the runtime environment. 
* Right click on that server and choose 'Add and Remove'.
* In the dialog that opens you should see `casda_data_access`.  Make sure it is in the Configured pane, then click 
Finish.  
* Now double-click on the server in the Servers view and edit the following:
** Timeouts
*** Change the startup timeout to 120 seconds and the stop timeout to 30.
** Ports
*** 8035 (Tomcat admin)
*** 8030 (HTTP)
*** 8437 (HTTPS)
*** 8039 (AJP)

The endpoints will now be available under `http://localhost:8030/casda_data_access`.  (You will still need to create the RTC target directory as described above.  Changes to configuration - as shown above - are most simply made against the `src/test/resources/config` `application-casda_data_access.properties` file but you might also want to consider creating a `application-casda_data_access.properties` in the root directory.)

Configuration
-------------

### Logging

The log4j configuration file is detected using default behaviour of log4j, by being named according to convention (`log4j2.xml`) and provided on the classpath. When running locally or in the application environments, the log4j configuration file is bundled with the application. When running under Eclipse, the log configuration file is copied into the local Eclipse-managed server's WEB-INF/classes directory, because it's in the src/test/resources folder, and loaded via the classpath.

We have decided to use a single log configuration file common to local and server environments.  Consequently, in your local environment, you will see log4j configuration errors as the logging system tries to talk to syslog and all logs be written to:
> `/CASDA/application/tomcat/logs/casda_data_access.log`

### Application Properties

The system uses Spring Boot's mechanism for loading application properties.  When running on a server, the properties are drawn from two locations:

- `$CATALINA_HOME/webapps/casda_data_access/WEB-INF/classes/application.properties`
- `$CATALINA_HOME/config/application-casda_data_access.properties`

The second file is loaded because it is under a `config` directory relative to the 'run' directory.  Locally the run directory is different and the 'local' file is picked up at:

- `$CATALINA_HOME/bin/config/application-casda_data_access.properties`

When running under Eclipse, the `src/test/resources/config` folder is copied into the local Eclipse-managed server's WEB-INF/classes directory (as it's in the classpath by dint of being located under src/test/resources).

Test cases always load application.properties files manually.

Command Line Tools
------------------
Due to a known intermittent (appears and disappears from release to release) Java bug which occurs when the JVM calls fork()+exec() too often, this component uses a Python service to run commands on the server (the script can be found [here](https://bitbucket.csiro.au/projects/CASDA/repos/casda-runcommand-service/browse)). 
The properties file for the run command service has a whitelist of commands that may be run, any changes to the commands or their arguments means the whitelist must be updated.

The properties file can be found in [server_config](https://bitbucket.csiro.au/projects/CASDA/repos/server_config/browse) at these addresses:


- server_config/browse/pawsey/dev/files/ASKAP/access/dev/casda_deposit_tools/runcommand/config
- server_config/browse/pawsey/test/files/ASKAP/access/test/casda_deposit_tools/runcommand/config
- server_config/browse/pawsey/at/files/ASKAP/access/at/casda_deposit_tools/runcommand/config
- server_config/browse/pawsey/prd/files/ASKAP/prd-access/prd/casda_deposit_tools/runcommand/config


Updates the license header of the current project source files
--------------------------------------------------------------
Change the relevant value in pom.xml then run this command in command prompt:

$ mvn license:update-file-header
