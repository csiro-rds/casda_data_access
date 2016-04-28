<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@taglib prefix="s" uri="http://www.springframework.org/tags"%>
<%@ taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<%@ taglib prefix="sec"
	uri="http://www.springframework.org/security/tags"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>

<%@ page session="false"%>
<html>
<head>

<script type="text/javascript" src="/casda_data_access/js/jquery-2.2.1.min.js"></script>

<c:choose>
	<c:when test="${dataAccessJob.preparing || dataAccessJob.paused}">
		<script type="text/javascript">
			$(window).ready(function() {
				startTimer(30)
			});
		</script>
	</c:when>
</c:choose>

<title>CSIRO Data Access Portal - CASDA Access Request Status</title>
<%@include file="include/head.jsp"%>

<script type="text/javascript">
function startTimer()
{	
	var time = arguments[0];   
    setInterval( function() {        
        time--;        
        $('#time').html(time);        
        if (time === 0) {            
            location.reload()
        }       
    }, 1000 );
}
</script>


</head>

<body>

	<form:form name='modifyJobForm' method="post" action="modifyJob">

		<div id="wrapper">

			<%@include file="include/header.jsp"%>

			<c:if test="${not empty message}">
				<c:set var="flashMessage" value="${message}" />
				<c:if test="${fn:startsWith(message, 'FAILURE')}">
					<c:set var="flashBackgroundColour" value="#FBC8C8" />
				</c:if>
				<c:if test="${fn:startsWith(message, 'SUCCESS')}">
					<c:set var="flashBackgroundColour" value="#CBF8CB" />
				</c:if>
				<div
					style="margin: 50px; padding: 10px; border: 1px solid black; background-color: ${flashBackgroundColour};">
					${flashMessage}</div>
			</c:if>

<c:choose>
	<c:when test="${dataAccessJob.preparing || dataAccessJob.paused}">
			<div class="refreshBox preparing">
				This page shows the current status of your request and will
				automatically refresh until your data is ready to be retrieved. Next
				refresh in &nbsp;<span class="timer" id="time">...</span>&nbsp;sec.
			</div>
	</c:when>
</c:choose>

			<div id="content">
				<fieldset>
					<legend>Retrieve Files</legend>

					<s:eval
						expression="T(au.csiro.casda.access.DataAccessUtil).formatDateTimeToUTC(dataAccessJob.createdTimestamp)"
						var="createdDateFormatted" />
					<p>This request submitted ${createdDateFormatted } includes
						${totalFiles} file(s)${totalSize} It may take some time before
						your data is retrieved and can be accessed. This page will show
						the current status of your request.</p>

					<p>
						To come back to this page later you can bookmark this page or use 
						the following reusable link: <a href="${requestURL}">
							${requestURL} </a>
					</p>

					<div class="centered">
						<h2>Status</h2>
						<c:choose>
							<c:when test="${dataAccessJob.preparing || dataAccessJob.paused}">
								<div class="statusBox preparing">
									<p>Preparing your data for retrieval</p>
								</div>
							</c:when>
							<c:when test="${dataAccessJob.ready}">
								<div class="statusBox ready">
									<p>Your data is ready for retrieval</p>
								</div>
							</c:when>
							<c:when test="${dataAccessJob.expired}">
								<div class="statusBox expired">
									<p>Your data access request has expired</p>
								</div>
							</c:when>
							<c:when test="${dataAccessJob.error}">
								<div class="statusBox error">
									<p>Error processing request</p>
								</div>
							</c:when>
							<c:when test="${dataAccessJob.cancelled}">
								<div class="statusBox error">
									<p>Request has been cancelled</p>
								</div>
							</c:when>
						</c:choose>
					</div>

					<br />

					<div class="floatLeft">
						<c:choose>
							<c:when test="${dataAccessJob.ready}">
								<s:eval
									expression="T(au.csiro.casda.access.DataAccessUtil).formatDateTimeToUTC(dataAccessJob.expiredTimestamp)"
									var="expiredDateFormatted" />
								<p>You have ${remainingTime} remaining to download your
									data. This data access request will expire at
									${expiredDateFormatted }</p>

								<c:choose>
									<c:when test="${dataAccessJob.downloadMode == 'PAWSEY_HTTP'}">

										<p>Your data can be accessed at the Pawsey Supercomputing
											Centre. You must have already arranged access to the Pawsey
											facilities.</p>

										<h2>Download your files below</h2>

									</c:when>
									<c:otherwise>
										<h2>Download your files below</h2>
									</c:otherwise>
								</c:choose>
							</c:when>

							<c:otherwise>
								<h2>Files</h2>
							</c:otherwise>
						</c:choose>

						<table class="filesTable">

							<colgroup>
								<col />
								<col />
								<col />
							</colgroup>

							<tr class="tableHeader">
								<td>Name</td>
								<td>Size (kB)</td>
								<td>Link</td>
							</tr>

							<c:forEach var="downloadFile" items="${downloadFiles}"
								varStatus="rowCounter">
								<tr class="odd">
									<td>${downloadFile.displayName }</td>
									<td>${downloadFile.sizeKb}</td>
									<td><c:choose>
											<c:when test="${dataAccessJob.ready}">
												<s:eval
													expression="T(au.csiro.casda.access.DataAccessUtil).getRelativeLinkForFile(dataAccessJob.downloadMode, dataAccessJob.requestId, downloadFile.filename)"
													var="link" />
												<a href="${baseUrl}${link}">${downloadFile.filename }</a>
											</c:when>
											<c:otherwise>Unavailable</c:otherwise>
										</c:choose></td>

								</tr>

								<tr class="even">
									<td>${downloadFile.displayName}.checksum</td>
									<!-- these files are always the same size (and tiny), so just put in 1kb as default -->
									<td>1</td>
									<td><c:choose>
											<c:when test="${dataAccessJob.ready}">
												<s:eval
													expression="T(au.csiro.casda.access.DataAccessUtil).getRelativeLinkForFileChecksum(dataAccessJob.downloadMode, dataAccessJob.requestId, downloadFile.filename)"
													var="link" />
												<a href="${baseUrl}${link}">${downloadFile.filename}.checksum</a>
											</c:when>
											<c:otherwise>Unavailable</c:otherwise>
										</c:choose></td>
								</tr>
							</c:forEach>

						</table>
						<c:choose>
							<c:when test="${dataAccessJob.ready}">
								<a href="${downloadLink}" class="downloadLink" title="Click to download the urls of the above files in a single text file.">Save links as Text file</a>
							</c:when>
						</c:choose>
					</div>
				</fieldset>

				<sec:authorize access="hasRole('ROLE_CASDA_ADMIN')">
					<div>
						<table>
							<tr>
								<c:choose>
									<c:when	test="${dataAccessJob.expired || executionPhase == 'EXECUTING'}">
										<td><input type="submit" name="action" value="Restart"
											style="height: 35px; width: 70px" disabled /></td>
										<td><input type="submit" name="action" value="Cancel"
											style="height: 35px; width: 70px" ${(executionPhase == 'EXECUTING') ? '' : 'disabled'}/></td>
										<td><input type="submit" name="action" value="Pause"
											style="height: 35px; width: 70px" disabled /></td>
									</c:when>
									<c:otherwise>
										<td><input type="submit" name="action" value="Restart"
											style="height: 35px; width: 70px"
											${(dataAccessJob.error) ? '' : 'disabled'} /></td>
										<td><input type="submit" name="action" value="Cancel"
											style="height: 35px; width: 70px"
											${(dataAccessJob.preparing || dataAccessJob.ready) ? '' : 'disabled'} /></td>
										<td><input type="submit" name="action"
											value="${(dataAccessJob.paused) ? 'Resume' : 'Pause'}"
											style="height: 35px; width: 70px"
											${(pausable || dataAccessJob.paused)? '' : 'disabled'} /></td>
									</c:otherwise>
								</c:choose>
								<td><input type="submit" name="action" value="Back"
									style="height: 35px; width: 70px" /></td>
							</tr>
						</table>
					</div>
				</sec:authorize>

			</div>
			<%@include file="include/footer.jsp"%>
		</div>

		<input type="hidden" name="requestId" value=${requestId } />

	</form:form>

</body>


</html>
