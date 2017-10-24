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

<title>CSIRO Data Access Portal - CASDA Access Request Status - ${date}</title>
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

$(document).ready(function(){
	$("#BookmarkIE").click(function(e){
		e.preventDefault();
		var bookmarkUrl = window.location.href;
		var bookmarkTitle = "CSIRO Data Access Portal - CASDA Access Request Status - ${date}";
	 
		if( window.external || document.all) 
		{
			window.external.AddFavorite( bookmarkUrl, bookmarkTitle);
		}
	});
	});
	
	function choosePostfix()
	{
		var number = ${position}
		if(number == 1 || (number.toString().charAt(number.toString().length-2) != '1' && number.toString().charAt(number.toString().length-1) == '1'))
		{
			return number+'st'
		}
		else if(number == 2 || (number.toString().charAt(number.toString().length-2) != '1' && number.toString().charAt(number.toString().length-1) == '2'))
		{
			return number+'nd'
		}
		else if(number == 3 || (number.toString().charAt(number.toString().length-2) != '1' && number.toString().charAt(number.toString().length-1) == '3'))
		{
			return number+'rd'
		}
		else
		{
			return number+'th'
		}
	}
</script>


</head>

<body>
<c:set var="browser" value="${header['User-Agent']}"/>

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
			<div class="refreshBox queued">
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
							<c:when test="${(dataAccessJob.preparing || dataAccessJob.paused) && position == 0}">
								<div class="statusBox preparing">
									<p>Preparing your data for retrieval</p>
								</div>
							</c:when>
							<c:when test="${(dataAccessJob.preparing || dataAccessJob.paused) && position > 0}">
								<div class="statusBox queued">
									<p>Your request is currently
									<script type="text/javascript">
									document.writeln(choosePostfix());
						            </script>
									in the queue</p>
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

										<h2>Download individual files</h2>

									</c:when>
									<c:otherwise>
										<h2>Download individual files</h2>
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
								<col />
							</colgroup>

							<tr class="tableHeader">
								<td>Name</td>
								<td>Size (kB)</td>
								<td>Link</td>
								<td>Checksum</td>
							</tr>

							<c:forEach var="downloadFile" items="${downloadFiles}" varStatus="rowCounter">
								<c:choose>
									<c:when test="${rowCounter.count % 2 == 0}">
										<c:set var="rowStyle" scope="page" value="even" />
									</c:when>
									<c:otherwise>
										<c:set var="rowStyle" scope="page" value="odd" />
									</c:otherwise>
								</c:choose>
								<tr class="${rowStyle}">
									<td>${downloadFile.displayName }</td>
									<td>${downloadFile.sizeKb}</td>
									<td><c:choose>
											<c:when test="${dataAccessJob.ready or errorOnly}">
												<s:eval
													expression="T(au.csiro.casda.access.DataAccessUtil).getRelativeLinkForFile(dataAccessJob.downloadMode, dataAccessJob.requestId, downloadFile.filename)"
													var="link" />
												<a href="${baseUrl}${link}">${downloadFile.filename }</a>
											</c:when>
											<c:otherwise>Unavailable</c:otherwise>
										</c:choose>
									</td>
									<td>
									<c:choose>
											<c:when test="${dataAccessJob.ready or errorOnly}">
												<s:eval
													expression="T(au.csiro.casda.access.DataAccessUtil).getRelativeLinkForFileChecksum(dataAccessJob.downloadMode, dataAccessJob.requestId, downloadFile.filename)"
													var="link" />
												<a href="${baseUrl}${link}">Available</a>
											</c:when>
											<c:otherwise>Unavailable</c:otherwise>
										</c:choose>
									</td>
								</tr>
							</c:forEach>

						</table>
						<form:form name="changePageForm" method="post" action="changePage">
						<table style="margin: auto; width: 50%">
							<tr>
								<c:if test="${currentPage > 1}">
									<td>
										<input type="submit" name="action" title="Back to first page" value="|&lt;" style="height: 40px; width:40px"/>
									</td>
								</c:if>
								<c:if test="${currentPage >= 11}">
									<td>
										<input type="submit" name="action" title="Back 10 pages" value="&lt;&lt;" style="height: 40px; width:40px"/>
									</td>
								</c:if>
								<c:if test="${currentPage > 1}">
									<td>
										<input type="submit" name="action" title="Back 1 page" value="&lt;" style="height: 40px; width:40px"/>
									</td>
								</c:if>
								<c:if test="${lastPage != 1}">
									<td style="width=100%">${currentPage}</td>
								</c:if>
								<c:if test="${lastPage != currentPage}">
									<td>
										<input type="submit" name="action" title="Forward 1 pages" value="&gt;" style="height: 40px; width:40px"/>
									</td>
								</c:if>
								<c:if test="${lastPage-currentPage >= 10}">
									<td>
										<input type="submit" name="action" title="Forward 10 pages" value="&gt;&gt;" style="height: 40px; width:40px"/>
									</td>
								</c:if>
								<c:if test="${lastPage != currentPage}">
									<td>
										<input type="submit" name="action" title="Forward to last page" value="&gt;|" style="height: 40px; width:40px"/>
									</td>
								</c:if>
							</tr>				
						</table>
						<input type="hidden" name="currentPage" value=${currentPage} />
						</form:form>
						<table style="margin-top:10px">
							<tr>
								<td>
									<c:if test="${browser.contains('Firefox')}">
										<a id="bookmarkFF" href="" title="CSIRO Data Access Portal - CASDA Access Request Status - ${date}" rel="sidebar" class="downloadLink">
											Bookmark this page
										</a>
										<script>
											document.getElementById("bookmarkFF").href=window.location.href;
										</script>
									</c:if>
									<c:if test="${browser.contains('MSIE') || browser.contains('Trident') || browser.contains('Edge')}">
										<a id="BookmarkIE" class="downloadLink" href="">Bookmark this page</a>
									</c:if>
								</td>
								<td>
									<c:choose>	
										<c:when test="${dataAccessJob.ready}">
											<span class="downloadLink" style="padding-bottom: 5px;">|</span>
										</c:when>
									</c:choose>
									
								</td>
								<td>
									<c:choose>	
										<c:when test="${dataAccessJob.ready}">
											<a href="${downloadLink}" class="downloadLink" title="Click to download the urls of the above files in a single text file.">Save links as text file</a>																					
										</c:when>
									</c:choose>
								</td>
							</tr>
							<tr style="border-width:0px">
								<td></td><td></td>
								<td>
									<c:choose>
										<c:when test="${pawsey}">
	
											<h2>Next Steps</h2>
											
											<ul class="disc">
											    <li>Login to a Pawsey server such as galaxy or magnus using your Pawsey account details</li>
											    <li>Navigate to the desired path</li>
											    <li>To transfer these files to Pawsey, try using the 'wget' or the 'xargs' unix commands with the text file above.</li>
											    <li>For more detailed information refer to the CASDA User Guide here: 
											    	<a href="http://www.atnf.csiro.au/observers/data/casdaguide.html" target="_blank" >www.atnf.csiro.au/observers/data/casdaguide.html</a></li>
											</ul>
	
										</c:when>
									</c:choose>
								</td>
							</tr>
						</table>
					</div>
				</fieldset>

				<sec:authorize access="hasRole('ROLE_CASDA_ADMIN')">
				<form:form name='modifyJobForm' method="post" action="${pageContext.request.contextPath}/requests/modifyJob">
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
					<input type="hidden" name="requestId" value=${requestId } />
				</form:form>
				</sec:authorize>
									
			</div>
			<%@include file="include/footer.jsp"%>
		</div>


</body>


</html>
