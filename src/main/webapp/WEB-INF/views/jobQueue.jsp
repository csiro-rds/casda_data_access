<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@taglib prefix="s" uri="http://www.springframework.org/tags"%>
<%@ taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>

<%@ page session="false"%>
<%--
 jobqueue.jsp: Displays the list of recent data access jobs.

  Copyright 2015, CSIRO Australia
  All rights reserved.

 --%>
<html>
<head>
<title>CSIRO Data Access Portal - CASDA Access Job Queue</title>
<%@include file="include/head.jsp"%>
</head>
<script type="text/javascript" src="/casda_data_access/js/jquery-2.2.1.min.js"></script>
<script src="/casda_data_access/js/jquery.loading-indicator.min.js"></script>
<link rel="stylesheet" type="text/css" href="<c:url value='/css/loading.css'/>" />


<body>
	<form:form id="modifyJobForm" name='modifyJobForm' method="post"
		action="requests/modifyJob">

		<input type="hidden" id="requestId" name="requestId" value="" />

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

			<div id="content">
				<div class="indentRight">
					<a href="/casda_data_access/queuedJobs">Processing Queue</a>&nbsp;|&nbsp;<a  href="javascript:clearAllCache();">delete all cache</a>&nbsp;|&nbsp;<a href="/casda_data_access/logout">logout</a>
				</div>
				<br>

				<c:set var="thelist" scope="request" value="${jobList }" />
				<c:set var="tabletitle" scope="request" value="Current Access Requests" />
				<c:set var="showpause" scope="request" value="true" />
				<c:set var="showstatus" scope="request" value="uws" />
				<c:set var="showqueue" scope="request" value="true" />
				<c:set var="updatePriority" scope="request" value="true" />
				<%@include file="include/joblist.jsp"%>

				<c:set var="thelist" scope="request" value="${available }" />
				<c:set var="tabletitle" scope="request"
					value="Completed Jobs (Last ${availabledays } days)" />
					<c:set var="showpause" scope="request" value="false" />
				<c:set var="showstatus" scope="request" value="db" />
				<c:set var="showqueue" scope="request" value="false" />
				<c:set var="showdate" scope="request" value="completed" />
				<c:set var="updatePriority" scope="request" value="false" />
				<%@include file="include/joblist.jsp"%>

				<c:set var="thelist" scope="request" value="${failed }" />
				<c:set var="tabletitle" scope="request"
					value="Failed Jobs (Last ${faileddays } days)" />
					<c:set var="showpause" scope="request" value="false" />
				<c:set var="showstatus" scope="request" value="" />
				<c:set var="showqueue" scope="request" value="true" />
				<c:set var="showdate" scope="request" value="failed" />
				<c:set var="updatePriority" scope="request" value="false" />
				<%@include file="include/joblist.jsp"%>

			</div>
			<%@include file="include/footer.jsp"%>
		</div>

	</form:form>

</body>

<script type="text/javascript">

	function DoSubmit(reqId) {
		var homeLoader = $('body').loadingIndicator({
			useImage: false,
		}).data("loadingIndicator");
		document.getElementById("requestId").value = reqId;
		modifyJobForm.submit;
		return false;
	}
	
	function clearAllCache()
	{
		if(confirm('This action will delete all cached files and mark all running and completed jobs to expired. Are you sure you want to continue?') &&
			confirm('This action is irreversible. Are you really sure you want to continue?'))
		{
		
			var homeLoader = $('body').loadingIndicator({
				useImage: false,
			}).data("loadingIndicator");
			var input = $("<input>")
	            .attr("type", "hidden")
	            .attr("name", "action").val("deleteAllCache");
				$('#modifyJobForm').append($(input));
			document.getElementById('modifyJobForm').submit();
		}
		return false;		
	}
</script>

</html>
