<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page session="false" %>
<html>
<head>
	<title>Casda Data Access Home</title>
	<%@include file="include/head.jsp" %>
</head>

<body>
	<div id="wrapper">
		<div id="content">
			<%@include file="include/header.jsp" %>
			<h2 class="indentLeft">Casda Data Access</h2>
			<fieldset>
				<legend>CASDA Data Access</legend>		
				<div class="floatLeft">
				  <ul>
					<li><a href="jobs">Job Queue</a></li>
					<li><a href="queuedJobs">Processing Queue</a></li>
					<li><a href="sdoc.jsp">API Documentation</a></li>
				  </ul>
				</div>
			</fieldset>
			<%@include file="include/footer.jsp" %>
		</div>
	</div>
</body>

</html>
