<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>
<%@taglib prefix="s" uri="http://www.springframework.org/tags"%>
<%@ taglib uri="http://www.springframework.org/tags/form" prefix="form"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn"%>
<%@ page session="false" %>
<html>
<head>
	<title>CSIRO Data Access Portal - Image Cutout</title>
	<%@include file="include/head.jsp" %>
</head>

<body>
	<div id="wrapper">
	
		<%@include file="include/header.jsp" %>
		<div class="main-content">
			<h2>Image Cutout</h2>
			<c:if test="${not empty error}">
				<c:set var="flashMessage" value="${error}" />
				<c:set var="flashBackgroundColour" value="#FBC8C8" />
				<div
					style="margin: 50px; padding: 10px; border: 1px solid black; background-color: ${flashBackgroundColour};">
					${flashMessage}</div>
			</c:if>
			<form:form id="cutoutForm" name='cutoutForm' method="post"
				action="cutoutui">
                   <input type="hidden" name="ID" value="${id}"/>
                   <div class="formContent">
                       <label>Scheduling Block ID</label>
                       ${sbid}
                       <br/>
                   </div>
                   <div class="formContent">
                       <label>Image</label>
                       ${imageName}
                       <br/>
                   </div>
                   <div class="formContent">
                       <label for="format">Right Ascension</label>
                       <input type='text' name='ra' value='${centreRa}'>
                       (decimal degrees)
                       <br/>
                   </div>
                   <div class="formContent">
                       <label for="format">Declination</label>
                       <input type='text' name='dec' value='${centreDec}'>
                       (decimal degrees)
                       <br/>
                   </div>
                   <div class="formContent">
                       <label for="format">Radius</label>
                       <input type='text' name='radius' value='1.0'>
                       (decimal degrees)
                       <br/>
                   </div>
                   <div class="formContent">
                       <label >&nbsp;</label>
                       <input type="submit" value="Submit" style="width:auto;">
                       <br/>
                   </div>
			</form:form>
            <br/>

        </div>
	    <%@include file="include/footer.jsp" %>
    </div>

</body>
</html>
