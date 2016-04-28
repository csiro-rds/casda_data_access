<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt"%>
<%@taglib prefix="s" uri="http://www.springframework.org/tags"%>
<%@taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<fieldset>
	<legend>${tabletitle } </legend>

	<br />

	<div class="floatLeft">

        <p>
			<c:forEach var="queue" items="${pausableQueues}">
				<c:choose>
					<c:when test="${!queue.paused && showpause == 'true'}">
						<input type="submit" name="action"
							value="Pause ${queue.name} Queue"
							style="height: 35px; width: 200px"
							onclick="DoSubmit('${queue.name}')" />
						${queue.description}<br/>
					</c:when>
					<c:when test="${queue.paused && showpause == 'true'}">
						<input type="submit" name="action"
							value="Resume ${queue.name} Queue"
							style="height: 35px; width: 200px"
							onclick="DoSubmit('${queue.name}')" />
						${queue.description}<br/>
					</c:when>
					<c:otherwise>
						&nbsp;
					</c:otherwise>
				</c:choose>
            </c:forEach>
		</p>
		<table class="jobsTable">

			<colgroup>
				<col />
				<col />
				<col />
				<col />
				<col />
			</colgroup>

			<tr class="tableHeader">
				<td>Request Id</td>
				<c:choose>
					<c:when test="${showqueue == 'true' }">
						<td>Queue</td>
					</c:when>
				</c:choose>
				<c:choose>
					<c:when test="${showstatus != ''}">
						<td>Status</td>
					</c:when>
				</c:choose>
				<td>Date Requested</td>
				<c:choose>
					<c:when test="${showdate == 'completed' }">
						<td>Date Completed</td>
					</c:when>
					<c:when test="${showdate == 'failed' }">
						<td>Date Failed</td>
					</c:when>
				</c:choose>
				<td>User Name</td>
				<td>User Email</td>
				<td>Access Method</td>
				<td>Format</td>
				<c:if test="${updatePriority}">
					<td colspan="2">Update Priority</td>
				</c:if>
			</tr>

			<c:forEach var="job" items="${thelist}" varStatus="rowCounter">
				<c:choose>
					<c:when test="${rowCounter.count % 2 == 0}">
						<c:set var="rowStyle" scope="page" value="even" />
					</c:when>
					<c:otherwise>
						<c:set var="rowStyle" scope="page" value="odd" />
					</c:otherwise>
				</c:choose>
				<c:choose>
					<c:when test="${job.uwsStatus == 'COMPLETED'}">
						<c:set var="statusTitle" scope="page"
							value="Start: ${job.formattedDateStarted}, Finish: ${job.formattedDateCompleted}; Took: ${job.executionDuration}s" />
					</c:when>
					<c:when test="${job.uwsStatus == 'EXECUTING'}">
						<c:set var="statusTitle" scope="page"
							value="Start: ${job.formattedDateStarted}" />
					</c:when>
					<c:otherwise>
						<c:set var="statusTitle" scope="page" value="" />
					</c:otherwise>
				</c:choose>

				<tr class="${rowStyle}">
					<td><a href="<c:url value='/requests/${job.requestId}'/>">${job.requestId}</a></td>
					<c:choose>
						<c:when test="${showqueue == 'true' }">
							<td>${job.queue}</td>
						</c:when>
					</c:choose>
					<c:choose>
						<c:when test="${showstatus == 'uws'}">
							<td title="${statusTitle}">${job.uwsStatus}</td>
						</c:when>
						<c:when test="${showstatus == 'db'}">
                            <td title="${statusTitle}">${job.status}</td>
                        </c:when>
					</c:choose>
					<td>${job.formattedDateRequested}</td>
					<c:choose>
					    <c:when test="${showdate == 'completed' }">
					        <td>${job.formattedDateCompleted }</td>
					    </c:when>
					    <c:when test="${showdate == 'failed' }">
                            <td>${job.formattedDateFailed }</td>
                        </c:when>
                    </c:choose>
					<td>${job.user}</td>
					<td>${job.email }</td>
					<td>${job.accessMethod}</td>
					<td>${job.format}</td>
					
					<!-- START: for Job queue section only -->
					<c:choose>						
						<c:when test="${updatePriority && (fn:toLowerCase(job.uwsStatus) == 'queued')}">
							<td><input type="submit" name="action" value="First"
								style="height: 35px; width: 60px"
								onclick="DoSubmit('${job.requestId}')" /></td>
							<td><input type="submit" name="action" value="Last"
								style="height: 35px; width: 60px"
								onclick="DoSubmit('${job.requestId}')" /></td>
						</c:when>
						<c:otherwise>
							<td>&nbsp;</td>
							<td>&nbsp;</td>
						</c:otherwise>
					</c:choose>
					<!-- END: for Job queue section only -->
				</tr>
			</c:forEach>
		</table>
	</div>
</fieldset>