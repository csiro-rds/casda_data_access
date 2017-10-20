<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<div id="header">

<c:if test="${not empty trackerId}">
	<script>
	  (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
	  (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
	  m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
	  })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
	
	  ga('create', '${trackerId}', 'auto');
	  ga('send', 'pageview');
	
	</script>
</c:if>


	<div class="screen-grey-out"></div>
    <div class="form-holder"></div>
    
    <div class="wrapper"></div>
            
	<header role="banner">
	    <div class="banner-container"></div>
	    <div class="header-container">
	        <a href="<c:url value='/'/>" class="logo">
	            <img src="<c:url value='/img/logo.png'/>" alt="CSIRO Logo" width="80" height="80" />
	        </a>
			<div class="top-nav">
				<div class="sub-navigation">
					<ul>
						<li id="ctl14_ctl01_rpt_li_0"><a target="dapLinks"
							href="https://data.csiro.au/dap/home?execution=e1s1">DATA ACCESS PORTAL</a></li>
						<li id="ctl14_ctl01_rpt_li_0"><a target="dapLinks"
							href="http://www.atnf.csiro.au/">CASDA</a></li>
						<li id="ctl14_ctl01_rpt_li_1"><a target="dapLinks"
							href="http://www.csiro.au/en/News">News</a></li>
						<li id="ctl14_ctl01_rpt_li_2"><a target="dapLinks"
							href="https://publications.csiro.au/rpr/home">Publications</a></li>
					</ul>
				</div>
			</div>
		
		</div>
	</header>
</div>

