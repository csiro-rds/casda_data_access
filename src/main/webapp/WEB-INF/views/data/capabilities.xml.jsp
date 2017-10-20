<%@ page contentType="application/xml" %><?xml version="1.0" encoding="UTF-8"?>
<vosi:capabilities
    xmlns:vosi="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:vod="http://www.ivoa.net/xml/VODataService/v1.1">
    <capability standardID="ivo://ivoa.net/std/VOSI#capabilities">
        <interface xsi:type="vod:ParamHTTP" version="1.0">
            <accessURL use="full">
                ${capabilitiesURL}
            </accessURL>
        </interface>
    </capability>
	<capability standardID="ivo://ivoa.net/std/VOSI#availability">
    	<interface xsi:type="vod:ParamHTTP" version="1.0">
	       <accessURL use="full">
                ${availabilityURL}
	       </accessURL>
	   </interface>
	</capability>
    <capability standardID="ivo://ivoa.net/std/SODA#sync-1.0">
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${syncURL}
           </accessURL>
           <queryType>GET</queryType>
           <queryType>POST</queryType>
           <param use="required" std="true">
           		<name>ID</name>
           		<description>publisher dataset identifier</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           	</param>
       </interface>
    </capability>
    <capability standardID="ivo://ivoa.net/std/SODA#async-1.0">
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}
           </accessURL>
           <queryType>GET</queryType>
           <resultType>application/xml</resultType>
           <param use="optional" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
		<interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/phase
           </accessURL>
           <queryType>GET</queryType>
           <resultType>text/plain</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
       <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/phase
           </accessURL>
           <queryType>POST</queryType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
           <param use="required" std="true">
           		<name>phase</name>
           		<description>the phase</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
        	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/owner
           </accessURL>
           <queryType>GET</queryType>
           <resultType>application/json</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       </interface>
       <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/executionduration
           </accessURL>
           <queryType>GET</queryType>
           <resultType>text/plain</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       </interface> 
       <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/quote
           </accessURL>
           <queryType>GET</queryType>
           <resultType>text/plain</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/destruction
           </accessURL>
           <queryType>GET</queryType>
           <resultType>text/plain</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/error
           </accessURL>
           <queryType>GET</queryType>
           <resultType>text/plain</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/results
           </accessURL>
           <queryType>GET</queryType>
           <resultType>application/xml</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/parameters
           </accessURL>
           <queryType>GET</queryType>
           <queryType>POST</queryType>
           <!-- queryType>DELETE</queryType -->
           <resultType>application/xml</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
           <param use="required" std="true">
           		<name>name</name>
           		<description>the parameter name</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       	</interface>
        <interface xsi:type="vod:ParamHTTP" role="std" version="1.0">
           <accessURL use="full">
               ${asyncURL}/{jobId}/parameters/{name}
           </accessURL>
           <queryType>GET</queryType>
           <!-- queryType>DELETE</queryType -->
           <resultType>application/xml</resultType>
           <param use="required" std="true">
           		<name>jobId</name>
           		<description>the data access job id</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
           <param use="required" std="true">
           		<name>name</name>
           		<description>the parameter name</description>
           		<ucd>meta.id;meta.main</ucd>
           		<dataType>string</dataType>
           </param>
       </interface>
    </capability>
</vosi:capabilities>
