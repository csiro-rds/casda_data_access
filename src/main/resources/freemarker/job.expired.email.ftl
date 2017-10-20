<#--
 #%L
 CSIRO Data Access Portal
 %%
 Copyright (C) 2010 - 2012 Commonwealth Scientific and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
 %%
 Licensed under the CSIRO Open Source License Agreement (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License in the LICENSE file.
 #L%
-->
<#include "csiroEmailHeader.ftl">

<p>Dear ${userName},</p>

<p>Data Access Job ${jobID} has expired. This means access to these files can no longer be guaranteed, they will be removed when space is needed in the cache.
<#include "link.ftl">

<#include "csiroEmailFooter.ftl">