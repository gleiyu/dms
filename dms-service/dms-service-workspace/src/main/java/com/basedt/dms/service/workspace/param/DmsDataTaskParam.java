/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.basedt.dms.service.workspace.param;

import com.basedt.dms.service.base.param.PaginationParam;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import jakarta.validation.constraints.NotNull;

@Data
@EqualsAndHashCode(callSuper = true)
public class DmsDataTaskParam extends PaginationParam {

    @NotNull
    private Long workspaceId;

    private Long datasourceId;

    private String fileName;

    @NotNull
    private String taskType;

    private String taskStatus;

    private String creator;

    @Schema(name = "createTimeFrom", title = "create time from")
    private String createTimeFrom;

    @Schema(name = "createTimeTo", title = "create time to")
    private String createTimeTo;

}
