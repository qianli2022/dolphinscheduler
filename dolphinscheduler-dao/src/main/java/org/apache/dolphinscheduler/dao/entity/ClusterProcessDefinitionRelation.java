/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.dao.entity;

import org.apache.dolphinscheduler.common.enums.ReleaseState;

import java.util.Date;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * ClusterProcessDefinitionRelation
 */
@TableName("t_ds_cluster_process_definition_relation")
public class ClusterProcessDefinitionRelation {

    @TableId(value = "id", type = IdType.AUTO)
    private int id;

    /**
     * cluster code
     */
    private Long clusterCode;

    /**
     * cluster name
     */
    @TableField(exist = false)
    private String clusterName;

    /**
     * process definition code
     */
    private Long processDefinitionCode;

    /**
     * process definition version
     */
    private int processDefinitionVersion;

    /**
     * cluster parameters,json,overwrite process clusterParams
     */
    private String clusterParams;

    /**
     * release state : online/offline,need set process state too
     */
    private ReleaseState releaseState;

    private Date createTime;

    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Long getProcessDefinitionCode() {
        return processDefinitionCode;
    }

    public void setProcessDefinitionCode(Long processDefinitionCode) {
        this.processDefinitionCode = processDefinitionCode;
    }

    public Long getClusterCode() {
        return this.clusterCode;
    }

    public void setClusterCode(Long clusterCode) {
        this.clusterCode = clusterCode;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public String toString() {
        return "ClusterProcessDefinitionRelation{"
            + "id= " + id
            + ", clusterCode= " + clusterCode
            + ", processDefinitionCode= " + processDefinitionCode
            + ", processDefinitionVersion= " + processDefinitionVersion
            + ", clusterParams= " + clusterParams
            + ", releaseState= " + releaseState
            + ", createTime= " + createTime
            + ", updateTime= " + updateTime
            + "}";
    }

    public int getProcessDefinitionVersion() {
        return processDefinitionVersion;
    }

    public void setProcessDefinitionVersion(int processDefinitionVersion) {
        this.processDefinitionVersion = processDefinitionVersion;
    }

    public String getClusterParams() {
        return clusterParams;
    }

    public void setClusterParams(String clusterParams) {
        this.clusterParams = clusterParams;
    }

    public ReleaseState getReleaseState() {
        return releaseState;
    }

    public void setReleaseState(ReleaseState releaseState) {
        this.releaseState = releaseState;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}
