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

import java.util.Date;

import com.baomidou.mybatisplus.annotation.IdType;
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
     * worker group id
     */
    private String processDefinition;

    /**
     * operator user id
     */
    private Integer operator;

    private Date createTime;

    private Date updateTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getProcessDefinition() {
        return processDefinition;
    }

    public void setProcessDefinition(String processDefinition) {
        this.processDefinition = processDefinition;
    }

    public Long getClusterCode() {
        return this.clusterCode;
    }

    public void setClusterCode(Long clusterCode) {
        this.clusterCode = clusterCode;
    }

    public Integer getOperator() {
        return this.operator;
    }

    public void setOperator(Integer operator) {
        this.operator = operator;
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
                + ", processDefinition= " + processDefinition
                + ", operator= " + operator
                + ", createTime= " + createTime
                + ", updateTime= " + updateTime
                + "}";
    }

}
