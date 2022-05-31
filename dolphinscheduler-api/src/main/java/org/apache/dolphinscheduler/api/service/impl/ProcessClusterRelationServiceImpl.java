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

package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessClusterRelationService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.entity.ClusterProcessDefinitionRelation;
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.dao.mapper.ClusterProcessDefinitionRelationMapper;

import org.apache.commons.collections.CollectionUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

public class ProcessClusterRelationServiceImpl extends BaseServiceImpl implements ProcessClusterRelationService {

    @Autowired
    private ClusterProcessDefinitionRelationMapper relationMapper;

    @Autowired
    private ClusterMapper clusterMapper;

//    /**
//     * update cluster and process relation after process updated, so the auth check process has done
//     * here check cluster code
//     *
//     * @param processDefinitionCode
//     * @param processDefinitionVersion
//     * @param clusterCode
//     * @param clusterParams
//     * @return
//     */
//    @Override
//    public Map<String, Object> updateCluster(Long processDefinitionCode, Integer processDefinitionVersion, Long clusterCode, String clusterParams) {
//        Map<String, Object> result = new HashMap<>();
//
//
//        return null;
//    }

    /**
     * update cluster and process relation after process updated, so the auth check process has done
     * here check cluster code
     *
     * @param processDefinitionCode
     * @param processDefinitionVersion
     * @param clusterLists
     * @return
     */
    @Override
    public Map<String, Object> updateProcessClusterRelation(Long processDefinitionCode, Integer processDefinitionVersion, List<ClusterProcessDefinitionRelation> clusterLists) {
        Map<String, Object> result = new HashMap<>();

        relationMapper.deleteByProcessVersionAndCode(processDefinitionCode, processDefinitionVersion);

        if (CollectionUtils.isEmpty(clusterLists)) {
            result.put(Constants.STATUS, Status.SUCCESS);
            return result;
        }

        //check cluster code
        for (ClusterProcessDefinitionRelation relation : clusterLists) {
            if (relation.getClusterCode() == null) {
                putMsg(result, Status.CLUSTER_NAME_EXISTS, relation.getClusterCode());
                return result;
            }

            Cluster cluster = clusterMapper.queryByClusterCode(relation.getClusterCode());
            if (cluster == null) {
                putMsg(result, Status.QUERY_CLUSTER_BY_CODE_ERROR, relation.getClusterCode());
                return result;
            }
        }

        //insert relations
        for (ClusterProcessDefinitionRelation relation : clusterLists) {
            insertRelation(processDefinitionCode, processDefinitionVersion, relation.getClusterCode(), relation.getClusterParams());
        }

        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }


    @Override
    public Map<String, Object> updateProcessClusterStatus(Long processDefinitionCode, Integer processDefinitionVersion, Long clusterCode, ReleaseState releaseState) {
        Map<String, Object> result = new HashMap<>();

        ClusterProcessDefinitionRelation relation = relationMapper.queryByUniqueKey(clusterCode, processDefinitionCode, processDefinitionVersion);
        if (relation == null) {
            putMsg(result, Status.QUERY_CLUSTER_BY_CODE_ERROR, relation.getClusterCode());

        }

        return null;
    }

    private void insertRelation(Long processDefinitionCode, Integer processDefinitionVersion, Long clusterCode, String clusterParams) {
        ClusterProcessDefinitionRelation relation = new ClusterProcessDefinitionRelation();
        relation.setClusterCode(clusterCode);
        relation.setProcessDefinitionCode(processDefinitionCode);
        relation.setProcessDefinitionVersion(processDefinitionVersion);
        relation.setClusterParams(clusterParams);
        relation.setReleaseState(ReleaseState.OFFLINE);
        relation.setCreateTime(new Date());
        relation.setUpdateTime(new Date());
        relationMapper.insert(relation);
    }


}
