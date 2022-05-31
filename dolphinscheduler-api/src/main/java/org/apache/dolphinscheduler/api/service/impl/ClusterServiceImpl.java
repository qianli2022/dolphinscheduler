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
import org.apache.dolphinscheduler.api.service.ClusterService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.ClusterConfUtils;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils;
import org.apache.dolphinscheduler.common.utils.CodeGenerateUtils.CodeGenerateException;
import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.entity.ClusterProcessDefinitionRelation;
import org.apache.dolphinscheduler.dao.entity.K8sNamespace;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.dao.mapper.ClusterProcessDefinitionRelationMapper;
import org.apache.dolphinscheduler.dao.mapper.K8sNamespaceMapper;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.service.k8s.K8sManager;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;

/**
 * cluster definition service impl
 */
@Service
public class ClusterServiceImpl extends BaseServiceImpl implements ClusterService {

    private static final Logger logger = LoggerFactory.getLogger(ClusterServiceImpl.class);

    @Autowired
    private ClusterMapper clusterMapper;

    @Autowired
    private ClusterProcessDefinitionRelationMapper relationMapper;

    @Autowired
    private K8sManager k8sManager;

    @Autowired
    private K8sNamespaceMapper k8sNamespaceMapper;

    /**
     * create cluster
     *
     * @param loginUser login user
     * @param name      cluster name
     * @param config    cluster config
     * @param desc      cluster desc
     */
    @Transactional(rollbackFor = RuntimeException.class)
    @Override
    public Map<String, Object> createCluster(User loginUser, String name, String config, String desc) {
        Map<String, Object> result = new HashMap<>();
        if (isNotAdmin(loginUser, result)) {
            return result;
        }

        Map<String, Object> checkResult = checkParams(name, config);
        if (checkResult.get(Constants.STATUS) != Status.SUCCESS) {
            return checkResult;
        }

        Cluster clusterExistByName = clusterMapper.queryByClusterName(name);
        if (clusterExistByName != null) {
            putMsg(result, Status.CLUSTER_NAME_EXISTS, name);
            return result;
        }

        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setConfig(config);
        cluster.setDescription(desc);
        cluster.setOperator(loginUser.getId());
        cluster.setCreateTime(new Date());
        cluster.setUpdateTime(new Date());
        long code = 0L;
        try {
            code = CodeGenerateUtils.getInstance().genCode();
            cluster.setCode(code);
        } catch (CodeGenerateException e) {
            logger.error("Cluster code get error, ", e);
        }
        if (code == 0L) {
            putMsg(result, Status.INTERNAL_SERVER_ERROR_ARGS, "Error generating cluster code");
            return result;
        }

        if (clusterMapper.insert(cluster) > 0) {
            result.put(Constants.DATA_LIST, cluster.getCode());
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.CREATE_CLUSTER_ERROR);
        }
        return result;
    }

    /**
     * query cluster paging
     *
     * @param pageNo    page number
     * @param searchVal search value
     * @param pageSize  page size
     * @return cluster list page
     */
    @Override
    public Result queryClusterListPaging(Integer pageNo, Integer pageSize, String searchVal) {
        Result result = new Result();

        Page<Cluster> page = new Page<>(pageNo, pageSize);

        IPage<Cluster> clusterIPage = clusterMapper.queryClusterListPaging(page, searchVal);

        PageInfo<Cluster> pageInfo = new PageInfo<>(pageNo, pageSize);
        pageInfo.setTotal((int) clusterIPage.getTotal());
        pageInfo.setTotalList(clusterIPage.getRecords());
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query all cluster
     *
     * @return all cluster list
     */
    @Override
    public Map<String, Object> queryAllClusterList() {
        Map<String, Object> result = new HashMap<>();
        List<Cluster> clusterList = clusterMapper.queryAllClusterList();
        result.put(Constants.DATA_LIST, clusterList);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * query cluster
     *
     * @param code cluster code
     */
    @Override
    public Map<String, Object> queryClusterByCode(Long code) {
        Map<String, Object> result = new HashMap<>();

        Cluster cluster = clusterMapper.queryByClusterCode(code);
        if (cluster == null) {
            putMsg(result, Status.QUERY_CLUSTER_BY_CODE_ERROR, code);
        } else {
            result.put(Constants.DATA_LIST, cluster);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * query cluster
     *
     * @param name cluster name
     */
    @Override
    public Map<String, Object> queryClusterByName(String name) {
        Map<String, Object> result = new HashMap<>();

        Cluster cluster = clusterMapper.queryByClusterName(name);
        if (cluster == null) {
            putMsg(result, Status.QUERY_CLUSTER_BY_NAME_ERROR, name);
        } else {
            result.put(Constants.DATA_LIST, cluster);
            putMsg(result, Status.SUCCESS);
        }
        return result;
    }

    /**
     * delete cluster
     *
     * @param loginUser login user
     * @param code      cluster code
     */
    @Transactional(rollbackFor = RuntimeException.class)
    @Override
    public Map<String, Object> deleteClusterByCode(User loginUser, Long code) {
        Map<String, Object> result = new HashMap<>();
        if (isNotAdmin(loginUser, result)) {
            return result;
        }

//        Integer relatedProcessNumber = relationMapper
//            .selectCount(new QueryWrapper<ClusterProcessDefinitionRelation>().lambda().eq(ClusterProcessDefinitionRelation::getClusterCode, code));
//
//        if (relatedProcessNumber > 0) {
//            putMsg(result, Status.DELETE_CLUSTER_RELATED_TASK_EXISTS);
//            return result;
//        }

        Integer relatedNamespaceNumber = k8sNamespaceMapper
            .selectCount(new QueryWrapper<K8sNamespace>().lambda().eq(K8sNamespace::getClusterCode, code));

        if (relatedNamespaceNumber > 0) {
            putMsg(result, Status.DELETE_CLUSTER_RELATED_NAMESPACE_EXISTS);
            return result;
        }

        int delete = clusterMapper.deleteByCode(code);
        if (delete > 0) {
            relationMapper.delete(new QueryWrapper<ClusterProcessDefinitionRelation>()
                .lambda()
                .eq(ClusterProcessDefinitionRelation::getClusterCode, code));
            putMsg(result, Status.SUCCESS);
        } else {
            putMsg(result, Status.DELETE_CLUSTER_ERROR);
        }
        return result;
    }


    /**
     * update cluster
     *
     * @param loginUser login user
     * @param code      cluster code
     * @param name      cluster name
     * @param config    cluster config
     * @param desc      cluster desc
     */
    @Transactional(rollbackFor = RuntimeException.class)
    @Override
    public Map<String, Object> updateClusterByCode(User loginUser, Long code, String name, String config, String desc) {
        Map<String, Object> result = new HashMap<>();
        if (isNotAdmin(loginUser, result)) {
            return result;
        }

        Map<String, Object> checkResult = checkParams(name, config);
        if (checkResult.get(Constants.STATUS) != Status.SUCCESS) {
            return checkResult;
        }

        Cluster clusterExistByName = clusterMapper.queryByClusterName(name);
        if (clusterExistByName != null && !clusterExistByName.getCode().equals(code)) {
            putMsg(result, Status.CLUSTER_NAME_EXISTS, name);
            return result;
        }

        Cluster clusterExist = clusterMapper.queryByClusterCode(code);
        if (clusterExist == null) {
            putMsg(result, Status.CLUSTER_NOT_EXISTS, name);
            return result;
        }

        //need update namespace name
        if (!clusterExist.getName().equals(name)) {
            k8sNamespaceMapper.updateNamespaceClusterName(clusterExist.getCode(), clusterExist.getName());
        }
        //update cluster
        clusterExist.setConfig(config);
        clusterExist.setName(name);
        clusterMapper.updateById(clusterExist);
        //need not update relation

        //k8s config not change,need not update
        if (!Constants.K8S_LOCAL_TEST_CLUSTER.equals(name)
            && !config.equals(ClusterConfUtils.getK8sConfig(clusterExist.getConfig()))) {
            try {
                k8sManager.getAndUpdateK8sClient(code, true);
            } catch (RemotingException e) {
                putMsg(result, Status.K8S_CLIENT_OPS_ERROR, name);
                return result;
            }
        }

        putMsg(result, Status.SUCCESS);
        return result;
    }

    /**
     * verify cluster name
     *
     * @param clusterName cluster name
     * @return true if the cluster name not exists, otherwise return false
     */
    @Override
    public Map<String, Object> verifyCluster(String clusterName) {
        Map<String, Object> result = new HashMap<>();

        if (StringUtils.isEmpty(clusterName)) {
            putMsg(result, Status.CLUSTER_NAME_IS_NULL);
            return result;
        }

        Cluster cluster = clusterMapper.queryByClusterName(clusterName);
        if (cluster != null) {
            putMsg(result, Status.CLUSTER_NAME_EXISTS, clusterName);
            return result;
        }

        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<String, Object> queryClusterListByProcessCodeVersion(Long processCode, Integer processVersion) {
        QueryWrapper<ClusterProcessDefinitionRelation> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("process_definition_code", processCode)
            .eq("process_definition_version", processVersion);

        List<ClusterProcessDefinitionRelation> clusterProcessDefinitionRelationList = relationMapper.selectList(queryWrapper);
        if (CollectionUtils.isNotEmpty(clusterProcessDefinitionRelationList)) {
            List<Cluster> clusterList = clusterMapper.queryAllClusterList();
            Map<Long, String> clusterCodeNameMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(clusterList)) {
                for (Cluster cluster : clusterList) {
                    clusterCodeNameMap.put(cluster.getCode(), cluster.getName());
                }
            }
            for (ClusterProcessDefinitionRelation relation : clusterProcessDefinitionRelationList) {
                relation.setClusterName(clusterCodeNameMap.get(relation.getClusterCode()));
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.DATA_LIST, clusterProcessDefinitionRelationList);

        return result;
    }

    private Map<String, Object> checkUsedClusterProcessDefinitionRelation(Set<String> deleteKeySet, String clusterName, Long clusterCode) {
        Map<String, Object> result = new HashMap<>();
        for (String processDefinition : deleteKeySet) {
            ClusterProcessDefinitionRelation clusterProcessDefinitionRelation = relationMapper
                .selectOne(new QueryWrapper<ClusterProcessDefinitionRelation>().lambda()
                    .eq(ClusterProcessDefinitionRelation::getClusterCode, clusterCode)
                    .eq(ClusterProcessDefinitionRelation::getProcessDefinitionCode, processDefinition));

            if (Objects.nonNull(clusterProcessDefinitionRelation)) {
                putMsg(result, Status.UPDATE_CLUSTER_PROCESS_DEFINITION_RELATION_ERROR, processDefinition, clusterName, clusterProcessDefinitionRelation.getProcessDefinitionCode());
                return result;
            }
        }
        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }

    public Map<String, Object> checkParams(String name, String config) {
        Map<String, Object> result = new HashMap<>();
        if (StringUtils.isEmpty(name)) {
            putMsg(result, Status.CLUSTER_NAME_IS_NULL);
            return result;
        }
        if (StringUtils.isEmpty(config)) {
            putMsg(result, Status.CLUSTER_CONFIG_IS_NULL);
            return result;
        }
        result.put(Constants.STATUS, Status.SUCCESS);
        return result;
    }

}

