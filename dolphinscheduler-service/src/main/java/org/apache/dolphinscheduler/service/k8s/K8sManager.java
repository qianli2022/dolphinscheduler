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

package org.apache.dolphinscheduler.service.k8s;

import org.apache.dolphinscheduler.common.utils.ClusterConfUtils;
import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.entity.K8s;
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.dao.mapper.K8sMapper;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * A separate class, because then wait for multiple environment feature, currently using db configuration, later unified
 */
@Component
public class K8sManager {

    private static final Logger logger = LoggerFactory.getLogger(K8sManager.class);
    /**
     * cache k8s client
     */
    private static Map<String, KubernetesClient> clientMap = new Hashtable<>();

    @Autowired
    private K8sMapper k8sMapper;

    private ClusterMapper clusterMapper;

    public KubernetesClient getK8sClient(String k8sName) {
        if (null == k8sName) {
            return null;
        }
        return clientMap.get(k8sName);
    }

    @EventListener
    public void buildApiClientAll(ApplicationReadyEvent readyEvent) throws RemotingException {
        List<Cluster> clusterList = clusterMapper.selectList(null);
        if (clusterList != null && clusterList.size() > 0) {
            for(Cluster item :clusterList ) {
                String k8sConfig = ClusterConfUtils.getK8sConfig(item.getConfig());
                if(k8sConfig!=null)
                {
                    DefaultKubernetesClient client = getClient(k8sConfig);
                    clientMap.put(item.getName(), client);
                }
            }
        }
    }

    private DefaultKubernetesClient getClient(String configYaml) throws RemotingException {
        try {
            Config config = Config.fromKubeconfig(configYaml);
            return new DefaultKubernetesClient(config);
        } catch (Exception e) {
            logger.error("fail to get k8s ApiClient", e);
            throw new RemotingException("fail to get k8s ApiClient:" + e.getMessage());
        }
    }

    private List<String> getK8sList() {
        List<Cluster> clusterList = clusterMapper.selectList(null);
        List<String> k8sConfigs = new ArrayList<>();
        if (clusterList != null && clusterList.size() > 0) {
            for(Cluster item :clusterList ) {
                String k8sConfig = ClusterConfUtils.getK8sConfig(item.getConfig());
                k8sConfigs.add(k8sConfig);
            }
        }
        return k8sConfigs;
    }
}
