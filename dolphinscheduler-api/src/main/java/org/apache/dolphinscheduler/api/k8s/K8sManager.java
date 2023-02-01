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

package org.apache.dolphinscheduler.api.k8s;

import org.apache.dolphinscheduler.dao.entity.Cluster;
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.service.utils.ClusterConfUtils;

import java.util.Hashtable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * use multiple environment feature
 */
@Component
public class K8sManager {

    private static final Logger logger = LoggerFactory.getLogger(K8sManager.class);
    /**
     * cache k8s client
     */
    private static Map<Long, KubernetesClient> clientMap = new Hashtable<>();

    /**
     * cache old cluster
     */
    private static Map<Long, Cluster> clusterMap = new Hashtable<>();

    @Autowired
    private ClusterMapper clusterMapper;

    /**
     * get k8s client for api use
     *
     * @param clusterCode
     * @return
     */
    public synchronized KubernetesClient getK8sClient(Long clusterCode) throws RemotingException {

        if (null == clusterCode) {
            return null;
        }

        Cluster clusterDb = clusterMapper.queryByClusterCode(clusterCode);
        if (clusterDb == null) {
            return null;
        }

        String k8sConfigDb = ClusterConfUtils.getK8sConfig(clusterDb.getConfig());
        String k8sConfigUsed = null;
        Cluster clusterUsed = clusterMap.getOrDefault(clusterCode, null);

        if(clusterUsed!= null) {
            k8sConfigUsed = ClusterConfUtils.getK8sConfig(clusterUsed.getConfig());
        }

        if(k8sConfigUsed == null || !k8sConfigUsed.equals(k8sConfigDb)) {
            updateCacheMap(clusterDb);
        }

        return clientMap.get(clusterCode);
    }

    private void updateCacheMap(Cluster clusterDb)  throws RemotingException {
        KubernetesClient clientUsed = clientMap.get(clusterDb.getCode());
        if (clientUsed != null) {
            clientUsed.close();
            clusterMap.remove(clusterDb.getCode());
        }

        String k8sConfig = ClusterConfUtils.getK8sConfig(clusterDb.getConfig());
        if (k8sConfig != null) {
            DefaultKubernetesClient client = null;
            try {
                client = getClient(k8sConfig);
                clientMap.put(clusterDb.getCode(), client);
                clusterMap.put(clusterDb.getCode(),clusterDb);
            } catch (RemotingException e) {
                logger.error("cluster code ={},fail to get k8s ApiClient:  {}", clusterDb.getCode(), e.getMessage());
                throw new RemotingException("fail to get k8s ApiClient:" + e.getMessage());
            }
        }
    }

    private DefaultKubernetesClient getClient(String configYaml) throws RemotingException {
        try {
            Config config = Config.fromKubeconfig(configYaml);
            return new DefaultKubernetesClient(config);
        } catch (Exception e) {
            logger.error("Fail to get k8s ApiClient", e);
            throw new RemotingException("fail to get k8s ApiClient:" + e.getMessage());
        }
    }

}
