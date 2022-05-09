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
import org.apache.dolphinscheduler.dao.mapper.ClusterMapper;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;

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
    private ClusterMapper clusterMapper;

    public synchronized KubernetesClient getK8sClient(Long clusterCode) {
        if (null == clusterCode) {
            return null;
        }
        if (clientMap.containsKey(clusterCode)) {
            return clientMap.get(clusterCode);
        } else {
            createK8sClient(clusterCode);
        }
        return clientMap.get(clusterCode);
    }


    private void createK8sClient(Long clusterCode) {
        Cluster cluster = clusterMapper.queryByClusterCode(clusterCode);
        if (cluster == null) {
            return;
        }

        String k8sConfig = ClusterConfUtils.getK8sConfig(cluster.getConfig());
        if (k8sConfig != null) {
            DefaultKubernetesClient client = null;
            try {
                client = getClient(k8sConfig);
                clientMap.put(cluster.getName(), client);
            } catch (RemotingException e) {
                logger.error("cluster code ={},fail to get k8s ApiClient:  {}", clusterCode, e.getMessage());
            }
        }
    }


//    @EventListener
//    public void buildApiClientAll(ApplicationReadyEvent readyEvent) throws RemotingException {
//        List<Cluster> clusterList = clusterMapper.selectList(null);
//        if (clusterList != null && clusterList.size() > 0) {
//            for(Cluster item :clusterList ) {
//                String k8sConfig = ClusterConfUtils.getK8sConfig(item.getConfig());
//                if(k8sConfig!=null)
//                {
//                    DefaultKubernetesClient client = getClient(k8sConfig);
//                    clientMap.put(item.getName(), client);
//                }
//            }
//        }
//    }

    private DefaultKubernetesClient getClient(String configYaml) throws RemotingException {
        try {
            Config config = Config.fromKubeconfig(configYaml);
            return new DefaultKubernetesClient(config);
        } catch (Exception e) {
            logger.error("fail to get k8s ApiClient", e);
            throw new RemotingException("fail to get k8s ApiClient:" + e.getMessage());
        }
    }

}
