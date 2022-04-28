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

package org.apache.dolphinscheduler.common.utils;

import org.springframework.boot.configurationprocessor.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * cluster conf will include all ,current only k8s
 */
public class ClusterConfUtils {


    /**
     * get
     * @param config
     * @param k8s
     * @return
     */
    public String getK8s(String config, String k8s) {

        ObjectNode conf = JSONUtils.parseObject(config);
        if (conf == null) {
            return null;
        }
        JsonNode k8sMap = conf.get("k8s");
        if (k8sMap == null || k8sMap.get(k8s) == null) {
            return null;
        }

        return k8sMap.get(k8s).asText();
    }


    private JSONObject getJSONObjByKey(String key) {
        return null;
    }


}
