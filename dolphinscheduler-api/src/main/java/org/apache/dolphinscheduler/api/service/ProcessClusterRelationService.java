package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.dao.entity.ClusterProcessDefinitionRelation;

import java.util.List;
import java.util.Map;

public interface ProcessClusterRelationService {


//   Map<String, Object> addCluster(Long projectCode,Long clusterCode,String config);

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
//    Map<String, Object> updateCluster(Long processDefinitionCode, Integer processDefinitionVersion, Long clusterCode,String clusterParams);

    /**
     * update cluster and process relation after process updated, so the auth check process has done
     * here check cluster code
     *
     * @param processDefinitionCode
     * @param processDefinitionVersion
     * @param clusterLists
     * @return
     */
    Map<String, Object> updateProcessClusterRelation(Long processDefinitionCode, Integer processDefinitionVersion, List<ClusterProcessDefinitionRelation> clusterLists);


    Map<String, Object> updateProcessClusterStatus(Long processDefinitionCode, Integer processDefinitionVersion, Long clusterCode, ReleaseState releaseState);
}
