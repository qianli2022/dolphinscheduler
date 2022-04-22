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

package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.dao.BaseDaoTest;
import org.apache.dolphinscheduler.dao.entity.ClusterProcessDefinitionRelation;

import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ClusterProcessDefinitionRelationMapperTest extends BaseDaoTest {

    @Autowired
    private ClusterProcessDefinitionRelationMapper clusterProcessDefinitionRelationMapper;

    @Before
    public void setUp() {
        clearTestData();
    }

    @After
    public void after() {
        clearTestData();
    }

    public void clearTestData() {
        clusterProcessDefinitionRelationMapper.selectList(null).stream().forEach(cluster -> {
            clusterProcessDefinitionRelationMapper.deleteById(cluster.getId());
        });
    }

    /**
     * insert
     *
     * @return ProcessDefinition
     */
    private ClusterProcessDefinitionRelation insertOne() {
        //insertOne
        ClusterProcessDefinitionRelation relation = new ClusterProcessDefinitionRelation();
        relation.setClusterCode(1L);
        relation.setProcessDefinition("default");
        relation.setOperator(1);
        relation.setUpdateTime(new Date());
        relation.setCreateTime(new Date());
        clusterProcessDefinitionRelationMapper.insert(relation);
        return relation;
    }

    /**
     * test query
     */
    @Test
    public void testQuery() {
        insertOne();
        //query
        List<ClusterProcessDefinitionRelation> relations = clusterProcessDefinitionRelationMapper.selectList(null);
        Assert.assertEquals(relations.size(), 1);
    }

    @Test
    public void testQueryByClusterCode() {
        ClusterProcessDefinitionRelation relation = insertOne();
        List<ClusterProcessDefinitionRelation> clusterProcessDefinitionRelations = clusterProcessDefinitionRelationMapper.queryByClusterCode(1L);
        Assert.assertNotEquals(clusterProcessDefinitionRelations.size(), 0);
    }

    @Test
    public void testQueryByProcessDefinitionName() {
        ClusterProcessDefinitionRelation relation = insertOne();
        List<ClusterProcessDefinitionRelation> clusterProcessDefinitionRelations = clusterProcessDefinitionRelationMapper.queryByProcessDefinitionName("default");
        Assert.assertNotEquals(clusterProcessDefinitionRelations.size(), 0);
    }

    @Test
    public void testDeleteByCode() {
        ClusterProcessDefinitionRelation relation = insertOne();
        int i = clusterProcessDefinitionRelationMapper.deleteByCode(1L, "default");
        Assert.assertNotEquals(i, 0);
    }
}
