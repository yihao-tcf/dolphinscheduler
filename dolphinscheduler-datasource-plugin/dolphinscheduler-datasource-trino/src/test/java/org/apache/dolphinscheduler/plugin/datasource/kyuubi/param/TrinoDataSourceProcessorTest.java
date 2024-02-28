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

package org.apache.dolphinscheduler.plugin.datasource.kyuubi.param;

import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.plugin.datasource.api.plugin.DataSourceClientProvider;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.CommonUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.DataSourceUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.enums.DbType;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Class.class, DriverManager.class, DataSourceUtils.class, CommonUtils.class,
        DataSourceClientProvider.class, PasswordUtils.class})
public class TrinoDataSourceProcessorTest {

    private TrinoDataSourceProcessor trinoDatasourceProcessor = new TrinoDataSourceProcessor();

    @Test
    public void testCreateConnectionParams() {
        Map<String, String> props = new HashMap<>();
        props.put("serverTimezone", "utc");
        TrinoDataSourceParamDTO trinoDatasourceParamDTO = new TrinoDataSourceParamDTO();
        trinoDatasourceParamDTO.setHost("localhost");
        trinoDatasourceParamDTO.setPort(30001);
        trinoDatasourceParamDTO.setDatabase("system");
        trinoDatasourceParamDTO.setUserName("root");
        trinoDatasourceParamDTO.setPassword(null);
        trinoDatasourceParamDTO.setOther(props);
        PowerMockito.mockStatic(PasswordUtils.class);
        PowerMockito.when(PasswordUtils.encodePassword(Mockito.anyString())).thenReturn("test");
        TrinoConnectionParam connectionParams = (TrinoConnectionParam) trinoDatasourceProcessor
                .createConnectionParams(trinoDatasourceParamDTO);
        Assert.assertEquals("jdbc:trino://localhost:30001", connectionParams.getAddress());
        Assert.assertEquals("jdbc:trino://localhost:30001/system", connectionParams.getJdbcUrl());
    }

    @Test
    public void testCreateConnectionParams2() {
        String connectionJson =
                "{\"user\":\"root\",\"password\":\"\",\"address\":\"localhost\""
                        + ",\"database\":\"system\",\"jdbcUrl\":\"jdbc:trino://localhost:30001/system\"}";
        TrinoConnectionParam connectionParams = (TrinoConnectionParam) trinoDatasourceProcessor
                .createConnectionParams(connectionJson);
        Assert.assertNotNull(connectionParams);
        Assert.assertEquals("root", connectionParams.getUser());
    }

    @Test
    public void testGetDatasourceDriver() {
        Assertions.assertEquals(DataSourceConstants.IO_TRINO_JDBC_DRIVER,
                trinoDatasourceProcessor.getDatasourceDriver());
    }

    @Test
    public void testGetJdbcUrl() {
        TrinoConnectionParam trinoConnectionParam = new TrinoConnectionParam();
        trinoConnectionParam.setJdbcUrl("jdbc:trino://localhost:30001/default");
        trinoConnectionParam.setOther("other");
        Assert.assertEquals("jdbc:trino://localhost:30001/default?other",
                trinoDatasourceProcessor.getJdbcUrl(trinoConnectionParam));

    }

    @Test
    public void testGetDbType() {
        Assert.assertEquals(DbType.TRINO, trinoDatasourceProcessor.getDbType());
    }

    @Test
    public void testGetValidationQuery() {
        Assertions.assertEquals(DataSourceConstants.TRINO_VALIDATION_QUERY,
                trinoDatasourceProcessor.getValidationQuery());
    }
}
