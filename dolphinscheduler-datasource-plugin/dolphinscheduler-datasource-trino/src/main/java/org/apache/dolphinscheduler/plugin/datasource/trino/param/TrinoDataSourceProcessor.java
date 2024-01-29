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

package org.apache.dolphinscheduler.plugin.datasource.trino.param;

import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.constants.DataSourceConstants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.AbstractDataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.BaseDataSourceParamDTO;
import org.apache.dolphinscheduler.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.dolphinscheduler.plugin.datasource.api.utils.PasswordUtils;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.datasource.ConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import com.google.auto.service.AutoService;

@AutoService(DataSourceProcessor.class)
public class TrinoDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, TrinoDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        TrinoConnectionParam connectionParams = (TrinoConnectionParam) createConnectionParams(connectionJson);

        String[] hostSeperator = connectionParams.getAddress().split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);

       TrinoDataSourceParamDTO trinoDatasourceParamDTO = new TrinoDataSourceParamDTO();
        trinoDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        trinoDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);
        trinoDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        trinoDatasourceParamDTO.setUserName(connectionParams.getUser());
        trinoDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));

        return trinoDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        TrinoDataSourceParamDTO trinoParam = (TrinoDataSourceParamDTO) datasourceParam;
        String address =
                String.format("%s%s:%s", DataSourceConstants.JDBC_TRINO, trinoParam.getHost(), trinoParam.getPort());
        String jdbcUrl = address + "/" + trinoParam.getDatabase();

        TrinoConnectionParam trinoConnectionParam = new TrinoConnectionParam();
        trinoConnectionParam.setUser(trinoParam.getUserName());
        trinoConnectionParam.setPassword(PasswordUtils.encodePassword(trinoParam.getPassword()));
        trinoConnectionParam.setOther(transformOther(trinoParam.getOther()));
        trinoConnectionParam.setAddress(address);
        trinoConnectionParam.setJdbcUrl(jdbcUrl);
        trinoConnectionParam.setDatabase(trinoParam.getDatabase());
        trinoConnectionParam.setDriverClassName(getDatasourceDriver());
        trinoConnectionParam.setValidationQuery(getValidationQuery());
        trinoConnectionParam.setProps(trinoParam.getOther());

        return trinoConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, TrinoConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.IO_TRINO_JDBC_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.TRINO_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        TrinoConnectionParam trinoConnectionParam = (TrinoConnectionParam) connectionParam;
        if (!StringUtils.isEmpty(trinoConnectionParam.getOther())) {
            return String.format("%s?%s", trinoConnectionParam.getJdbcUrl(), trinoConnectionParam.getOther());
        }
        return trinoConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        TrinoConnectionParam trinoConnectionParam = (TrinoConnectionParam) connectionParam;
        TimeZone.setDefault(TimeZone.getTimeZone("+08:00"));
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(connectionParam),
                trinoConnectionParam.getUser(), PasswordUtils.decodePassword(trinoConnectionParam.getPassword()));
    }

    @Override
    public DbType getDbType() {
        return DbType.TRINO;
    }

    @Override
    public DataSourceProcessor create() {
        return new TrinoDataSourceProcessor();
    }

    private String transformOther(Map<String, String> otherMap) {
        if (MapUtils.isNotEmpty(otherMap)) {
            List<String> list = new ArrayList<>();
            otherMap.forEach((key, value) -> list.add(String.format("%s=%s", key, value)));
            return String.join("&", list);
        }
        return null;
    }

    private Map<String, String> parseOther(String other) {
        if (StringUtils.isEmpty(other)) {
            return null;
        }
        Map<String, String> otherMap = new LinkedHashMap<>();
        String[] configs = other.split("&");
        for (String config : configs) {
            otherMap.put(config.split("=")[0], config.split("=")[1]);
        }
        return otherMap;
    }
}
