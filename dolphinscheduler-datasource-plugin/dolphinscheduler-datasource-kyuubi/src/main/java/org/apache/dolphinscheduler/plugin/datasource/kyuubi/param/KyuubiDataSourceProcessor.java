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
public class KyuubiDataSourceProcessor extends AbstractDataSourceProcessor {

    @Override
    public BaseDataSourceParamDTO castDatasourceParamDTO(String paramJson) {
        return JSONUtils.parseObject(paramJson, KyuubiDataSourceParamDTO.class);
    }

    @Override
    public BaseDataSourceParamDTO createDatasourceParamDTO(String connectionJson) {
        KyuubiConnectionParam connectionParams = (KyuubiConnectionParam) createConnectionParams(connectionJson);

        String[] hostSeperator = connectionParams.getAddress().split(Constants.DOUBLE_SLASH);
        String[] hostPortArray = hostSeperator[hostSeperator.length - 1].split(Constants.COMMA);

       KyuubiDataSourceParamDTO kyuubiDatasourceParamDTO = new KyuubiDataSourceParamDTO();
        kyuubiDatasourceParamDTO.setPort(Integer.parseInt(hostPortArray[0].split(Constants.COLON)[1]));
        kyuubiDatasourceParamDTO.setHost(hostPortArray[0].split(Constants.COLON)[0]);
        kyuubiDatasourceParamDTO.setDatabase(connectionParams.getDatabase());
        kyuubiDatasourceParamDTO.setUserName(connectionParams.getUser());
        kyuubiDatasourceParamDTO.setOther(parseOther(connectionParams.getOther()));

        return kyuubiDatasourceParamDTO;
    }

    @Override
    public BaseConnectionParam createConnectionParams(BaseDataSourceParamDTO datasourceParam) {
        KyuubiDataSourceParamDTO kyuubiParam = (KyuubiDataSourceParamDTO) datasourceParam;
        String address =
                String.format("%s%s:%s", DataSourceConstants.JDBC_KYUUBI, kyuubiParam.getHost(), kyuubiParam.getPort());
        String jdbcUrl = address + "/" + kyuubiParam.getDatabase();

        KyuubiConnectionParam kyuubiConnectionParam = new KyuubiConnectionParam();
        kyuubiConnectionParam.setUser(kyuubiParam.getUserName());
        kyuubiConnectionParam.setPassword(PasswordUtils.encodePassword(kyuubiParam.getPassword()));
        kyuubiConnectionParam.setOther(transformOther(kyuubiParam.getOther()));
        kyuubiConnectionParam.setAddress(address);
        kyuubiConnectionParam.setJdbcUrl(jdbcUrl);
        kyuubiConnectionParam.setDatabase(kyuubiParam.getDatabase());
        kyuubiConnectionParam.setDriverClassName(getDatasourceDriver());
        kyuubiConnectionParam.setValidationQuery(getValidationQuery());
        kyuubiConnectionParam.setProps(kyuubiParam.getOther());

        return kyuubiConnectionParam;
    }

    @Override
    public ConnectionParam createConnectionParams(String connectionJson) {
        return JSONUtils.parseObject(connectionJson, KyuubiConnectionParam.class);
    }

    @Override
    public String getDatasourceDriver() {
        return DataSourceConstants.ORG_APACHE_KYUUBI_JDBC_HIVE_DRIVER;
    }

    @Override
    public String getValidationQuery() {
        return DataSourceConstants.KYUUBI_VALIDATION_QUERY;
    }

    @Override
    public String getJdbcUrl(ConnectionParam connectionParam) {
        KyuubiConnectionParam kyuubiConnectionParam = (KyuubiConnectionParam) connectionParam;
        if (!StringUtils.isEmpty(kyuubiConnectionParam.getOther())) {
            return String.format("%s?%s", kyuubiConnectionParam.getJdbcUrl(), kyuubiConnectionParam.getOther());
        }
        return kyuubiConnectionParam.getJdbcUrl();
    }

    @Override
    public Connection getConnection(ConnectionParam connectionParam) throws ClassNotFoundException, SQLException {
        KyuubiConnectionParam kyuubiConnectionParam = (KyuubiConnectionParam) connectionParam;
        Class.forName(getDatasourceDriver());
        return DriverManager.getConnection(getJdbcUrl(connectionParam),
                kyuubiConnectionParam.getUser(), PasswordUtils.decodePassword(kyuubiConnectionParam.getPassword()));
    }

    @Override
    public DbType getDbType() {
        return DbType.KYUUBI;
    }

    @Override
    public DataSourceProcessor create() {
        return new KyuubiDataSourceProcessor();
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
