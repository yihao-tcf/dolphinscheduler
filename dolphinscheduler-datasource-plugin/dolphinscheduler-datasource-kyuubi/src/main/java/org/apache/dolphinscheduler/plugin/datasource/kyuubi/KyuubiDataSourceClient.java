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

package org.apache.dolphinscheduler.plugin.datasource.kyuubi;

import org.apache.dolphinscheduler.plugin.datasource.api.client.CommonDataSourceClient;
import org.apache.dolphinscheduler.plugin.datasource.api.provider.JDBCDataSourceProvider;
import org.apache.dolphinscheduler.spi.datasource.BaseConnectionParam;
import org.apache.dolphinscheduler.spi.enums.DbType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class KyuubiDataSourceClient extends CommonDataSourceClient {
    private static final Logger logger = LoggerFactory.getLogger(KyuubiDataSourceClient.class);

    public KyuubiDataSourceClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        super(baseConnectionParam, dbType);
    }
    protected Connection connection;

    @Override
    protected void initClient(BaseConnectionParam baseConnectionParam, DbType dbType) {
        try {
            this.connection = JDBCDataSourceProvider.createAdHocJdbcDataSource(baseConnectionParam, dbType);
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Init {} success.", getClass().getName());
    }

    @Override
    public void checkClient() {

    }

    @Override
    public Connection getConnection() {
      return this.connection;
    }
    @Override
    public void close() {
        logger.info("do close dataSource {}.", baseConnectionParam.getDatabase());
        try {
            if (connection != null) {
                try {
                  connection.close();
                } catch (SQLException e) {
                  e.printStackTrace();
                }
            }
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}
