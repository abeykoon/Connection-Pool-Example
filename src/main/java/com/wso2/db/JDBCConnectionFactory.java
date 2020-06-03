/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wso2.db;

import org.wso2.carbon.mediation.connector.pool.ConnectionFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * How to create a new connection
 */
public class JDBCConnectionFactory implements ConnectionFactory<Connection> {

    //user inputs
    private String connectionURL;
    private String userName;
    private String password;

    public JDBCConnectionFactory(String driver, String connectionURL, String userName, String password) {
        super();
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException ce) {
            throw new IllegalArgumentException(
                    "Unable to find driver in classpath", ce);
        }

        this.connectionURL = connectionURL;
        this.userName = userName;
        this.password = password;
    }

    public Connection createNew() {
        try {
            return DriverManager.getConnection(connectionURL, userName, password);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Unable to create new connection", se);
        }
    }
}
