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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.mediation.connector.pool.Pool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;
import java.util.UUID;

/**
 * How to validate the connection is OK and how to invalidate (clear resources) of a connection
 */
public class JDBCConnectionValidator implements Pool.ConnectionValidator<Connection> {

    private static final Log log = LogFactory.getLog(JDBCConnectionValidator.class);

    public boolean isValid(Connection connection) {
        System.out.println("Checking if connection is valid...");
        if (connection == null) {
            System.out.println("Validation failed. Connection = null");
            return false;
        }
        try {
            boolean valid =connection.isValid(2);
            //boolean valid =  !connection.isClosed();
            if(valid) {
                System.out.println("Connection is valid");
            } else {
                System.out.println("Connection is invalid");
            }
            return valid;
        } catch (SQLException se) {
            System.out.println("Validation failed. SQL ex = " + se.getMessage());
            return false;
        }
    }

    public void invalidate(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("ERROR when closing connection", e);
        }
    }

    @Override
    public String getConnectionId(Connection connection) {
        return UUID.randomUUID().toString();
    }
}
