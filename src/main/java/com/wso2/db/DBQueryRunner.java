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

import org.wso2.carbon.mediation.connector.pool.BlockingPool;
import org.wso2.carbon.mediation.connector.pool.Pool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class DBQueryRunner implements Runnable{

    private BlockingPool<Connection> connectionPool;
    private int delayBetweenQueries;
    private int maxIterations;
    private boolean stop = false;

    public DBQueryRunner(BlockingPool<Connection> connectionPool, int delayBetweenQueries, int maxIterations) {
        this.connectionPool = connectionPool;
        this.delayBetweenQueries = delayBetweenQueries;
        this.maxIterations = maxIterations;
    }

    public void stop() {
        this.stop = true;
    }

    public void setDelayBetweenQueries(int delayBetweenQueries) {
        this.delayBetweenQueries = delayBetweenQueries;
    }

    @Override
    public void run() {
        int counter = 0;
        while (!stop && counter < maxIterations) {
            System.out.println("["+ Thread.currentThread().getId()+ "] " + "Mediating message...");
            try {
                //Connection conn = connectionPool.get(1000, TimeUnit.MILLISECONDS);
                Connection conn = connectionPool.get();
                if(conn == null) {
                    System.out.println("Connection received by pool is null");
                    continue;
                }
                Statement statement = conn.createStatement();
                ResultSet rs = null;
                try {
                    //String query = "INSERT INTO `CDC_CUSTOM` (`ID`, `NAME`, `ADDRESS`, `AMOUNT`) VALUES (001, \"john\", \"22/3, Tottenham Court, London\" , 1000);";
                    //statement.executeUpdate(query);
                    String query = "SELECT * FROM CDC_CUSTOM";
                    rs = statement.executeQuery(query);
                    int size = 0;
                    if (rs != null)
                    {
                        rs.last();    // moves cursor to the last row
                        size = rs.getRow(); // get row id
                    }
                    System.out.println("["+ Thread.currentThread().getId()+ "] " +"Executed query. ResultSet size = " + size);

                } finally {
                    if(Objects.nonNull(rs)) {
                        rs.close();
                    }
                    statement.close();
                    connectionPool.release(conn);   //releasing connection should be in a finally block
                    System.out.println("["+ Thread.currentThread().getId()+ "] " + "Connection is released");
                }
            } catch (SQLException e) {
                System.out.println("Error during message mediation");
                e.printStackTrace();
            }

            sleep();

            counter = counter + 1;
        }
        System.out.println("DB runner closed Thread id = " + Thread.currentThread().getId());
    }

    private void sleep() {
        try {
            Thread.sleep(delayBetweenQueries);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
