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
import org.wso2.carbon.mediation.connector.pool.BoundedBlockingPool;
import org.wso2.carbon.mediation.connector.pool.ExpandingBlockingPool;
import org.wso2.carbon.mediation.connector.pool.Pool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static BlockingPool<Connection> connectionPool;
    private static ExecutorService jdbcQueryExcecutorService;
    private static ArrayList<DBQueryRunner> DBRunners;

    public static void main(String[] args) {
        initialize();
    }

    private static void initialize() {

        DBRunners = new ArrayList<>(10);
        jdbcQueryExcecutorService = Executors.newFixedThreadPool(25);

        JDBCConnectionFactory connectionFactory = new JDBCConnectionFactory(
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test",
                "root",
                "root");

        Pool.ConnectionCreatorResiliencyParams resiliencyParams
                = new Pool.ConnectionCreatorResiliencyParams(15, 0.2F, -1, 120);

        //fixed size pool
        connectionPool = new BoundedBlockingPool<>("JDBC_Connection_Pool", 15,
                resiliencyParams, new JDBCConnectionValidator(), connectionFactory);

        //expanding pool
        //connectionPool = new ExpandingBlockingPool<>("JDBC_Connection_Pool", 5,
        //        15, 5, resiliencyParams, new JDBCConnectionValidator(), connectionFactory);


        System.out.println("==================5 threads====================");
        createRunners(5, 500);
        sleep(1 * 30 * 1000);

        System.out.println("==================20 threads====================");
        createRunners(15, 500);


    }

    private static void createRunners(int numberOfThreads, int delayBetweenMessages) {
        for(int count= 0 ; count < numberOfThreads ; count ++) {
            DBQueryRunner runner = new DBQueryRunner(connectionPool, delayBetweenMessages, 100000000);
            DBRunners.add(runner);
            jdbcQueryExcecutorService.submit(runner);
            sleep(100);
        }
    }

    private static void setSpeed(int numberOfThreads,  int delayBetweenMessages) {
        for(int count= 0 ; count < numberOfThreads ; count ++) {
            DBQueryRunner runner = DBRunners.get(count);
            runner.setDelayBetweenQueries(delayBetweenMessages);
        }
    }

    private static void stopRunners(int numberOfThreads) {
        Iterator<DBQueryRunner> runnerIterator = DBRunners.iterator();
        for(int count= 0 ; count < numberOfThreads ; count ++) {
            DBQueryRunner runner = runnerIterator.next();
            runner.stop();
            runnerIterator.remove();
        }
    }

    private static void sleep(int millies) {
        try {
            Thread.sleep(millies);
        } catch (InterruptedException e) {
            //ignore
        }
    }

}
