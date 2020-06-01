/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.mapreduce;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class TestHiveRunner {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Before
  public void setupDatabase() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
            .append("CREATE TABLE source_db.test_table (")
            .append("col_a STRING, col_b INT, col_c BOOLEAN")
            .append(")")
            .toString());
  }

  @Test
  public void insertRowsFromCode() {
    shell.insertInto("source_db", "test_table")
            .withAllColumns()
            .addRow("Value1", 1, true)
            .addRow("Value2", 99, false)
            .commit();

    printResult(shell.executeStatement("select * from source_db.test_table"));
  }

  public void printResult(List<Object[]> result) {
    System.out.println(String.format("Results: "));
    for (Object[] row : result) {
      System.out.println(Arrays.asList(row));
    }
  }
}
