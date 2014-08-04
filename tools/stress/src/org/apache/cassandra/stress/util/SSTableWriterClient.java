/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stress.util;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.service.ClientState;

import java.io.File;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A Client for writing directly to sstables on the disk of the node stress is running on.
 */
public class SSTableWriterClient implements Runnable
{
    private CQLSSTableWriter writer = null;
    private ArrayBlockingQueue<List<List<Object>>> rowsToAdd = new ArrayBlockingQueue<>(2000);
    private Thread sstableThread;
    private boolean finished = false;

    /**
     * For creating a SSTable Writer Client for a User Command, in this case the create statement will be
     * variable based upon what the user has specified.
     * @param createSchemaStatement
     * @param directory
     * @param insertStatement
     */
    public SSTableWriterClient(String createSchemaStatement, String directory, String insertStatement)
    {
        File outputDir;
        if (directory == null)
        {
            try
            {
                ClientState state = ClientState.forInternalCalls();
                ParsedStatement.Prepared prepared = QueryProcessor.getStatement(createSchemaStatement, state);
                CreateTableStatement stmt = (CreateTableStatement) prepared.statement;
                stmt.validate(state);
                Directories dir = new Directories(stmt.getCFMetaData());
                outputDir = dir.getDirectoryForNewSSTables();
            }
            catch (RequestValidationException e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            outputDir = new File(directory);
        }
        System.out.println("Creating SSTABLES in " + outputDir.toPath().toString());

        //256 is the on disk data size actual heap usage will be much larger
        this.writer = CQLSSTableWriter.builder().inDirectory(outputDir).forTable(createSchemaStatement)
                .inDirectory(outputDir).using(insertStatement).withBufferSizeInMB(256).build();
        sstableThread = new Thread(this, "SSTableWriterThread");
        sstableThread.start();
    }

    public static SSTableWriterClient getLegacySSTableWriterClient(String keyspace, String table, List<String> colNames, String directory)
    {
        String createStatement = createLegacyTableCreateStatement(keyspace, table, colNames);
        String insertStatement = createLegacyInsertStatement(keyspace, table, colNames);
        return new SSTableWriterClient(createStatement, directory, insertStatement);

    }

    private static String createLegacyTableCreateStatement(String keyspace, String table, List<String> colNames)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE \"").append(keyspace).append("\".").append(table).append(" ");

        sb.append("( key blob");
        for (String colName : colNames)
            sb.append(", \"").append(colName).append("\" blob");
        sb.append(", PRIMARY KEY ((key)) )");

        return sb.toString();
    }

    private static String createLegacyInsertStatement(String keyspace, String table, List<String> colNames)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO \"").append(keyspace).append("\".").append(table).append(" ");

        sb.append("( key");
        for (String colName: colNames)
            sb.append(", \"").append(colName).append("\"");
        sb.append(") VALUES ( ?");
        for (String colName: colNames)
            sb.append(", ?");
        sb.append(")");
        return sb.toString();
    }

    public void addRowsToSSTable(List<List<Object>> rowValues) throws InterruptedException
    {
        this.rowsToAdd.put(rowValues);
    }

    public void run()
    {
        List<List<Object>> rows;
        try
        {
            while (!finished)
            {
                while ((rows = rowsToAdd.poll()) != null)
                {

                    for (List<Object> row : rows)
                        this.writer.addRow(row);

                }
                Thread.yield();
            }
            this.writer.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public synchronized void close()
    {
        if (writer!=null)
            try
            {
                while (rowsToAdd.peek() != null)
                    Thread.sleep(1000);
                finished = true;
                sstableThread.join();
                writer=null;
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
    }

}
