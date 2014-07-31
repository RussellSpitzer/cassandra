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

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A Client for writing directly to sstables on the disk of the node stress is running on.
 */
public class SSTableWriterClient implements Runnable
{
    private CQLSSTableWriter writer = null;
    private ArrayBlockingQueue<List<List<Object>>> rowsToAdd = new ArrayBlockingQueue<>(10000);
    private Thread sstableThread;
    private boolean finished = false;

    public SSTableWriterClient(String createSchemaStatement, String directory, String insertStatement)
    {
        //256 is the on disk data size actual heap usage will be much larger
        //TODO warn if XMS is not sufficently high and using sstable writer
        this.writer = CQLSSTableWriter.builder().inDirectory(directory).forTable(createSchemaStatement)
                .inDirectory(directory).using(insertStatement).withBufferSizeInMB(256).build();
        sstableThread = new Thread(this, "SSTableWriterThread");
        sstableThread.start();
    }

    public void addRowsToSSTable(List<List<Object>> rowValues) throws InterruptedException
    {
        this.rowsToAdd.put(rowValues);
    }

    public void run()
    {
        List<List<Object>> rows;
        while (!finished)
            while ((rows = rowsToAdd.poll()) != null)
            {
                try
                {
                    for (List<Object> row : rows)
                        this.writer.addRow(row);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        try
        {
            this.writer.close();
        }
        catch (IOException e)
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
