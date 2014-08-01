package org.apache.cassandra.stress.operations.predefined;

import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.generate.PartitionGenerator;
import org.apache.cassandra.stress.operations.FixedOpDistribution;
import org.apache.cassandra.stress.settings.Command;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.SSTableWriterClient;
import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class for writing directly to sstables for legacy Stress Write Commands
 */
public class SSTableInserter extends PredefinedOperation
{
    private final long maxPartitions;
    private final int MAXBATCH = 30;
    private int partitionsInserted = 0;
    private int partitionsInBatch = 0;
    private List<List<Object>> currentBatch = new ArrayList<>();

    public SSTableInserter(Timer timer, PartitionGenerator generator, StressSettings settings)
    {
        super(Command.WRITE, timer, generator, settings);
        maxPartitions = (settings.command.count / settings.rate.threadCount);
    }

    @Override public void run(ThriftClient client) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override public void run(SSTableWriterClient client)
    {
        currentBatch.add(getRow());
        partitionsInBatch ++;
        partitionsInserted ++;

        if (partitionsInBatch == MAXBATCH || partitionsInserted >= maxPartitions)
        {
            try
            {
                timeWithRetry(new SSTableRunOp(client, currentBatch));
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            currentBatch = new ArrayList<>();
            partitionsInBatch = 0;
        }

    }

    public List<Object> getRow()
    {
        final ArrayList<Object> queryParams = new ArrayList<>();
        List<ByteBuffer> values = getColumnValues();
        queryParams.add(getKey());
        queryParams.addAll(values);
        return  queryParams;
    }

    protected class SSTableRunOp implements RunOp

    {
        final SSTableWriterClient client;
        final List<List<Object>> rows;


        public SSTableRunOp( SSTableWriterClient client, List<List<Object>> rows)
        {
            this.client = client;
            this.rows = rows;

        }

        @Override public boolean run() throws Exception
        {
            this.client.addRowsToSSTable(rows);
            return true;
        }

        @Override public int partitionCount()
        {
            return rows.size();
        }

        @Override public int rowCount()
        {
            return partitionCount();
        }
    }


}
