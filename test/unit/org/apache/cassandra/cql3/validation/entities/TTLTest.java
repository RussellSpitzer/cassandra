package org.apache.cassandra.cql3.validation.entities;

import org.apache.cassandra.cql3.Attributes;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class TTLTest extends CQLTester
{
    @Test
    public void testNullTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, null);

        assertRows(execute("SELECT k, v, TTL(v) FROM %s"), row(1, 1, null));
    }

    @Test(expected = InvalidRequestException.class)
    public void testErrorOnNegativeTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, -5);
    }

    @Test(expected = InvalidRequestException.class)
    public void testErrorOnBigTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES (?, ?) USING TTL ?", 1, 1, Attributes.MAX_TTL + 1);
    }

}
