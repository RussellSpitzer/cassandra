package org.apache.cassandra.stress.settings;
/*
 *
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
 *
 */

import com.google.common.collect.ImmutableList;

import java.lang.Math;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RetryBackoff
{
    private enum BackoffStrategy
    {
        CONSTANT("constant"),
        LINEAR("linear"),
        EXPONENTIAL("exponential");

        private final String name;

        BackoffStrategy(String name)
        {
            this.name = name;
        }
    }

    final long SECOND = 1000;

    private final long backoffInSec;
    private final BackoffStrategy backoffStrat;

    /**
     * Returns the backoff in milliseconds for the requested trial for the set
     * backoff strategy.
     *
     * @param trial The current iteration
     * @return The time to sleep in milliseconds
     */
    public long getBackoffMS(int trial)
    {
        switch (this.backoffStrat)
        {
        case CONSTANT:
            return backoffInSec * SECOND;
        case LINEAR:
            return backoffInSec * trial * SECOND;
        case EXPONENTIAL:
            return backoffInSec * Math.round(Math.pow(2.0, trial) * SECOND);
        }
        throw new AssertionError();
    }

    private static final Map<String, BackoffStrategy> LOOKUP;

    static
    {
        final Map<String, BackoffStrategy> lookup = new HashMap<>();
        for (BackoffStrategy rb : BackoffStrategy.values())
        {
            lookup.put(rb.name, rb);
        }
        LOOKUP = lookup;
    }

    RetryBackoff(String name, int backOffInSec)
    {
        this.backoffInSec = backOffInSec;
        this.backoffStrat = LOOKUP.get(name.toLowerCase());
    }
}


