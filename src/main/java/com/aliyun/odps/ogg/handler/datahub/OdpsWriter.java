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
package com.aliyun.odps.ogg.handler.datahub;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamRecordPack;
import com.aliyun.odps.tunnel.io.StreamWriter;

public class OdpsWriter {
    private static final Logger logger = LoggerFactory.getLogger(OdpsWriter.class);

    private Map<String, StreamRecordPack> partitionRecordsMap;
    private StreamWriter[] streamWriters;
    private TableSchema tableSchema;
    private int retryCount;
    private int batchSize;
    private Random random;
    private RecordBuilder recordBuilder;
    private Set<String> partitionSet;
    private Table table;

    public OdpsWriter(Table table,
                      StreamWriter[] streamWriters,
                      int batchSize,
                      int retryCount,
                      String dateFormatString,
                      List<String> inputColNames) throws TunnelException {
        this.table = table;
        this.streamWriters = streamWriters;
        this.tableSchema = table.getSchema();
        this.retryCount = retryCount;
        this.batchSize = batchSize;
        random = new Random();
        partitionRecordsMap = new HashMap<String, StreamRecordPack>();
        recordBuilder = new RecordBuilder(table, dateFormatString, inputColNames);
        partitionSet = new HashSet<String>();
        for (Partition partition: table.getPartitions()) {
            partitionSet.add(partition.getPartitionSpec().toString());
        }
    }

    public Set<String> getPartitionSet() {
        return partitionSet;
    }

    public void createPartition(PartitionSpec partitionSpec, boolean ifNotExist) throws OdpsException {
        table.createPartition(partitionSpec, ifNotExist);
    }

    public void addToList(String partition, Map<String, String> rowMap) throws IOException, TunnelException, ParseException {
        Record record = recordBuilder.buildRecord(rowMap);
        StreamRecordPack recordPack = partitionRecordsMap.get(partition);
        if (recordPack == null) {
            recordPack = new StreamRecordPack(tableSchema);
            partitionRecordsMap.put(partition, recordPack);
        }
        recordPack.append(record);
        if (recordPack.getRecordCount() >= batchSize) {
            flush(partition);
        }
    }

    public void flush(String partition) throws IOException, TunnelException {
        StreamRecordPack recordPack = partitionRecordsMap.remove(partition);
        writeToHub(partition, recordPack);
    }

    public void flushAll() throws IOException, TunnelException {
        // Iterate the map
        for(Iterator<Map.Entry<String, StreamRecordPack>> it = partitionRecordsMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, StreamRecordPack> entry = it.next();
            writeToHub(entry.getKey(), entry.getValue());
            it.remove();
        }
    }

    public static class WriteHubRetryFailedException extends RuntimeException {
        public WriteHubRetryFailedException(String msg, Exception e) {
            super(msg, e);
        }
    }

    public void writeToHub(String partition, StreamRecordPack pack) {
        if (pack == null) {
            return;
        }
        int retrys = 0;
        while (true) {
            try {
                if (StringUtils.isEmpty(partition)) {
                    streamWriters[random.nextInt(streamWriters.length)]
                        .write(pack);
                } else {
                    streamWriters[random.nextInt(streamWriters.length)]
                        .write(new PartitionSpec(partition), pack);
                }
                break;
            } catch (Exception e) {
                if (retrys >= retryCount) {
                    throw new WriteHubRetryFailedException("Failed writing data. Retry count reaches limit.", e);
                }
                int sleepTime = 3000;
                logger.warn("Write pack failed. Will retry after " + sleepTime + " ms.", e);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    // Do nothing
                }
                retrys++;
            }
        }
    }


}
