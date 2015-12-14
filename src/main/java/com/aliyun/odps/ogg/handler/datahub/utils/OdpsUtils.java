package com.aliyun.odps.ogg.handler.datahub.utils;

import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ogg.handler.datahub.OdpsWriter;
import com.aliyun.odps.tunnel.StreamClient;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.StreamWriter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

/**
 * Created by tianli on 15/12/13.
 */
public class OdpsUtils {
  public static String getColNameBefore(String s) {
    return s + "_before";
  }

  public static String getColNameAfter(String s) {
    return s + "_after";
  }

  public static OdpsWriter buildOdpsWriter(Table table,
                                           TableTunnel tunnel,
                                           String project,
                                           String tableName,
                                           long shardNumber,
                                           int batchSize,
                                           int retryCount,
                                           int shardTimeout,
                                           String dateFormatString,
                                           List<String> inputColNames)
      throws TunnelException, IOException, TimeoutException {
    StreamClient streamClient = tunnel.createStreamClient(project, tableName);
    streamClient.loadShard(shardNumber);
    StreamWriter[] streamWriters = buildStreamWriters(streamClient, shardNumber, shardTimeout);
    return new OdpsWriter(table, streamWriters, batchSize, retryCount, dateFormatString, inputColNames);
  }

  // Wait for loading shard, default timeout: 60s
  private static StreamWriter[] buildStreamWriters(StreamClient streamClient,
                                            long shardNumber,
                                            int shardTimeout)
      throws IOException, TunnelException, TimeoutException {
    StreamWriter[] streamWriters;
    final StreamClient.ShardState finish = StreamClient.ShardState.LOADED;
    long now = System.currentTimeMillis();
    long endTime = now + shardTimeout * 1000;
    List<Long> shardIDList = null;
    while (now < endTime) {
      HashMap<Long, StreamClient.ShardState> shardStatus = streamClient
          .getShardStatus();
      shardIDList = new ArrayList<Long>();
      Set<Long> keys = shardStatus.keySet();
      Iterator<Long> iter = keys.iterator();
      while (iter.hasNext()) {
        Long key = iter.next();
        StreamClient.ShardState value = shardStatus.get(key);
        if (value.equals(finish)) {
          shardIDList.add(key);
        }
      }
      now = System.currentTimeMillis();
      if (shardIDList.size() == shardNumber) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // DO NOTHING...
      }
    }
    if (shardIDList != null && shardIDList.size() > 0) {
      streamWriters = new StreamWriter[shardIDList.size()];
      for (int i = 0; i < shardIDList.size(); i++) {
        streamWriters[i] = streamClient.openStreamWriter(shardIDList.get(i));
      }
    } else {
      throw new TimeoutException("buildStreamWriters() error, have no loaded shards after " + shardTimeout + " seconds");
    }
    return streamWriters;
  }
}
