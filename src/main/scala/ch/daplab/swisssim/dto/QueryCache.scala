// Copyright (C) 2015 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch.daplab.swisssim.dto

/**
  * @param query
  * @param similarity
  * @param fingerprint
  * @param smile
  * @param details
  *
  *
  * CREATE TABLE swisssim.querycache (
    query blob,
    similarity double,
    fingerprint blob,
    smile text,
    details text,
    PRIMARY KEY (query, similarity, fingerprint, smile)
) WITH COMPACT STORAGE
    AND CLUSTERING ORDER BY (similarity DESC)
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}

    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = 'M'
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';
  */
case class QueryCache(query: Array[Byte],
                      similarity: Double, fingerprint: Array[Byte],
                      smile: String, details: String)
