## Examples

### show create table

```
$ curl -s $tidb_addr/schema/index_scan/item | tidbg eval '{{ . | showCreateTable}}'
CREATE TABLE `item` (
  `id` char(36) NOT NULL,
  `type` int(11) DEFAULT '0',
  `status` int(11) DEFAULT '0',
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `pad` text DEFAULT NULL,
  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
  KEY `idx_type_status` (`type`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

### decode plan

```
$ plan='sgfwPjAJMTZfOAkwCTk3LjcyCW9mZnNldDowLCBjb3VudDoxMDAJNjMJdGltZTozMDEuN21zLCBsb29wczoyCU4vQQEEHAoxCTMyXzEyFUU4aW5kZXg6TGltaXRfMTEJXkAAAWRkcF90YXNrOiB7bnVtOiA1NiwgbWF4OiA3Mi4FapBtaW46IDM4Mi4xwrVzLCBhdmc6IDUuMzVtcywgcDk1OiAzNi4xAQ08bWF4X3Byb2Nfa2V5czogNQUfLhIAFDQsIHRvdAUSFDogNDIuOAE2ARIcd2FpdDogMTgFVAxycGNfFYoBDQB0AfkUIDI5OS4yATGAY29wcl9jYWNoZV9oaXRfcmF0aW86IDAuMDIsIGJ1aWxkBc8IX2R1BRocbjogMTAwLjIFvAGigGRpc3RzcWxfY29uY3VycmVuY3k6IDF9CTg0NSBCeXRlcyVPADIhkxQxMQkxXzB+lgEEa3YFcQQ6ewH8AWMIOjMwAaghOAAwMTEIMi41ARQMcDgwOgEUITgAMgUmFGl0ZXJzOiF1DHRhc2sBClR9LCBzY2FuX2RldGFpbDoge3RvdGFsJTYIZXNzLV0ENjIlTDoYACRfc2l6ZTogNjMyKWsEYWwNL0w1NjQ5NywgZ2V0X3NuYXBzaG90XylXDDMwLjkBmmRyb2Nrc2RiOiB7ZGVsZXRlX3NraXBwZWRfY0WEKCAyNTQxNywga2V5PhoADDExNjYBXQxibG9jQUY5mw07JDM2MX19fQlOL0EBBCAKMwk0N18xMAk5Wyx0YWJsZTppdGVtLCBJuDxpZHhfdHlwZV9zdGF0dXMoAQwELCAJDUQpLCByYW5nZTpbMCAwLDAgMF0Bl0hlcCBvcmRlcjpmYWxzZQk2Mwl0/poBRpoBIAlOL0EJTi9BCg=='
$ tidbg eval '{{ "'$plan'" | decodePlan }}'
	id                     	task     	estRows	operator info                                                                     	actRows	execution info                                                                                                                                                                                                                                                                                           	memory   	disk
	Limit_8                	root     	97.72  	offset:0, count:100                                                               	63     	time:301.7ms, loops:2                                                                                                                                                                                                                                                                                    	N/A      	N/A
	└─IndexReader_12       	root     	97.72  	index:Limit_11                                                                    	63     	time:301.7ms, loops:2, cop_task: {num: 56, max: 72.7ms, min: 382.1µs, avg: 5.35ms, p95: 36.1ms, max_proc_keys: 5, p95_proc_keys: 4, tot_proc: 42.8ms, tot_wait: 185ms, rpc_num: 56, rpc_time: 299.2ms, copr_cache_hit_ratio: 0.02, build_task_duration: 100.2µs, max_distsql_concurrency: 1}           	845 Bytes	N/A
	  └─Limit_11           	cop[tikv]	97.72  	offset:0, count:100                                                               	63     	tikv_task:{proc max:30ms, min:0s, avg: 2.5ms, p80:0s, p95:20ms, iters:56, tasks:56}, scan_detail: {total_process_keys: 62, total_process_keys_size: 6324, total_keys: 56497, get_snapshot_time: 30.9ms, rocksdb: {delete_skipped_count: 25417, key_skipped_count: 116697, block: {cache_hit_count: 361}}}	N/A      	N/A
	    └─IndexRangeScan_10	cop[tikv]	97.72  	table:item, index:idx_type_status(type, status), range:[0 0,0 0], keep order:false	63     	tikv_task:{proc max:30ms, min:0s, avg: 2.5ms, p80:0s, p95:20ms, iters:56, tasks:56}                                                                                                                                                                                                                      	N/A      	N/A
```

### handle tso

```
$ tso=448639625858121729
$ tidbg eval "time: {{ tsoTime $tso }}, epoch: {{ tsoPhysical $tso }}, logical: {{ tsoLogical $tso }}"
time: 2024-03-26T11:39:14.012+08:00, epoch: 1711424354012, logical: 1

$ tidbg eval "{{ tsoCompose $(date +%s)000 0 }}"
449187359752192000

$ pd-ctl tso 449187359752192000
args:["pd-ctl","tso","449187359752192000"]
input:null
system:  2024-04-19 16:03:13 +0800 CST
logic:   0
```

### build keys

```
$ tidbg eval 't{{i64 110}}_i{{i64 2}}{{signed 0}}{{signed 0}}'
74800000000000006e5f698000000000000002038000000000000000038000000000000000

$ tidbg eval -f key 't{{i64 110}}_i{{i64 2}}{{signed 0}}{{signed 0}}'
7480000000000000ff6e5f698000000000ff0000020380000000ff0000000003800000ff0000000000000000fc

$ tidbg eval -f quote 't{{i64 110}}_i{{i64 2}}{{signed 0}}{{signed 0}}'
"t\200\000\000\000\000\000\000\377n_i\200\000\000\000\000\377\000\000\002\003\200\000\000\000\377\000\000\000\000\003\200\000\000\377\000\000\000\000\000\000\000\000\374"

$ echo "t\200\000\000\000\000\000\000\377n_i\200\000\000\000\000\377\000\000\002\003\200\000\000\000\377\000\000\000\000\003\200\000\000\377\000\000\000\000\000\000\000\000\374" | tidbg eval '{{ . | unquote | unkey }}'
74800000000000006e5f698000000000000002038000000000000000038000000000000000

$ mok "t\200\000\000\000\000\000\000\377n_i\200\000\000\000\000\377\000\000\002\003\200\000\000\000\377\000\000\000\000\003\200\000\000\377\000\000\000\000\000\000\000\000\374"
"t\\200\\000\\000\\000\\000\\000\\000\\377n_i\\200\\000\\000\\000\\000\\377\\000\\000\\002\\003\\200\\000\\000\\000\\377\\000\\000\\000\\000\\003\\200\\000\\000\\377\\000\\000\\000\\000\\000\\000\\000\\000\\374"
├─## table prefix
│ └─table: -2579946652266647504
└─## decode go literal key
  └─"t\200\000\000\000\000\000\000\377n_i\200\000\000\000\000\377\000\000\002\003\200\000\000\000\377\000\000\000\000\003\200\000\000\377\000\000\000\000\000\000\000\000\374"
    ├─## decode mvcc key
    │ └─"t\200\000\000\000\000\000\000n_i\200\000\000\000\000\000\000\002\003\200\000\000\000\000\000\000\000\003\200\000\000\000\000\000\000\000"
    │   ├─## table prefix
    │   │ └─table: 110
    │   └─## table index key
    │     ├─table: 110
    │     ├─index: 2
    │     └─"\003\200\000\000\000\000\000\000\000\003\200\000\000\000\000\000\000\000"
    │       └─## decode index values
    │         ├─kind: Int64, value: 0
    │         └─kind: Int64, value: 0
    └─## table prefix
      └─table: 255
```
