# Test 0170 Share Consumer Subscription Failure Report

**Generated:** 2026-04-10  
**Source:** job_logs (3).txt  
**Test:** 0170_share_consumer_subscription  
**Status:** FAILED  
**Duration:** 461.387 seconds

## Executive Summary

Test 0170_share_consumer_subscription failed with a timeout waiting for an admin operation result. The test hung for over 7 minutes (461 seconds) waiting for an IncrementalAlterConfigs operation to complete. The primary issue appears to be controller lookup failures and broker state transitions occurring during the test execution.

## Failure Details

**Error Message:**
```
[0170_share_consumer_subscription/ 15.220s] TEST FAILURE
### Test "0170_share_consumer_subscription" failed at test.c:6554:test_wait_admin_result() at Thu Apr  9 06:50:14 2026: ###
Timed out waiting for admin result (131072)
```

**Final Test Summary:**
```
|[31m 0170_share_consumer_subscription         |     FAILED | 461.387s [0m|
|[31m test_wait_admin_result():6554: Timed out waiting for admin result (131072)[0m
```

## Timeline of Events

### Initial Admin Operation (T+0s - 1775717399.003)
```
INCREMENTALALTERCONFIGS worker called in state initializing: Success
ADMIN: INCREMENTALALTERCONFIGS: looking up controller
Not selecting any broker for cluster connection: still suppressed for 49ms: lookup controller
```

### Controller Lookup Attempts (T+0s to T+30s)

The test repeatedly attempted to look up the controller for IncrementalAlterConfigs operations:

1. **1775717399.003** - Initial lookup
2. **1775717399.048** - Waiting for controller
3. **1775717399.056** - Still waiting  
4. **1775717399.059** - Looking up controller again
5. **1775717399.350** - Still waiting
6. **1775717399.688** - Looking up controller
7. **1775717400.026** - Waiting for controller
8. **1775717408.017** - Looking up controller (8 seconds later!)
9. **1775717409.086** - Looking up controller
10. **1775717414.175** - Looking up controller (at failure time)

### Critical Broker State Issues (T+14-15s)

At timestamp 1775717414.441-1775717414.449 (approximately 15 seconds into the test):

```
%7|1775717414.441|CGRPQUERY: Group "share-single-subscribe": no broker available for coordinator query
%7|1775717414.441|CONNECT: Cluster connection already in progress: coordinator query
```

**Broker Decommissioning:**
```
%7|1775717414.442|FAIL: localhost:49673/bootstrap: Decommissioning this broker (after 3740ms in state UP) (_DESTROY_BROKER)
%7|1775717414.449|FAIL: localhost:49663/bootstrap: Decommissioning this broker (after 3386ms in state UP) (_DESTROY_BROKER)
%7|1775717414.449|TERMINATE: localhost:49663/bootstrap: Handle is terminating in state DOWN
```

### Successful Response (T+30s - Too Late)

At timestamp 1775717429.461 (approximately 30 seconds into the test), a response finally arrived:

```
RECV: localhost:49673/4: Received IncrementalAlterConfigsRequestResponse (v1, 35 bytes, CorrId 2, rtt 359.62ms)
INCREMENTALALTERCONFIGS worker called in state waiting for response from broker: Success
```

**Analysis:** The response took ~30 seconds to arrive from when it was first initiated, with a round-trip time of 359ms indicating network/broker latency issues.

## Root Cause Analysis

### 1. Controller Lookup Failures
The INCREMENTALALTERCONFIGS operation requires a controller lookup, but the test framework could not reliably locate or communicate with the Kafka controller for extended periods.

### 2. Broker State Transitions
Multiple brokers were in transitional states (UP -> DOWN -> INIT -> CONNECT -> APIVERSION_QUERY) during the critical admin operation period, preventing stable connections.

### 3. Coordinator Unavailability  
Share group coordinator was unavailable for the group "share-single-subscribe", indicated by:
```
no broker available for coordinator query: intervaled in state wait-broker-transport
```

### 4. Concurrent Test Load
Three tests were running simultaneously (0170, 0171, 0172), potentially causing broker resource contention.

## API Version Negotiation Issues

During the failure window, multiple "Unknown" API keys were discovered:

- Unknown-74 through Unknown-92 (API keys 74-92)

These are legitimate unknown API keys (likely KIP-932 share consumer protocol extensions that librdkafka doesn't yet fully recognize in its version tables).

## Broker Information

**Cluster Details:**
- 3 brokers running on ports: 49663, 49668, 49673
- Node IDs: 2, 3, 4
- ClusterId: slHqxP71S7q7MSTSs2pM8A
- Controller: Node 2 (localhost:49663)

**Connection State at Failure:**
- localhost:49663/bootstrap: TERMINATING (decommissioned after 3386ms in UP)
- localhost:49673/bootstrap: TERMINATING (decommissioned after 3740ms in UP)
- localhost:49668: Various consumers/producers still connecting

## Request for Broker Team Investigation

### Questions for Broker Team:

1. **Why is the IncrementalAlterConfigs operation taking 30+ seconds to respond?**
   - Expected RTT should be <100ms, observed was 359ms
   - Total operation time was ~30 seconds

2. **Why are controller lookups failing repeatedly?**
   - The client made 10+ attempts to lookup the controller over 30 seconds
   - Connection suppressions of 40-50ms were reported

3. **Are there broker-side timeout configurations affecting admin operations?**
   - The admin result (131072) was never received by the test framework
   - This suggests either:
     - Broker dropped the response
     - Broker never completed the operation
     - Response was malformed/unrecognized

4. **Share Group Coordinator stability?**
   - Share group "share-single-subscribe" couldn't find coordinator
   - Is this related to KIP-932 share group implementation issues?

5. **Under what conditions would IncrementalAlterConfigs not return a result?**
   - Even failures should return error responses
   - Result code 131072 appears to be an internal librdkafka admin operation ID, not a Kafka protocol error code

## Recommended Actions

1. **Enable broker-side debug logging** for:
   - Controller election/lookup
   - IncrementalAlterConfigs request handling
   - Share group coordinator operations

2. **Check broker logs** around timestamp Thu Apr 9 06:49:59 - 06:50:30 2026 for:
   - Controller changes/elections
   - Slow request warnings
   - Share group coordinator errors

3. **Verify broker resource availability:**
   - CPU/memory pressure during parallel test execution
   - Thread pool exhaustion
   - Network socket availability

4. **Review KIP-932 share consumer implementation:**
   - Are there known issues with share group coordinator under load?
   - Are admin operations affected by share consumer state?

## Additional Context

**Deletion Context:**
After all tests completed, there was an attempt to delete all test topics:
```
====> Deleting all test topics with <====a timeout of 2 minutes
TEST FAILURE
Assertion failed: (0), function test_fail0, file test.c, line 7813.
```

This suggests the cleanup phase also encountered issues, possibly related to the same underlying broker communication problems.

---

**Note:** The specific error pattern "UNKNOWN:''" mentioned was not found in the analyzed log file (job_logs (3).txt). If this error appears in broker-side logs or a different client log file, please provide those details for further analysis.
