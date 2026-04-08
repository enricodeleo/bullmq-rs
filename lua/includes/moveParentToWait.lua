--[[
  Release a parent from waiting-children into the correct target queue.
]]
local function moveParentToWait(parentQueueKey, parentKey, parentId, timestamp)
  local waitKey = parentQueueKey .. ":wait"
  local activeKey = parentQueueKey .. ":active"
  local pausedKey = parentQueueKey .. ":paused"
  local prioritizedKey = parentQueueKey .. ":prioritized"
  local waitingChildrenKey = parentQueueKey .. ":waiting-children"
  local eventsKey = parentQueueKey .. ":events"
  local metaKey = parentQueueKey .. ":meta"
  local markerKey = parentQueueKey .. ":marker"
  local pcKey = parentQueueKey .. ":pc"

  local target, isPausedOrMaxed = getTargetQueueList(metaKey, activeKey, waitKey, pausedKey)
  local priority = tonumber(rcall("HGET", parentKey, "priority")) or 0

  rcall("ZREM", waitingChildrenKey, parentId)

  if priority > 0 then
    addJobWithPriority(markerKey, prioritizedKey, priority, parentId, pcKey, isPausedOrMaxed)
  else
    addJobInTargetList(target, markerKey, "LPUSH", isPausedOrMaxed, parentId)
  end

  rcall("XADD", eventsKey, "*", "event", "waiting", "jobId", parentId, "prev", "waiting-children")
end
