--[[
  Resolve whether a job should go to the wait or paused list, and whether
  workers should be signaled immediately.
]]
local function getTargetQueueList(queueMetaKey, activeKey, waitKey, pausedKey)
  local queueAttributes = rcall("HMGET", queueMetaKey, "paused", "concurrency")

  if queueAttributes[1] then
    return pausedKey, true
  end

  if queueAttributes[2] then
    local activeCount = rcall("LLEN", activeKey)
    if activeCount >= tonumber(queueAttributes[2]) then
      return waitKey, true
    end
  end

  return waitKey, false
end
