#!/bin/bash


function set_cgroup {
  local pid=$1
  local cpuCores=$2
  local cpuUs=-1

  if [ -z "$pid" ]; then
    echo "pid is empty"
    return 0
  fi

  if [ -z "$cpuCores" ]; then
    return 0
  fi

  if [ "$cpuCores" -le 0 ]; then
    return 0
  else
    cpuUs=$(printf "%.0f" `echo "scale=1; $cpuCores * 100000" | bc`)
  fi

  local dir=/sys/fs/cgroup/cpu/user.slice/rss_group
  mkdir -p "$dir"
  echo "$cpuUs" > "$dir"/cpu.cfs_quota_us

  # Centos 7 cgroup process id written to the tasks file cannot take effect,
  # it needs to be written to the cgroup.procs file
  #echo "$pid" > /sys/fs/cgroup/cpu/rss_group/tasks
  echo "$pid" > "$dir"/cgroup.procs

  return 0
}