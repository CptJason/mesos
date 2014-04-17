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

#include <iostream>
#include <iomanip>
#include <list>

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>

#include <process/collect.hpp>
#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/id.hpp>
#include <process/io.hpp>
#include <process/reap.hpp>

#include <stout/check.hpp>
#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/strings.hpp>
#include <stout/uuid.hpp>

#include "common/type_utils.hpp"

#include "slave/paths.hpp"

#include "slave/containerizer/external_containerizer.hpp"

using std::list;
using std::map;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using tuples::tuple;

using namespace process;

// Process user environment.
extern char** environ;

namespace mesos {
namespace internal {
namespace slave {

using state::ExecutorState;
using state::FrameworkState;
using state::RunState;
using state::SlaveState;


ExternalContainerizer::ExternalContainerizer(const Flags& flags)
{
  process = new ExternalContainerizerProcess(flags);
  spawn(process);
}


ExternalContainerizer::~ExternalContainerizer()
{
  terminate(process);
  process::wait(process);
  delete process;
}


Future<Nothing> ExternalContainerizer::recover(
    const Option<state::SlaveState>& state)
{
  return dispatch(process, &ExternalContainerizerProcess::recover, state);
}


Future<ExecutorInfo> ExternalContainerizer::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const FrameworkID& frameworkId,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &ExternalContainerizerProcess::launch,
                  containerId,
                  taskInfo,
                  frameworkId,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> ExternalContainerizer::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return dispatch(process,
                  &ExternalContainerizerProcess::update,
                  containerId,
                  resources);
}


Future<ResourceStatistics> ExternalContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(process, &ExternalContainerizerProcess::usage, containerId);
}


Future<Containerizer::Termination> ExternalContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(process, &ExternalContainerizerProcess::wait, containerId);
}


void ExternalContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(process, &ExternalContainerizerProcess::destroy, containerId);
}


ExternalContainerizerProcess::ExternalContainerizerProcess(
    const Flags& _flags) : flags(_flags) {}


Future<Nothing> ExternalContainerizerProcess::recover(
    const Option<state::SlaveState>& state)
{
  // Filter the executor run states that we attempt to recover and do
  // so.
  if (state.isSome()) {
    foreachvalue (const FrameworkState& framework, state.get().frameworks) {
      foreachvalue (const ExecutorState& executor, framework.executors) {

        VLOG(2) << "Recovering executor '" << executor.id
                << "' of framework " << framework.id;

        if (executor.info.isNone()) {
          LOG(WARNING) << "Skipping recovery of executor '" << executor.id
                       << "' of framework " << framework.id
                       << " because its info could not be recovered";
          continue;
        }

        if (executor.latest.isNone()) {
          LOG(WARNING) << "Skipping recovery of executor '" << executor.id
                       << "' of framework " << framework.id
                       << " because its latest run could not be recovered";
          continue;
        }

        // We are only interested in the latest run of the executor!
        const ContainerID& containerId = executor.latest.get();
        Option<RunState> run = executor.runs.get(containerId);
        CHECK_SOME(run);

        // We need the pid so the reaper can monitor the executor so skip this
        // executor if it's not present. This is not an error because the slave
        // will try to wait on the container which will return a failed
        // Termination and everything will get cleaned up.
        if (!run.get().forkedPid.isSome()) {
          continue;
        }

        if (run.get().completed) {
          LOG(INFO) << "Skipping recovery of executor '" << executor.id
                    << "' of framework " << framework.id
                    << " because its latest run '" << containerId << "'"
                    << " is completed";
          continue;
        }

        const pid_t pid(run.get().forkedPid.get());

        running.put(containerId, Owned<Running>(new Running(pid)));

        process::reap(pid)
          .onAny(defer(
            PID<ExternalContainerizerProcess>(this),
            &ExternalContainerizerProcess::reaped,
            containerId,
            lambda::_1));

        // Recreate the sandbox information.
        // TODO (tillt): This recreates logic that is supposed to be
        // further up, within the slave implementation.
        const string& directory = paths::createExecutorDirectory(
          flags.work_dir,
          state.get().id,
          framework.id,
          executor.id,
          containerId);

        CHECK(framework.info.isSome());

        const Option<string>& user = flags.switch_user
          ? Option<string>(framework.info.get().user()) : None();

        sandboxes.put(
          containerId,
          Owned<Sandbox>(new Sandbox(directory, user)));
      }
    }
  }

  return Nothing();
}


Future<ExecutorInfo> ExternalContainerizerProcess::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const FrameworkID& frameworkId,
    const std::string& directory,
    const Option<std::string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  VLOG(1) << "Launch triggered for container '" << containerId << "'";

  // Get the executor from our task. If no executor is associated with
  // the given task, this function renders an ExecutorInfo using the
  // mesos-executor as its command.
  ExecutorInfo executor = containerExecutorInfo(flags, taskInfo, frameworkId);
  executor.mutable_resources()->MergeFrom(taskInfo.resources());

  if (running.contains(containerId)) {
    return Failure("Cannot start already running container '"
      + containerId.value() + "'");
  }

  sandboxes.put(containerId, Owned<Sandbox>(new Sandbox(directory, user)));

  map<string, string> environment = executorEnvironment(
      executor,
      directory,
      slaveId,
      slavePid,
      checkpoint,
      flags.recovery_timeout);
  environment["HADOOP_HOME"] = flags.hadoop_home;

  TaskInfo task;
  task.CopyFrom(taskInfo);
  CommandInfo* command = task.has_executor()
    ? task.mutable_executor()->mutable_command()
    : task.mutable_command();
  // When the selected command has no container attached, use the
  // default from the slave startup flags, if available.
  if (!command->has_container()) {
    if (flags.default_container_image.isSome()) {
      command->mutable_container()->set_image(
          flags.default_container_image.get());
    } else {
      LOG(INFO) << "No container specified in task and no default given. "
                << "The external containerizer will have to fill in "
                << "defaults.";
    }
  }

  stringstream output;
  task.SerializeToOstream(&output);

  vector<string> parameters;
  parameters.push_back("--mesos-executor");
  parameters.push_back(path::join(flags.launcher_dir, "mesos-executor"));

  Try<ChildProcess> invoked = invoke(
      "launch",
      parameters,
      environment,
      containerId,
      output.str());

  if (invoked.isError()) {
    return Failure("Launch of container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Record the process.
  running.put(containerId, Owned<Running>(new Running(invoked.get().pid)));

  // Observe the process status and install a callback for status
  // changes.
  invoked.get().status
    .onAny(lambda::bind(os::close, invoked.get().err))
    .onAny(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::reaped,
        containerId,
        lambda::_1));

  VLOG(2) << "Now awaiting data from pipe...";

  // Read from the result-pipe and invoke callbacks when reaching EOF.
  return read(invoked.get().out)
    .onAny(lambda::bind(os::close, invoked.get().out))
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_launch,
        containerId,
        invoked.get().pid,
        frameworkId,
        executor,
        slaveId,
        checkpoint,
        lambda::_1));
}


Future<ExecutorInfo> ExternalContainerizerProcess::_launch(
    const ContainerID& containerId,
    pid_t pid,
    const FrameworkID& frameworkId,
    const ExecutorInfo executorInfo,
    const SlaveID& slaveId,
    bool checkpoint,
    const Future<string>& future)
{
  VLOG(1) << "Launch callback triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  if (!future.isReady()) {
    terminate(containerId);
    return Failure("Could not receive any result from external containerizer");
  }

  string result = future.get();

  Try<bool> support = commandSupported(result);
  if (support.isError()) {
    terminate(containerId);
    return Failure(support.error());
  }

  if (!support.get()) {
    // We generally need to use an internal implementation in these
    // cases.
    // For the specific case of a launch however, there can not be an
    // internal implementation for a external containerizer, hence
    // we need to fail or even abort at this point.
    // TODO(tillt): Consider using posix-isolator as a fall back.
    terminate(containerId);
    return Failure("External containerizer does not support launch");
  }

  VLOG(1) << "Launch supported by external containerizer";

  ExternalStatus ps;
  if (!ps.ParseFromString(result)) {
    // TODO(tillt): Consider not terminating the containerizer due
    // to protocol breach but only fail the operation.
    terminate(containerId);
    return Failure("Could not parse launch result protobuf (error: "
      + protobufError(ps) + ")");
  }

  VLOG(2) << "Launch result: '" << ps.message() << "'";

  // Checkpoint the container's pid if requested.
  if (checkpoint) {
    const string& path = slave::paths::getForkedPidPath(
        slave::paths::getMetaRootDir(flags.work_dir),
        slaveId,
        frameworkId,
        executorInfo.executor_id(),
        containerId);

    LOG(INFO) << "Checkpointing container '" << containerId << "' pid "
              << pid << " to '" << path <<  "'";

    Try<Nothing> checkpointed =
        slave::state::checkpoint(path, stringify(pid));

    if (checkpointed.isError()) {
      terminate(containerId);
      return Failure("Failed to checkpoint container '" + containerId.value()
        + "' pid " + stringify(pid) + " to '" + path + "'");
    }
  }

  VLOG(1) << "Launch finishing up for container '" << containerId << "'";
  return executorInfo;
}


Future<Containerizer::Termination> ExternalContainerizerProcess::wait(
    const ContainerID& containerId)
{
  VLOG(1) << "Wait triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    LOG(ERROR) << "not running";
    return Failure("Container '" + containerId.value() + "' not running");
  }

  Try<ChildProcess> invoked = invoke("wait", containerId);

  if (invoked.isError()) {
    LOG(ERROR) << "not running";
    terminate(containerId);
    return Failure("Wait on container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Await both, input from the pipe as well as an exit of the
  // process.
  await(read(invoked.get().out), invoked.get().status)
    .onAny(lambda::bind(os::close, invoked.get().out))
    .onAny(lambda::bind(os::close, invoked.get().err))
    .onAny(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_wait,
        containerId,
        lambda::_1));

  return running[containerId]->termination.future();
}


void ExternalContainerizerProcess::_wait(
    const ContainerID& containerId,
    const Future<ResultFutures>& future)
{
  VLOG(1) << "Wait callback triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId << "' not running";
    return;
  }

  Owned<Running> run = running[containerId];

  string result;
  Try<bool> support = commandSupported(future, result);
  if (support.isError()) {
    run->termination.fail(support.error());
    return;
  }

  // Final clean up is delayed until someone has waited on the
  // container so set the future to indicate this has occurred.
  if (!run->waited.future().isReady()) {
    run->waited.set(true);
  }

  if (support.get()) {
    VLOG(1) << "Wait supported by external containerizer";

    ExternalTermination pt;
    if (!pt.ParseFromString(result)) {
      // TODO(tillt): Consider not terminating the containerizer due
      // to protocol breach but only fail the operation.
      run->termination.fail("Could not parse wait result protobuf (error: "
        + protobufError(pt) + ")");
      return;
    }

    VLOG(2) << "Wait result: '" << pt.DebugString() << "'";

    // Satisfy the promise with the termination information we got
    // from the external containerizer
    Containerizer::Termination termination(
        pt.status(),
        pt.killed(),
        pt.message());
    run->termination.set(termination);

    return;
  }
  VLOG(1) << "Wait requests default implementation";
}


Future<Nothing> ExternalContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  VLOG(1) << "Update triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "'' not running");
  }

  running[containerId]->resources = resources;

  // Wrap the Resource protobufs into a ResourceArray protobuf to
  // avoid any problems with streamed protobufs.
  // See http://goo.gl/d1x14F for more on that issue.
  ResourceArray resourceArray;
  foreach (const Resource& r, resources) {
    Resource *resource = resourceArray.add_resource();
    resource->CopyFrom(r);
  }

  stringstream output;
  resourceArray.SerializeToOstream(&output);

  Try<ChildProcess> invoked = invoke("update", containerId, output.str());

  if (invoked.isError()) {
    terminate(containerId);
    return Failure("Update of container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Await both, input from the pipe as well as an exit of the
  // process.
  return await(read(invoked.get().out), invoked.get().status)
    .onAny(lambda::bind(os::close, invoked.get().out))
    .onAny(lambda::bind(os::close, invoked.get().err))
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_update,
        containerId,
        lambda::_1));
}


Future<Nothing> ExternalContainerizerProcess::_update(
    const ContainerID& containerId,
    const Future<ResultFutures>& future)
{
  VLOG(1) << "Update callback triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  string result;
  Try<bool> support = commandSupported(future, result);
  if (support.isError()) {
    terminate(containerId);
    return Failure(support.error());
  }

  if (support.get()) {
    VLOG(1) << "Update supported by external containerizer";

    ExternalStatus ps;
    if (!ps.ParseFromString(result)) {
      // TODO(tillt): Consider not terminating the containerizer due
      // to protocol breach but only fail the operation.
      terminate(containerId);
      return Failure("Could not parse update result protobuf (error: "
        + protobufError(ps) + ")");
    }

    VLOG(2) << "Update result: '" << ps.message() << "'";

    return Nothing();
  }

  VLOG(1) << "Update requests default implementation";
  LOG(INFO) << "Update ignoring updates as the external containerizer does"
            << "not support it";

  return Nothing();
}


Future<ResourceStatistics> ExternalContainerizerProcess::usage(
    const ContainerID& containerId)
{
  VLOG(1) << "Usage triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "'' not running");
  }

  Try<ChildProcess> invoked = invoke("usage", containerId);

  if (invoked.isError()) {
    terminate(containerId);
    return Failure("Usage on container '" + containerId.value()
      + "' failed (error: " + invoked.error() + ")");
  }

  // Await both, input from the pipe as well as an exit of the
  // process.
  return await(read(invoked.get().out), invoked.get().status)
    .onAny(lambda::bind(os::close, invoked.get().out))
    .onAny(lambda::bind(os::close, invoked.get().err))
    .then(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_usage,
        containerId,
        lambda::_1));
}


Future<ResourceStatistics> ExternalContainerizerProcess::_usage(
    const ContainerID& containerId,
    const Future<ResultFutures>& future)
{
  VLOG(1) << "Usage callback triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    return Failure("Container '" + containerId.value() + "' not running");
  }

  string result;
  Try<bool> support = commandSupported(future, result);
  if (support.isError()) {
    terminate(containerId);
    return Failure(support.error());
  }

  ResourceStatistics statistics;

  if (!support.get()) {
    VLOG(1) << "Usage requests default implementation";

    pid_t pid = running[containerId]->pid;

    Result<os::Process> process = os::process(pid);

    if (!process.isSome()) {
      return Failure(process.isError()
        ? process.error()
        : "Process does not exist or may have terminated already");
    }

    statistics.set_timestamp(Clock::now().secs());

    // Set the resource allocations.
    // TODO(idownes): After recovery resources won't be known until
    // after an update() because they aren't part of the SlaveState.
    const Resources& resources = running[containerId]->resources;
    const Option<Bytes>& mem = resources.mem();
    if (mem.isSome()) {
      statistics.set_mem_limit_bytes(mem.get().bytes());
    }

    const Option<double>& cpus = resources.cpus();
    if (cpus.isSome()) {
      statistics.set_cpus_limit(cpus.get());
    }

    if (process.get().rss.isSome()) {
      statistics.set_mem_rss_bytes(process.get().rss.get().bytes());
    }

    // We only show utime and stime when both are available, otherwise
    // we're exposing a partial view of the CPU times.
    if (process.get().utime.isSome() && process.get().stime.isSome()) {
      statistics.set_cpus_user_time_secs(process.get().utime.get().secs());
      statistics.set_cpus_system_time_secs(process.get().stime.get().secs());
    }

    // Now aggregate all descendant process usage statistics.
    const Try<set<pid_t> >& children = os::children(pid, true);

    if (children.isError()) {
      return Failure("Failed to get children of "
        + stringify(pid) + ": " + children.error());
    }

    // Aggregate the usage of all child processes.
    foreach (pid_t child, children.get()) {
      process = os::process(child);

      // Skip processes that disappear.
      if (process.isNone()) {
        continue;
      }

      if (process.isError()) {
        LOG(WARNING) << "Failed to get status of descendant process " << child
                     << " of parent " << pid << ": "
                     << process.error();
        continue;
      }

      if (process.get().rss.isSome()) {
        statistics.set_mem_rss_bytes(
            statistics.mem_rss_bytes() + process.get().rss.get().bytes());
      }

      // We only show utime and stime when both are available,
      // otherwise we're exposing a partial view of the CPU times.
      if (process.get().utime.isSome() && process.get().stime.isSome()) {
        statistics.set_cpus_user_time_secs(
            statistics.cpus_user_time_secs()
              + process.get().utime.get().secs());
        statistics.set_cpus_system_time_secs(
            statistics.cpus_system_time_secs()
              + process.get().stime.get().secs());
      }
    }
  } else {
    VLOG(1) << "Usage supported by external containerizer";

    if (!statistics.ParseFromString(result)) {
      // TODO(tillt): Consider not terminating the containerizer due
      // to protocol breach but only fail the operation.
      terminate(containerId);
      return Failure("Could not parse usage result protobuf (error: "
        + protobufError(statistics) + ")");
    }

    VLOG(2) << "Usage result: '" << statistics.DebugString() << "'";
  }

  LOG(INFO) << "containerId '" << containerId << "' "
            << "total mem usage "
            << statistics.mem_rss_bytes() << " "
            << "total CPU user usage "
            << statistics.cpus_user_time_secs() << " "
            << "total CPU system usage "
            << statistics.cpus_system_time_secs();

  return statistics;
}


void ExternalContainerizerProcess::destroy(const ContainerID& containerId)
{
  VLOG(1) << "Destroy triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId << "' not running";
    return;
  }

  Try<ChildProcess> invoked = invoke("destroy", containerId);

  if (invoked.isError()) {
    LOG(ERROR) << "Destroy of container '" << containerId
               << "' failed (error: " << invoked.error() << ")";
    terminate(containerId);
    return;
  }

  // Await both, input from the pipe as well as an exit of the
  // process.
  await(read(invoked.get().out), invoked.get().status)
    .onAny(lambda::bind(os::close, invoked.get().out))
    .onAny(lambda::bind(os::close, invoked.get().err))
    .onAny(defer(
        PID<ExternalContainerizerProcess>(this),
        &ExternalContainerizerProcess::_destroy,
        containerId,
        lambda::_1));
}


void ExternalContainerizerProcess::_destroy(
    const ContainerID& containerId,
    const Future<ResultFutures>& future)
{
  VLOG(1) << "Destroy callback triggered on container '" << containerId << "'";

  if (!running.contains(containerId)) {
    LOG(ERROR) << "Container '" << containerId.value() << "' not running";
    return;
  }

  string result;
  Try<bool> support = commandSupported(future, result);

  if (!support.isError()) {
    if (support.get()) {
      VLOG(1) << "Destroy supported by external containerizer";

      ExternalStatus ps;
      if (!ps.ParseFromString(result)) {
        LOG(ERROR) << "Could not parse update result protobuf (error: "
                   << protobufError(ps) << ")";
        // Continue regular program flow as we need to kill the
        // containerizer process.
      }
      VLOG(2) << "Destroy result: '" << ps.message() << "'";
    } else {
      VLOG(1) << "Destroy requests default implementation";
    }
  }

  // Additionally to the optional external destroy-command, we need to
  // terminate the external containerizer's process.
  terminate(containerId);
}


void ExternalContainerizerProcess::reaped(
    const ContainerID& containerId,
    const Future<Option<int> >& status)
{
  if (!running.contains(containerId)) {
    LOG(WARNING) << "Container '" << containerId << "' not running";
    return;
  }

  Timer::cancel(escalationTimer);

  VLOG(2) << "status-future on containerId '" << containerId
          << "' has reached: "
          << (status.isReady() ? "READY" :
             status.isFailed() ? "FAILED: " + status.failure() :
             "DISCARDED");

  Future<Containerizer::Termination> future;
  if (!status.isReady()) {
    // Something has gone wrong, probably an unsuccessful terminate().
    future = Failure(
        "Failed to get status: " +
        (status.isFailed() ? status.failure() : "discarded"));
  } else {
    LOG(INFO) << "Container '" << containerId << "' "
              << "has terminated with "
              << (status.get().isSome() ?
                  "exit-code: " + stringify(status.get().get()) :
                  "no result");
    future = Containerizer::Termination(
        status.get(), false, "Containerizer terminated");
  }

  // Set the promise to alert others waiting on this container.
  running[containerId]->termination.set(future);

  // Ensure someone notices this termination by deferring final clean
  // up until the container has been waited on.
  running[containerId]->waited.future()
    .onAny(defer(PID<ExternalContainerizerProcess>(this),
                 &ExternalContainerizerProcess::cleanup,
                 containerId));
}


void ExternalContainerizerProcess::cleanup(
    const ContainerID& containerId)
{
  VLOG(1) << "Callback performing final cleanup of running state";

  if (sandboxes.contains(containerId)) {
    sandboxes.erase(containerId);
  } else {
    LOG(WARNING) << "Container '" << containerId << "' not sandboxed";
  }

  if (running.contains(containerId)) {
    running.erase(containerId);
  } else {
    LOG(WARNING) << "Container '" << containerId << "' not running anymore";
  }
}


void ExternalContainerizerProcess::escalateTerminate(
    const ContainerID& containerId)
{
  if (!running.contains(containerId)) {
    LOG(WARNING) << "Container '" << containerId << "' not running";
    return;
  }

  pid_t pid = running[containerId]->pid;

  VLOG(2) << "About to send a SIGKILL to: " << pid;

  Try<list<os::ProcessTree> > trees =
    os::killtree(pid, SIGKILL, true, true);

  if (trees.isError()) {
    LOG(WARNING) << "Failed to kill the process tree rooted at pid "
                 << pid << ": " << trees.error();
    return;
  }
}


void ExternalContainerizerProcess::terminate(const ContainerID& containerId)
{
  if (!running.contains(containerId)) {
    LOG(WARNING) << "Container '" << containerId << "' not running";
    return;
  }

  pid_t pid = running[containerId]->pid;

  VLOG(2) << "About to send a SIGTERM to: " << pid;

  // Terminate the containerizer and all processes in the containerizer's
  // process group and session.
  Try<list<os::ProcessTree> > trees =
    os::killtree(pid, SIGTERM, true, true);

  if (trees.isError()) {
    LOG(WARNING) << "Failed to kill the process tree rooted at pid "
                 << pid << ": " << trees.error();
    return;
  }

  LOG(INFO) << "Terminated the following process tree/s:\n"
            << stringify(trees.get());

  Duration graceTimeout = Seconds(3);
  escalationTimer = delay(
    graceTimeout,
    self(),
    &Self::escalateTerminate,
    containerId);
}


Future<string> ExternalContainerizerProcess::read(int pipe)
{
  Try<Nothing> nonblock = os::nonblock(pipe);

  if (nonblock.isError()) {
    return Failure("Failed to accept nonblock (error: " + nonblock.error()
      + ")");
  }
  // Read all data from the input pipe until it is closed by the
  // sender.
  return io::read(pipe);
}


Try<bool> ExternalContainerizerProcess::commandSupported(
    const Future<ResultFutures>& future,
    string& resultString)
{
  if (!future.isReady()) {
    return Error("Could not receive any result");
  }

  Try<string> res = result(future.get());
  if (res.isError()) {
    return Error(res.error());
  }
  resultString = res.get();

  Try<int> stat = status(future.get());
  if (stat.isError()) {
    return Error(stat.error());
  }

  return commandSupported(resultString, stat.get());
}


Try<bool> ExternalContainerizerProcess::commandSupported(
    const string& result,
    int status)
{
  // The status is a waitpid-result which has to be checked for SIGNAL
  // based termination before masking out the exit-code.
  if (!WIFEXITED(status)) {
    return Error(string("External containerizer terminated by signal ")
      + strsignal(WTERMSIG(status)));
  }

  int exitCode = WEXITSTATUS(status);
  if (exitCode != 0) {
    return Error("External containerizer failed (exit: "
      + stringify(exitCode) + ")");
  }

  bool implemented = result.length() != 0;
  if (!implemented) {
    LOG(INFO) << "External containerizer exited 0 and had no output, which "
              << "requests the default implementation";
  }

  return implemented;
}


Try<string> ExternalContainerizerProcess::result(
    const ResultFutures& futures)
{
  Future<string> resultFuture = tuples::get<0>(futures);

  if (resultFuture.isFailed()) {
    return Error("Could not receive any result (error: "
      + resultFuture.failure() + ")");
  }
  return resultFuture.get();
}


Try<int> ExternalContainerizerProcess::status(const ResultFutures& futures)
{
  Future<Option<int> > statusFuture = tuples::get<1>(futures);

  if (statusFuture.isFailed()) {
    return Error("Could not get an exit-code (error: "
      + statusFuture.failure() + ")");
  }
  Option<int> statusOption = statusFuture.get();

  if (statusOption.isNone()) {
    return Error("No exit-code available");
  }
  return statusOption.get();
}


Try<ExternalContainerizerProcess::ChildProcess>
  ExternalContainerizerProcess::invoke(
      const string& command,
      const ContainerID& containerId)
{
  string output;
  return invoke(
      command,
      containerId,
      output);
}


Try<ExternalContainerizerProcess::ChildProcess>
  ExternalContainerizerProcess::invoke(
      const string& command,
      const ContainerID& containerId,
      const string& output)
{
  vector<string> parameters;
  map<string, string> environment;
  return invoke(
      command,
      parameters,
      environment,
      containerId,
      output);
}


Try<ExternalContainerizerProcess::ChildProcess>
  ExternalContainerizerProcess::invoke(
      const string& command,
      const vector<string>& parameters,
      const map<string, string>& environment,
      const ContainerID& containerId,
      const string& output)
{
  CHECK(flags.containerizer_path.isSome())
    << "containerizer_path not set";

  CHECK(sandboxes.contains(containerId));

  VLOG(1) << "Invoking external containerizer for method '" << command << "'";

  // Construct the argument vector.
  vector<string> argv;
  argv.push_back(flags.containerizer_path.get());
  argv.push_back(command);
  argv.push_back(containerId.value());
  if (parameters.size()) {
    argv.insert(argv.end(), parameters.begin(), parameters.end());
  }

  VLOG(2) << "calling: [" << strings::join(" ", argv) << "]";
  VLOG(2) << "directory: " << sandboxes[containerId]->directory;
  if(sandboxes[containerId]->user.isSome()) {
    VLOG(2) << "user: " << sandboxes[containerId]->user.get();
  }

  // Re/establish the sandbox conditions for the containerizer.
  if (sandboxes[containerId]->user.isSome()) {
    Try<Nothing> chown = os::chown(
        sandboxes[containerId]->user.get(),
        sandboxes[containerId]->directory);
    if (chown.isError()) {
      return Error(string("Failed to chown work directory: ") +
        strerror(errno));
    }
  }

  // Fork exec of external process. Run a chdir within the child-
  // context.
  // TODO(tillt): Shift over to using process::Subprocess once it
  // supports:
  // - in-child-context commands (chdir) after fork and before
  // exec.
  // - direct execution without the invocation of /bin/sh.
  // - external ownership of pipe file descriptors.
  Try<ChildProcess> external = childProcessStart(
      argv,
      sandboxes[containerId]->directory,
      environment);

  if (external.isError()) {
    return Error(string("Failed to execute external containerizer: ")
      + external.error());
  }

  // Redirect output (stderr) from the containerizer to log file in the
  // executor work directory, chown'ing it if a user is specified.
  Try<int> err = os::open(
      path::join(sandboxes[containerId]->directory, "stderr"),
      O_WRONLY | O_CREAT | O_APPEND | O_NONBLOCK,
      S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO);

  if (err.isError()) {
    return Error("Failed to redirect stderr:" + err.error());
  }

  Try<Nothing> nonblock = os::nonblock(external.get().err);
  if (nonblock.isError()) {
    os::close(err.get());
    return Error("Failed to redirect stderr:" + nonblock.error());
  }

  io::splice(external.get().err, err.get())
    .onAny(lambda::bind(&os::close, err.get()));

  // Transmit protobuf data via stdout towards the external
  // containerizer.
  if (output.length() > 0) {
    VLOG(2) << "Writing to child's standard input "
            << "(" << output.length() << " bytes)";

    ssize_t len = output.length();
    if (write(external.get().in, output.c_str(), len) < len) {
      return Error("Failed to write protobuf to pipe");
    }

    VLOG(2) << "Data written, closing pipe";
  }

  // We are done sending data to the external process, close the pipe.
  os::close(external.get().in);

  VLOG(2) << "Returning PID:" << external.get().pid;
  VLOG(2) << "Child output pipe:" << external.get().out;

  return external;
}


Try<ExternalContainerizerProcess::ChildProcess>
  ExternalContainerizerProcess::childProcessStart(
      const vector<string>& argv,
      const string& directory,
      const map<string, string>& childEnvironment) const
{
  // Create pipes for stdin, stdout, stderr.
  // Index 0 is for reading, and index 1 is for writing.
  int stdinPipe[2];
  int stdoutPipe[2];
  int stderrPipe[2];

  if (pipe(stdinPipe) == -1) {
    return ErrnoError("Failed to create pipe");
  } else if (pipe(stdoutPipe) == -1) {
    os::close(stdinPipe[0]);
    os::close(stdinPipe[1]);
    return ErrnoError("Failed to create pipe");
  } else if (pipe(stderrPipe) == -1) {
    os::close(stdinPipe[0]);
    os::close(stdinPipe[1]);
    os::close(stdoutPipe[0]);
    os::close(stdoutPipe[1]);
    return ErrnoError("Failed to create pipe");
  }

  // Render c-string argument vector.
  vector<const char*> arguments;
  foreach(const string& arg, argv) {
    arguments.push_back(arg.c_str());
  }
  arguments.push_back(NULL);

  // Render c-string environment vector while appending the slave
  // process environment.
  std::vector<const char*> environment;
  // Transform environment map into joined environment strings.
  std::vector<Owned<const string> > joinedChildEnv;
  foreachpair (const string& key, const string& value, childEnvironment) {
    Owned<const string> joined = Owned<const string>(
        new const string(key + "=" + value));
    joinedChildEnv.push_back(joined);
    environment.push_back(joined->c_str());
  }
  char** parentEnv = environ;
  while (*parentEnv) {
    environment.push_back(*parentEnv);
    ++parentEnv;
  }
  environment.push_back(NULL);

  pid_t pid;
  if ((pid = fork()) == -1) {
    os::close(stdinPipe[0]);
    os::close(stdinPipe[1]);
    os::close(stdoutPipe[0]);
    os::close(stdoutPipe[1]);
    os::close(stderrPipe[0]);
    os::close(stderrPipe[1]);
    return ErrnoError("Failed to fork");
  }

  if (pid == 0) {
    // Child.
    // Close parent's end of the pipes.
    os::close(stdinPipe[1]);
    os::close(stdoutPipe[0]);
    os::close(stderrPipe[0]);

    // Make our pipes look like stdin, stderr, stdout before we exec.
    while (::dup2(stdinPipe[0], STDIN_FILENO)   == -1 && errno == EINTR);
    while (::dup2(stdoutPipe[1], STDOUT_FILENO) == -1 && errno == EINTR);
    while (::dup2(stderrPipe[1], STDERR_FILENO) == -1 && errno == EINTR);

    // Close the copies.
    os::close(stdinPipe[0]);
    os::close(stdoutPipe[1]);
    os::close(stderrPipe[1]);

    // Put child into its own process session to prevent slave suicide
    // on child process SIGKILL/SIGTERM.
    if (::setsid() < 0) {
      ABORT("Could not put external containerizer in its own process session");
    }

    // Re/establish the sandbox conditions for the containerizer.
    if (::chdir(directory.c_str()) < 0) {
      ABORT("Failed to chdir to work directory");
    }

    // Both args and envp have to be c-style typecasted at this point.
    // std::vector members are guaranteed to be organized in a linear
    // memory region, hence those typecasts are safe.
    ::execve(
        arguments[0],
        (char* const*)&arguments[0],
        (char* const*)&environment[0]);

    ABORT("Failed to execve ", arguments[0],  "\n");
  }

  // Parent.

  // Close the child's end of the pipes.
  os::close(stdinPipe[0]);
  os::close(stdoutPipe[1]);
  os::close(stderrPipe[1]);

  // Transfer ownership of the pipe's file descriptors. Start reaping
  // the child process.
  return ChildProcess(
      pid,
      stdinPipe[1],
      stdoutPipe[0],
      stderrPipe[0],
      process::reap(pid));
}


ExecutorInfo containerExecutorInfo(
    const Flags& flags,
    const TaskInfo& task,
    const FrameworkID& frameworkId)
{
  CHECK_NE(task.has_executor(), task.has_command())
    << "Task " << task.task_id()
    << " should have either CommandInfo or ExecutorInfo set but not both";

  if (!task.has_command()) {
      return task.executor();
  }

  ExecutorInfo executor;
  // Command executors share the same id as the task.
  executor.mutable_executor_id()->set_value(task.task_id().value());
  executor.mutable_framework_id()->CopyFrom(frameworkId);

  // Prepare an executor name which includes information on the
  // task and a possibly attached container.
  string name =
    "(External Containerizer Task: " + task.task_id().value();
  if (task.command().has_container()) {
    name += " Container image: " + task.command().container().image();
  }
  name += ")";

  executor.set_name("Command Executor " + name);
  executor.set_source(task.task_id().value());
  executor.mutable_command()->MergeFrom(task.command());
  return executor;
}


string protobufError(const google::protobuf::Message& message)
{
  vector<string> errors;
  message.FindInitializationErrors(&errors);
  return strings::join(", ", errors);
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {