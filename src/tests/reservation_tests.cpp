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

#include <gmock/gmock.h>

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include <mesos/executor.hpp>
#include <mesos/scheduler.hpp>

#include <process/clock.hpp>
#include <process/future.hpp>
#include <process/gmock.hpp>
#include <process/pid.hpp>

#include <stout/some.hpp>
#include <stout/strings.hpp>

#include "master/constants.hpp"
#include "master/detector.hpp"
#include "master/flags.hpp"
#include "master/master.hpp"

#include "master/allocator/allocator.hpp"
#include "master/allocator/mesos/hierarchical.hpp"

#include "tests/containerizer.hpp"
#include "tests/mesos.hpp"

using mesos::internal::master::allocator::HierarchicalDRFAllocator;

using mesos::internal::master::Master;

using mesos::internal::slave::Slave;

using process::PID;
using process::Future;

using std::map;
using std::string;
using std::vector;

using testing::_;
using testing::AtMost;
using testing::DoAll;
using testing::DoDefault;
using testing::Eq;
using testing::SaveArg;

namespace mesos {
namespace internal {
namespace tests {

template <typename T>
class ReservationTest : public MesosTest
{
protected:
  TestAllocator<T> allocator;
};


typedef ::testing::Types<HierarchicalDRFAllocator> AllocatorTypes;


// Causes all TYPED_TEST(MasterAllocatorTest, ...) to be run for
// each of the specified Allocator classes.
TYPED_TEST_CASE(ReservationTest, AllocatorTypes);


// This tests that a framework can send back a Reserve offer operation
// as a response to an offer, which updates the resources in the
// allocator and results in the reserved resources being reoffered to
// the framework. The framework then sends back an Unreserved offer
// operation to unreserve the reserved resources. Finally, We test
// that the framework receives the unreserved resources.
TYPED_TEST(ReservationTest, ReserveThenUnreserve)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _));

  EXPECT_CALL(sched, registered(&driver, _, _));

  // In first offer, expect all of the slave's unreserved resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _));

  // In the next offer, expect an offer with the reserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {RESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), reserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _));

  // In the next offer, expect an offer with the unreserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {UNRESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This tests that a framework can send back a Reserve followed by
// a LaunchTasks offer operation as a response to an offer, which
// updates the resources in the allocator then proceeds to launch
// the task with the reserved resources. The reserved resources are
// reoffered to the framework on task completion. The framework then
// sends back an Unreserved offer operation to unreserve the reserved
// resources. We test that the framework receives the unreserved
// resources.
TYPED_TEST(ReservationTest, ReserveAndLaunchThenUnreserve)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  MockExecutor exec(DEFAULT_EXECUTOR_ID);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(&exec, slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _));

  EXPECT_CALL(sched, registered(&driver, _, _));

  // In the first offer, expect all of the slave's resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  // Create a task.
  TaskInfo taskInfo = createTask(offer.slave_id(), reserved, "exit 1", exec.id);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _));

  EXPECT_CALL(exec, registered(_, _, _, _));

  EXPECT_CALL(exec, launchTask(_, _))
    .WillOnce(SendStatusUpdateFromTask(TASK_FINISHED));

  EXPECT_CALL(sched, statusUpdate(_, _))
    .WillOnce(DoDefault());

  // In the next offer, expect an offer with the reserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {RESERVE(reserved), LAUNCH({taskInfo})});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), reserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _));

  // In the next offer, expect an offer with the unreserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {UNRESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test launches 2 frameworks in the same role. framework1
// reserves resources by sending back a Reserve offer operation.
// We first test that framework1 receives the reserved resources,
// then on the next resource offer, framework1 declines the offer.
// This should lead to framework2 receiving the resources that
// framework1 reserved.
TYPED_TEST(ReservationTest, ReserveShareWithinRole)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  std::string role = "role";

  FrameworkInfo frameworkInfo1 = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo1.set_role(role);

  FrameworkInfo frameworkInfo2 = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo2.set_role(role);

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = role;

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched1;
  MesosSchedulerDriver driver1(
      &sched1, frameworkInfo1, master.get(), DEFAULT_CREDENTIAL);

  MockScheduler sched2;
  MesosSchedulerDriver driver2(
      &sched2, frameworkInfo2, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _)).Times(2);

  EXPECT_CALL(sched1, registered(&driver1, _, _));

  // In first offer, expect all of the slave's unreserved resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched1, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver1.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo1.role(), createReservationInfo(frameworkInfo1.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _));

  // In the next offer, expect an offer with the reserved resources.
  EXPECT_CALL(sched1, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver1.acceptOffers({offer.id()}, {RESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), reserved);

  Filters filters;
  filters.set_refuse_seconds(0);

  driver1.declineOffer(offer.id(), filters);

  // In the next offer, expect an offer with the reserved resources.
  EXPECT_CALL(sched2, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  EXPECT_CALL(sched2, registered(&driver2, _, _));

  driver2.start();

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), reserved);

  driver1.stop();
  driver1.join();

  driver2.stop();
  driver2.join();

  this->Shutdown();
}


// This tests that a Reserve offer operation where the specified
// resources do not match the framework's role is dropped.
TYPED_TEST(ReservationTest, DropReserveNonMatchingRole)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role1");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _));

  EXPECT_CALL(sched, registered(&driver, _, _));

  // In first offer, expect all of the slave's unreserved resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources reserved = unreserved.flatten(
      "role2", createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _)).Times(0);

  // In the next offer, expect an offer with the unreserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {RESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This tests that a Reserve offer operation where the specified
// resources do not match the framework's principal is dropped.
TYPED_TEST(ReservationTest, DropReserveNonMatchingPrincipal)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _));

  EXPECT_CALL(sched, registered(&driver, _, _));

  // In first offer, expect all of the slave's unreserved resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo("principal"));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  EXPECT_CALL(this->allocator, updateAllocation(_, _, _)).Times(0);

  // In the next offer, expect an offer with the unreserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {RESERVE(reserved)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This tests that a Reserve offer operation where the specified
// resources does not exist in the given offer (too large, in this
// case) is dropped.
TYPED_TEST(ReservationTest, DropReserveTooLarge)
{
  EXPECT_CALL(this->allocator, initialize(_, _, _));

  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(&this->allocator, masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:1;mem:512;disk:0;ports:[]";

  EXPECT_CALL(this->allocator, addSlave(_, _, _, _));

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(this->allocator, addFramework(_, _, _));

  EXPECT_CALL(sched, registered(&driver, _, _));

  // In the first offer, expect all of the slave's resources.
  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.start();

  Resources unreserved = Resources::parse("cpus:1;mem:512").get();
  Resources unreservedTooLarge = Resources::parse("cpus:1;mem:1024").get();
  Resources reservedTooLarge = unreservedTooLarge.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  Offer offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  // We expect that the reserve offer operation will be dropped.
  EXPECT_CALL(this->allocator, updateAllocation(_, _, _)).Times(0);

  // In the next offer, expect an offer with the unreserved resources.
  EXPECT_CALL(sched, resourceOffers(_, _))
    .WillOnce(FutureArg<1>(&offers));

  driver.acceptOffers({offer.id()}, {RESERVE(reservedTooLarge)});

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1);

  offer = offers.get()[0];
  EXPECT_EQ(offer.resources(), unreserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}

}  // namespace tests {
}  // namespace internal {
}  // namespace mesos {
