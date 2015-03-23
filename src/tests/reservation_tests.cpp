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


// This test verifies that CheckpointResourcesMessages are sent to the
// slave when a framework reserve/unreserves resources, and
// the resources in the messages correctly reflect the resources that
// need to be checkpointed on the slave.
TYPED_TEST(ReservationTest, SendingCheckpointResourcesMessage)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:4096;disk:0;ports:[]";

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  Resource::ReservationInfo reservation =
    createReservationInfo(frameworkInfo.principal());

  Resources unreserved1 = Resources::parse("cpus:8").get();
  Resources reserved1 = unreserved1.flatten(frameworkInfo.role(), reservation);

  Resources unreserved2 = Resources::parse("mem:2048").get();
  Resources reserved2 = unreserved2.flatten(frameworkInfo.role(), reservation);

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1u);

  Offer offer = offers.get()[0];

  Future<CheckpointResourcesMessage> message3 =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  Future<CheckpointResourcesMessage> message2 =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  Future<CheckpointResourcesMessage> message1 =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  driver.acceptOffers(
      { offer.id() },
      { RESERVE(reserved1), RESERVE(reserved2), UNRESERVE(reserved1) });

  // NOTE: Currently, we send one message per operation. But this is
  // an implementation detail which is subject to change.
  AWAIT_READY(message1);
  EXPECT_EQ(Resources(message1.get().resources()), reserved1);

  AWAIT_READY(message2);
  EXPECT_EQ(Resources(message2.get().resources()), reserved1 + reserved2);

  AWAIT_READY(message3);
  EXPECT_EQ(Resources(message3.get().resources()), reserved2);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test verifies that the slave checkpoints the resources for
// dynamic reservations to the disk, recovers them upon restart, and
// sends them to the master during re-registration.
TYPED_TEST(ReservationTest, ResourcesCheckpointing)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:4096;disk:0;ports:[]";

  Try<PID<Slave>> slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  Resources unreserved = Resources::parse("cpus:8;mem:2048").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1u);

  Offer offer = offers.get()[0];

  Future<CheckpointResourcesMessage> checkpointResources =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, slave.get());

  driver.acceptOffers({ offer.id() }, { RESERVE(reserved) });

  AWAIT_READY(checkpointResources);

  // Restart the slave.
  this->Stop(slave.get());

  Future<ReregisterSlaveMessage> reregisterSlave =
    FUTURE_PROTOBUF(ReregisterSlaveMessage(), _, _);

  slave = this->StartSlave(slaveFlags);
  ASSERT_SOME(slave);

  AWAIT_READY(reregisterSlave);
  EXPECT_EQ(reregisterSlave.get().checkpointed_resources(), reserved);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test verifies the case where a slave that has checkpointed
// dynamic reservations reregisters with a failed over master, and the
// dynamic reservations are later correctly offered to the framework.
TYPED_TEST(ReservationTest, MasterFailover)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  StandaloneMasterDetector detector(master.get());

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:2048";

  Try<PID<Slave>> slave = this->StartSlave(&detector, slaveFlags);
  ASSERT_SOME(slave);

  MockScheduler sched;
  TestingMesosSchedulerDriver driver(&sched, &detector, frameworkInfo);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers1;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers1))
    .WillRepeatedly(Return());

  driver.start();

  Resources unreserved = Resources::parse("cpus:8;mem:2048").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers1);
  ASSERT_EQ(offers1.get().size(), 1u);

  Offer offer1 = offers1.get()[0];

  Future<CheckpointResourcesMessage> checkpointResources =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, slave.get());

  driver.acceptOffers({ offer1.id() }, { RESERVE(reserved) });

  AWAIT_READY(checkpointResources);

  // This is to make sure CheckpointResourcesMessage is processed.
  process::Clock::pause();
  process::Clock::settle();
  process::Clock::resume();

  // Simulate failed over master by restarting the master.
  this->Stop(master.get());

  EXPECT_CALL(sched, disconnected(&driver));

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<SlaveReregisteredMessage> slaveReregistered =
    FUTURE_PROTOBUF(SlaveReregisteredMessage(), _, _);

  Future<vector<Offer>> offers2;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers2))
    .WillRepeatedly(Return());

  master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  // Simulate a new master detected event on the slave so that the
  // slave will do a re-registration.
  detector.appoint(master.get());

  AWAIT_READY(slaveReregistered);

  AWAIT_READY(offers2);
  ASSERT_EQ(offers2.get().size(), 1u);

  Offer offer2 = offers2.get()[0];

  EXPECT_TRUE(Resources(offer2.resources()).contains(reserved));

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test verifies that a slave can restart as long as the
// checkpointed resources it recovers are compatible with the
// slave resources specified using the '--resources' flag.
TYPED_TEST(ReservationTest, CompatibleCheckpointedResources)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:4096;disk:0;ports:[]";

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  StandaloneMasterDetector detector(master.get());

  MockSlave slave1(slaveFlags, &detector, &containerizer);
  spawn(slave1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  Resources unreserved = Resources::parse("cpus:8;mem:2048").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1u);

  Offer offer = offers.get()[0];

  Future<CheckpointResourcesMessage> checkpointResources =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  driver.acceptOffers({ offer.id() }, { RESERVE(reserved) });

  AWAIT_READY(checkpointResources);

  terminate(slave1);
  wait(slave1);

  // Simulate a reboot of the slave machine by modify the boot ID.
  ASSERT_SOME(os::write(slave::paths::getBootIdPath(
      slave::paths::getMetaRootDir(slaveFlags.work_dir)),
      "rebooted! ;)"));

  // Change the slave resources so that it is compatible with the
  // checkpointed resources.
  slaveFlags.resources = "cpus:12;mem:2048";

  MockSlave slave2(slaveFlags, &detector, &containerizer);

  Future<Future<Nothing>> recover;
  EXPECT_CALL(slave2, __recover(_))
    .WillOnce(DoAll(FutureArg<0>(&recover), Return()));

  spawn(slave2);

  AWAIT_READY(recover);
  AWAIT_READY(recover.get());

  terminate(slave2);
  wait(slave2);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test verifies that a slave can restart as long as the
// checkpointed resources it recovers are compatible with the
// slave resources specified using the '--resources' flag.
TYPED_TEST(ReservationTest,
           CompatibleCheckpointedResourcesWithPersistentVolumes)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:4096;disk:1024;ports:[]";

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  StandaloneMasterDetector detector(master.get());

  MockSlave slave1(slaveFlags, &detector, &containerizer);
  spawn(slave1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  Resource::ReservationInfo reservation =
    createReservationInfo(frameworkInfo.principal());

  Resources unreserved = Resources::parse("cpus:8;mem:2048").get();
  Resources reserved = unreserved.flatten(frameworkInfo.role(), reservation);

  Resource unreserved_disk = Resources::parse("disk", "1024", "*").get();

  Resource reserved_disk = unreserved_disk;
  reserved_disk.set_role(frameworkInfo.role());
  reserved_disk.mutable_reservation()->CopyFrom(
      createReservationInfo(frameworkInfo.principal()));

  Resource volume = reserved_disk;
  volume.mutable_disk()->CopyFrom(
      createDiskInfo("persistence_id", "container_path"));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1u);

  Offer offer = offers.get()[0];

  Future<CheckpointResourcesMessage> message2 =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  Future<CheckpointResourcesMessage> message1 =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  driver.acceptOffers(
      { offer.id() },
      { RESERVE(reserved + reserved_disk), CREATE(volume) });

  // NOTE: Currently, we send one message per operation. But this is
  // an implementation detail which is subject to change.
  AWAIT_READY(message1);
  EXPECT_EQ(Resources(message1.get().resources()), reserved + reserved_disk);

  AWAIT_READY(message2);
  EXPECT_EQ(Resources(message2.get().resources()), reserved + volume);

  terminate(slave1);
  wait(slave1);

  // Simulate a reboot of the slave machine by modify the boot ID.
  ASSERT_SOME(os::write(slave::paths::getBootIdPath(
      slave::paths::getMetaRootDir(slaveFlags.work_dir)),
      "rebooted! ;)"));

  // Change the slave resources so that it is not compatible with the
  // checkpointed resources.
  slaveFlags.resources = "cpus:12;mem:2048;disk:1024";

  MockSlave slave2(slaveFlags, &detector, &containerizer);

  Future<Future<Nothing>> recover;
  EXPECT_CALL(slave2, __recover(_))
    .WillOnce(DoAll(FutureArg<0>(&recover), Return()));

  spawn(slave2);

  AWAIT_READY(recover);
  AWAIT_READY(recover.get());

  terminate(slave2);
  wait(slave2);

  driver.stop();
  driver.join();

  this->Shutdown();
}


// This test verifies that a slave will refuse to start if the
// checkpointed resources it recovers are not compatible with the
// slave resources specified using the '--resources' flag.
TYPED_TEST(ReservationTest, IncompatibleCheckpointedResources)
{
  FrameworkInfo frameworkInfo = DEFAULT_FRAMEWORK_INFO;
  frameworkInfo.set_role("role");

  master::Flags masterFlags = this->CreateMasterFlags();
  masterFlags.roles = frameworkInfo.role();

  Try<PID<Master>> master = this->StartMaster(masterFlags);
  ASSERT_SOME(master);

  slave::Flags slaveFlags = this->CreateSlaveFlags();
  slaveFlags.resources = "cpus:8;mem:4096;disk:0;ports:[]";

  MockExecutor exec(DEFAULT_EXECUTOR_ID);
  TestContainerizer containerizer(&exec);
  StandaloneMasterDetector detector(master.get());

  MockSlave slave1(slaveFlags, &detector, &containerizer);
  spawn(slave1);

  MockScheduler sched;
  MesosSchedulerDriver driver(
      &sched, frameworkInfo, master.get(), DEFAULT_CREDENTIAL);

  EXPECT_CALL(sched, registered(&driver, _, _));

  Future<vector<Offer>> offers;
  EXPECT_CALL(sched, resourceOffers(&driver, _))
    .WillOnce(FutureArg<1>(&offers))
    .WillRepeatedly(Return());

  driver.start();

  Resources unreserved = Resources::parse("cpus:8;mem:2048").get();
  Resources reserved = unreserved.flatten(
      frameworkInfo.role(), createReservationInfo(frameworkInfo.principal()));

  AWAIT_READY(offers);
  ASSERT_EQ(offers.get().size(), 1u);

  Offer offer = offers.get()[0];

  Future<CheckpointResourcesMessage> checkpointResources =
    FUTURE_PROTOBUF(CheckpointResourcesMessage(), _, _);

  driver.acceptOffers({ offer.id() }, { RESERVE(reserved) });

  AWAIT_READY(checkpointResources);

  terminate(slave1);
  wait(slave1);

  // Simulate a reboot of the slave machine by modify the boot ID.
  ASSERT_SOME(os::write(slave::paths::getBootIdPath(
      slave::paths::getMetaRootDir(slaveFlags.work_dir)),
      "rebooted! ;)"));

  // Change the slave resources so that it's not compatible with the
  // checkpointed resources.
  slaveFlags.resources = "cpus:4;mem:2048";

  MockSlave slave2(slaveFlags, &detector, &containerizer);

  Future<Future<Nothing>> recover;
  EXPECT_CALL(slave2, __recover(_))
    .WillOnce(DoAll(FutureArg<0>(&recover), Return()));

  spawn(slave2);

  AWAIT_READY(recover);
  AWAIT_FAILED(recover.get());

  terminate(slave2);
  wait(slave2);

  driver.stop();
  driver.join();

  this->Shutdown();
}

}  // namespace tests {
}  // namespace internal {
}  // namespace mesos {
