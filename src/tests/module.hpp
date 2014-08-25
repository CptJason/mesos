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

#ifndef __TESTS_MODULE_HPP__
#define __TESTS_MODULE_HPP__

#include <stout/try.hpp>

#include <stout/memory.hpp>

#include <modules/module.hpp>

class TestModule : public Module {
public:
  TestModule() : Module(Module::TEST_MODULE) {}

  virtual ~TestModule() {}

  static Try<memory::shared_ptr<TestModule> > init(DynamicLibrary& library)
  {
    return module::init<TestModule>(library, "create");
  }

  virtual int foo(char a, long b) = 0;

  virtual int bar(float a, double b) = 0;
};

#endif // __TESTS_MODULE_HPP__
