/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include "sim/interference/no_interference_transfer_including.h"

#include "base/common.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "sim/simulator_utils.h"

DECLARE_uint64(runtime);
DECLARE_double(trace_speed_up);

namespace firmament {
namespace sim {

NoInterferenceTransfer::NoInterferenceTransfer(
    unordered_map<TaskID_t, uint64_t>* task_runtime,
    shared_ptr<ResourceMap_t> resource_map,
	 SimulatedDataLayerManager* data_layer_manager)
  : task_runtime_(task_runtime),
	 resource_map_(resource_map),
	 data_layer_manager_(data_layer_manager){
}

NoInterferenceTransfer::~NoInterferenceTransfer() {
  // The object doesn't own task_runtime_.
}

void NoInterferenceTransfer::OnTaskCompletion(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
}

void NoInterferenceTransfer::OnTaskEviction(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  //LOG(INFO) << "NoInterferenceTransfer::OnTaskEviction";
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  task_end_runtimes.set_previous_end_time(td_ptr->finish_time());
  uint64_t transfer_time = GetTransferTimeUS(td_ptr, res_id);
  // Useful work that the task did before eviction
  uint64_t task_executed_for = 0;
  if(current_time_us - td_ptr->start_time() >= transfer_time){
      task_executed_for = std::max(static_cast<uint64_t>(0),
                                 (current_time_us - 
                                 td_ptr->start_time()) - transfer_time);
  }
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until eviction is
    // saved. Hence, we update the time the task has left to run.
    InsertOrUpdate(task_runtime_, task_id, *runtime_ptr - task_executed_for);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->clear_start_time();
  td_ptr->set_submit_time(current_time_us);
  tasks_end_time->push_back(task_end_runtimes);
}

void NoInterferenceTransfer::OnTaskMigration(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t old_res_id,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  //LOG(INFO) << "NoInterferenceTransfer::OnTaskMigration";
  TaskID_t task_id = td_ptr->uid();
  uint64_t old_transfer_time = GetTransferTimeUS(td_ptr, old_res_id);
  uint64_t new_transfer_time = GetTransferTimeUS(td_ptr, res_id);

  // Useful work that the task did before migration
  uint64_t task_executed_for = 0;
  if(current_time_us - td_ptr->start_time() >= old_transfer_time){
      task_executed_for = std::max(static_cast<uint64_t>(0ULL), 
                            (current_time_us - td_ptr->start_time()) -
                            old_transfer_time);
  }
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until migration is
    // saved. Hence, we update the time the task has left to run.
    InsertOrUpdate(task_runtime_, task_id, *runtime_ptr - task_executed_for);
    // Update the finish time since the transfer will have to be done again
    td_ptr->set_finish_time(current_time_us + new_transfer_time + 
                            *runtime_ptr - task_executed_for);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // the timestamp just after the end of the simulation.
    td_ptr->set_finish_time(FLAGS_runtime / FLAGS_trace_speed_up + 1);
  }
  td_ptr->set_submit_time(current_time_us);
  td_ptr->set_start_time(current_time_us);
}



uint64_t NoInterferenceTransfer::GetTransferTimeUS(
    TaskDescriptor* td_ptr,
    ResourceID_t res_id) {
 //LOG(INFO) << "NoInterferenceTransfer::GetTransferTimeUS";
 ResourceID_t machine_res_id =
	MachineResIDForResource(resource_map_, res_id);
 return data_layer_manager_->GetEstimatedTransferTimeUS(td_ptr,
                             machine_res_id);
}



void NoInterferenceTransfer::OnTaskPlacement(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  //LOG(INFO) << "NoInterferenceTransfer::OnTaskPlacement";
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  td_ptr->set_start_time(current_time_us);
  td_ptr->set_total_unscheduled_time(UpdateTaskTotalUnscheduledTime(*td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, td_ptr->uid());
  uint64_t transfer_time = GetTransferTimeUS(td_ptr, res_id);
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    td_ptr->set_finish_time(current_time_us + transfer_time + *runtime_ptr);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // the timestamp just after the end of the simulation.
    td_ptr->set_finish_time(FLAGS_runtime / FLAGS_trace_speed_up + 1);
  }
  task_end_runtimes.set_current_end_time(td_ptr->finish_time());
  tasks_end_time->push_back(task_end_runtimes);
}

}  // namespace sim
}  // namespace firmament
