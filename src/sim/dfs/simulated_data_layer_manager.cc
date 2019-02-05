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

#include "sim/dfs/simulated_data_layer_manager.h"

#include "base/units.h"
#include "misc/map-util.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_bounded_dfs.h"
#include "sim/dfs/simulated_hdfs.h"
#include "sim/dfs/simulated_skewed_dfs.h"
#include "sim/dfs/simulated_uniform_dfs.h"
#include "sim/google_runtime_distribution.h"

// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
              "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
              "Runtime power law distribution: power parameter.");
// Distributed filesystem options
DEFINE_uint64(simulated_block_size, 536870912,
              "The size of a DFS block in bytes");
DEFINE_uint64(simulated_dfs_blocks_per_machine, 12288,
              "Number of blocks each machine stores. "
              "Defaults to 12288, i.e. 6 TB for 512MB blocks.");
DEFINE_uint64(simulated_dfs_replication_factor, 4,
              "The number of times each block should be replicated.");
DEFINE_string(simulated_dfs_type, "bounded", "The type of DFS to simulated. "
              "Options: uniform | bounded | hdfs | skewed");

DEFINE_uint64(simulated_remote_transfer_time, 250,
              "Time in microseconds to transfer 1 Mb data"
              "to a remote rack");

//100 ~ 10 Gbps
DEFINE_uint64(simulated_rack_transfer_time, 100,
              "Time in microseconds to transfer 1 Mb data"
              "to another machine in the same rack");



namespace firmament {
namespace sim {

SimulatedDataLayerManager::SimulatedDataLayerManager(
    TraceGenerator* trace_generator) {
  LOG(INFO) << "Initializing SimulatedDataLayerManager";
  input_block_dist_ = new GoogleBlockDistribution();
  runtime_dist_ =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);
  if (!FLAGS_simulated_dfs_type.compare("uniform")) {
    dfs_ = new SimulatedUniformDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("bounded")) {
    dfs_ = new SimulatedBoundedDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("hdfs")) {
    dfs_ = new SimulatedHDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("skewed")) {
    dfs_ = new SimulatedSkewedDFS(trace_generator);
  } else {
    LOG(FATAL) << "Unexpected simulated DFS type: " << FLAGS_simulated_dfs_type;
  }
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
  delete input_block_dist_;
  delete runtime_dist_;
  delete dfs_;
}

EquivClass_t SimulatedDataLayerManager::AddMachine(
    const string& hostname,
    ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, machine_res_id));
  return dfs_->AddMachine(machine_res_id);
}

void SimulatedDataLayerManager::GetFileLocations(
    const string& file_path, list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  dfs_->GetFileLocations(file_path, locations);
}

int64_t SimulatedDataLayerManager::GetFileSize(const string& file_path) {
  // TODO(ionel): Implement!
  return 0;
}

bool SimulatedDataLayerManager::RemoveMachine(const string& hostname) {
  ResourceID_t* machine_res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(machine_res_id);
  ResourceID_t res_id_tmp = *machine_res_id;
  hostname_to_res_id_.erase(hostname);
  return dfs_->RemoveMachine(res_id_tmp);
}

uint64_t SimulatedDataLayerManager::AddFilesForTask(
    const TaskDescriptor& td,
    uint64_t avg_runtime,
    bool long_running_service,
    uint64_t max_machine_spread) {
  if (!long_running_service) {
    double cumulative_probability =
      runtime_dist_->ProportionShorterTasks(avg_runtime);
    uint64_t input_size = input_block_dist_->Inverse(cumulative_probability);
    uint64_t num_blocks = input_size / FLAGS_simulated_block_size;
    // Need to increase if there was a remainder, since integer division
    // truncates.
    if (input_size % FLAGS_simulated_block_size != 0) {
      num_blocks++;
    }
    dfs_->AddBlocksForTask(td, num_blocks, max_machine_spread);
    return num_blocks * FLAGS_simulated_block_size;
  } else {
    return 0;
  }
}

void SimulatedDataLayerManager::RemoveFilesForTask(const TaskDescriptor& td) {
  dfs_->RemoveBlocksForTask(td.uid());
}


void SimulatedDataLayerManager::GetClosestReplicas(string &file_location, 
					   ResourceID_t machine_res_id,	
						unordered_map<uint64_t, DataLocation>* closest_block_replicas){
	 CHECK_NOTNULL(closest_block_replicas);
	 EquivClass_t rack_ec = GetRackForMachine(machine_res_id);
	 list<DataLocation> locations;
    GetFileLocations(file_location, &locations);
    for (auto& location : locations) {
      InsertIfNotPresent(closest_block_replicas,
                               location.block_id_, location);
      DataLocation closest_loc =
                closest_block_replicas->find(location.block_id_)->second;
      if(machine_res_id != closest_loc.machine_res_id_){
         if (machine_res_id == location.machine_res_id_) {
             InsertOrUpdate(closest_block_replicas,
                                  location.block_id_, location);
         }
         else if(rack_ec == location.rack_id_ &&
                 rack_ec != closest_loc.rack_id_){
             InsertOrUpdate(closest_block_replicas,
                                  location.block_id_, location);
         }
      }
    }
}


uint64_t SimulatedDataLayerManager::ComputeDataStatsForMachine(
    TaskDescriptor* td_ptr, ResourceID_t machine_res_id,
    uint64_t* data_on_rack, uint64_t* data_on_machine) {
  EquivClass_t rack_ec = GetRackForMachine(machine_res_id);
  uint64_t input_size = 0;
  for (RepeatedPtrField<ReferenceDescriptor>::pointer_iterator
         dependency_it = td_ptr->mutable_dependencies()->pointer_begin();
       dependency_it != td_ptr->mutable_dependencies()->pointer_end();
       ++dependency_it) {
    auto& dependency = *dependency_it;
    string location = dependency->location();
    /*
    if (dependency->size() == 0) {
      dependency->set_size(data_layer_manager_->GetFileSize(location));
    }
    */
    input_size += dependency->size();
    /* Find block replicas that are closest to the machine */
    unordered_map<uint64_t, DataLocation> closest_block_replicas;
	 GetClosestReplicas(location, machine_res_id, &closest_block_replicas);
    uint64_t file_size = 0;
    for (auto& it : closest_block_replicas) {
      DataLocation location = it.second;
      if (machine_res_id == location.machine_res_id_) {
        *data_on_machine = *data_on_machine + location.size_bytes_;
      }
      if (rack_ec == location.rack_id_){
        *data_on_rack = *data_on_rack + location.size_bytes_;
      }
      file_size += location.size_bytes_;
    }
    //LOG(INFO)<<"dependency->size(): "<<dependency->size()<<", file_size: "<<file_size;
    CHECK_EQ(dependency->size(), file_size);
  }
  return input_size;
}


uint64_t SimulatedDataLayerManager::GetEstimatedTransferTimeUS(
    TaskDescriptor* td_ptr,
    ResourceID_t machine_res_id) {
 //LOG(INFO) << "SimulatedDataLayerManager::GetTransferTimeUS";
 uint64_t data_on_rack = 0;
 uint64_t data_on_machine = 0;
 uint64_t input_size =
 ComputeDataStatsForMachine(td_ptr, machine_res_id, &data_on_rack,
                              &data_on_machine);
 CHECK_GE(input_size, data_on_rack);

 uint64_t remote_data = input_size - data_on_rack;
 uint64_t rack_data = data_on_rack - data_on_machine;
 uint64_t remote_transfer_time =
               (FLAGS_simulated_remote_transfer_time
             * remote_data)/BYTES_TO_MBITS;
 uint64_t rack_transfer_time =
               (FLAGS_simulated_rack_transfer_time
             * rack_data)/BYTES_TO_MBITS;
 return remote_transfer_time + rack_transfer_time;
}


} // namespace sim
} // namespace firmament
