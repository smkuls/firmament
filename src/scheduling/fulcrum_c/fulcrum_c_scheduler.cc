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

// Naive implementation of a centralized fulcrum scheduler.
#include "scheduling/fulcrum_c/fulcrum_c_scheduler.h"

#include <boost/timer/timer.hpp>

#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "storage/object_store_interface.h"

DEFINE_bool(fulcrum_randomly_place_tasks, false, "Place tasks randomly");

namespace firmament {
namespace scheduler {

using store::ObjectStoreInterface;

FulcrumScheduler::FulcrumScheduler(
    shared_ptr<JobMap_t> job_map,
    shared_ptr<ResourceMap_t> resource_map,
    ResourceTopologyNodeDescriptor* resource_topology,
    shared_ptr<ObjectStoreInterface> object_store,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    shared_ptr<TopologyManager> topo_mgr,
    MessagingAdapterInterface<BaseMessage>* m_adapter,
    SchedulingEventNotifierInterface* event_notifier,
    ResourceID_t coordinator_res_id,
    const string& coordinator_uri,
    TimeInterface* time_manager,
    TraceGenerator* trace_generator)
    : EventDrivenScheduler(job_map, resource_map, resource_topology,
                           object_store, task_map, knowledge_base, topo_mgr,
                           m_adapter, event_notifier, coordinator_res_id,
                           coordinator_uri, time_manager, trace_generator) {
  data_layer_manager_ = knowledge_base_->mutable_data_layer_manager();
  machine_res_id_pus_ = knowledge_base_->mutable_machine_res_id_pus();
  VLOG(1) << "FulcrumScheduler initiated.";
}

FulcrumScheduler::~FulcrumScheduler() {
}

bool FulcrumScheduler::FindResourceForTask(TaskDescriptor& task_desc,
                                          ResourceID_t* best_resource) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc.uid() << "...";
 
  ResourceVector rvec = task_desc.resource_request();
  /*
  LOG (INFO) << "Task Requirements- "
             << ", CPU: " << rvec.cpu_cores()
             << ", ram_bw: " << rvec.ram_bw()
             << ", ram_cap: " << rvec.ram_cap()
             << ", disk_bw: " << rvec.disk_bw()
             << ", disk_cap: " << rvec.disk_cap()
             << ", net_tx_bw: " << rvec.net_tx_bw()
             << ", net_rx_bw: " << rvec.net_rx_bw();
  */

  unordered_map<EquivClass_t, uint64_t> data_on_ecs;
  unordered_map<ResourceID_t, uint64_t,
                boost::hash<ResourceID_t>> data_on_machines;
  // Compute the amount of data the task has on every machine and rack.
  uint64_t input_size = ComputeClusterDataStatistics(
										task_desc, &data_on_machines,
									   &data_on_ecs);

  //LOG(INFO) << "num_machines: " << data_on_machines.size();
  vector<ResourceID_t> data_machines;
  for (auto kv: data_on_machines){
  		data_machines.push_back(kv.first);
  }
  sort(data_machines.begin( ), data_machines.end( ),
  			[&data_on_machines]( const ResourceID_t& lhs, const ResourceID_t& rhs )
			  {  //Sort machines in the descending order of data
				  return data_on_machines[lhs] > data_on_machines[rhs];
			  });


  // Find the first idle resource in the resource map
  for (auto res_id = data_machines.begin();
       res_id != data_machines.end();
       ++res_id) {
    ResourceStatus* res_status =  FindPtrOrNull(*resource_map_, *res_id);
	 CHECK_NOTNULL(res_status);
    CHECK_EQ(res_status->descriptor().type(),
                  ResourceDescriptor::RESOURCE_MACHINE);
	 // Get machine's PUs
    ResourceID_t machine_res_id = MachineResIDForResource(resource_map_,
                           ResourceIDFromString(res_status->descriptor().uuid()));
	 pair<multimap<ResourceID_t, ResourceDescriptor*>::iterator,
		 multimap<ResourceID_t, ResourceDescriptor*>::iterator> range_it =
			 machine_res_id_pus_->equal_range(machine_res_id);
	 for (; range_it.first != range_it.second; range_it.first++) {
		 ResourceID_t pu_res_id =
			ResourceIDFromString(range_it.first->second->uuid());
		 ResourceStatus* pu_res_status = FindPtrOrNull(*resource_map_, pu_res_id);
		 CHECK_EQ(pu_res_status->descriptor().type(),
							ResourceDescriptor::RESOURCE_PU);
		 VLOG(3) << "Considering resource " << pu_res_id
					<< ", which is in state "
					<< pu_res_status->descriptor().state();
       /*
		 LOG(INFO) << "Considering resource with data " << pu_res_id
					<< ", which is in state "
					<< pu_res_status->descriptor().state()
					<<", of type "
					<< pu_res_status->descriptor().type();
       */
		 if (pu_res_status->descriptor().state() ==
			  ResourceDescriptor::RESOURCE_IDLE) {
			*best_resource = pu_res_id;
			//LOG(INFO)<< "Scheduling on machine with data. ";
			return true;
		 }
  	}
  }

  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    VLOG(3) << "Considering resource " << res_iter->first
            << ", which is in state "
            << res_iter->second->descriptor().state();
    /*
    LOG(INFO) << "Considering resource " << res_iter->first
            << ", which is in state "
            << res_iter->second->descriptor().state()
            <<", of type "
            << res_iter->second->descriptor().type();
    */
    if (res_iter->second->descriptor().state() ==
        ResourceDescriptor::RESOURCE_IDLE) {
      *best_resource = res_iter->first;
      return true;
    }
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return false;
}

bool FulcrumScheduler::FindRandomResourceForTask(const TaskDescriptor& task_desc,
                                                ResourceID_t* best_resource) {
  // TODO(malte): This is an extremely simple-minded approach to resource
  // selection (i.e. the essence of scheduling). We will simply traverse the
  // resource map in some order, and grab the first resource available.
  VLOG(2) << "Trying to place task " << task_desc.uid() << "...";
  LOG(INFO) << "FindRandomResourceForTask: #resources: " << resource_map_->size();
  vector<ResourceStatus*> resources;
  // Find the first idle resource in the resource map
  for (ResourceMap_t::iterator res_iter = resource_map_->begin();
       res_iter != resource_map_->end();
       ++res_iter) {
    resources.push_back(res_iter->second);
  }
  for (uint64_t max_attempts = 2000; max_attempts > 0; max_attempts--) {
    uint32_t resource_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % resources.size();
    if (resources[resource_index]->descriptor().state() ==
        ResourceDescriptor::RESOURCE_IDLE) {
      *best_resource = ResourceIDFromString(
          resources[resource_index]->descriptor().uuid());
      return true;
    }
  }
  // We have not found any idle resources in our local resource map. At this
  // point, we should start looking beyond the machine boundary and towards
  // remote resources.
  return false;
}

void FulcrumScheduler::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                           TaskFinalReport* report) {

  //LOG(INFO) << "Handle Task Completion";
  ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceDescriptor* rd_ptr = rs->mutable_descriptor();
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskCompletion(td_ptr, report);

  if(td_ptr->trace_job_id()%100 == 0 && td_ptr->uid()%100==3){
     LOG(INFO) << "Job Id: " << td_ptr->job_id()
        << "Task completed " << td_ptr->uid()
        << " StartTime: " << td_ptr->start_time()
        << " SubmitTime: " << td_ptr->submit_time()
        << " FinishTime: " << td_ptr->finish_time();
  }
}

void FulcrumScheduler::HandleTaskEviction(TaskDescriptor* td_ptr,
                                         ResourceDescriptor* rd_ptr) {
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskEviction(td_ptr, rd_ptr);
}

void FulcrumScheduler::HandleTaskFailure(TaskDescriptor* td_ptr) {
  ResourceID_t res_id = ResourceIDFromString(td_ptr->scheduled_to_resource());
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceDescriptor* rd_ptr = rs->mutable_descriptor();
  // TODO(ionel): This assumes no PU sharing.
  rd_ptr->clear_current_running_tasks();
  EventDrivenScheduler::HandleTaskFailure(td_ptr);
}

void FulcrumScheduler::KillRunningTask(
    TaskID_t task_id,
    TaskKillMessage::TaskKillReason reason) {
  // TODO(ionel): Make sure the task is removed from current_running_tasks
  // when it is killed.
}

void FulcrumScheduler::HandleTaskFinalReport(const TaskFinalReport& report,
                                            TaskDescriptor* td_ptr) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  EventDrivenScheduler::HandleTaskFinalReport(report, td_ptr);
  TaskID_t task_id = td_ptr->uid();
  vector<EquivClass_t> equiv_classes;
  // We create two equivalence class IDs:
  // 1) an equivalence class ID per task_id
  // 2) an equivalence class ID per program
  // We create these equivalence class IDs in order to make the EC
  // statistics view on the web UI work.
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashCommandLine(*td_ptr));
  equiv_classes.push_back(task_agg);
  equiv_classes.push_back(task_id);
  knowledge_base_->ProcessTaskFinalReport(equiv_classes, report);
}

void FulcrumScheduler::PopulateSchedulerResourceUI(
    ResourceID_t res_id,
    TemplateDictionary* dict) const {
  // At the moment to we do not show any resource-specific information
  // for the simple scheduler.
}

void FulcrumScheduler::PopulateSchedulerTaskUI(TaskID_t task_id,
                                              TemplateDictionary* dict) const {
  // At the moment to we do not show any task-specific information
  // for the simple scheduler.
}

uint64_t FulcrumScheduler::ScheduleAllJobs(SchedulerStats* scheduler_stats) {
  return ScheduleAllJobs(scheduler_stats, NULL);
}

uint64_t FulcrumScheduler::ScheduleAllJobs(SchedulerStats* scheduler_stats,
                                          vector<SchedulingDelta>* deltas) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  vector<JobDescriptor*> jobs;
  for (auto& job_id_jd : jobs_to_schedule_) {
    jobs.push_back(job_id_jd.second);
  }
  uint64_t num_scheduled_tasks = ScheduleJobs(jobs, scheduler_stats, deltas);
  return num_scheduled_tasks;
}

uint64_t FulcrumScheduler::ScheduleJob(JobDescriptor* jd_ptr,
                                      SchedulerStats* scheduler_stats) {
  uint64_t num_scheduled_tasks = 0;
  VLOG(2) << "Preparing to schedule job " << jd_ptr->uuid();
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  //LOG(INFO) << "START SCHEDULING " << jd_ptr->uuid();
  boost::timer::cpu_timer scheduler_timer;
  // Get the set of runnable tasks for this job
  unordered_set<TaskID_t> runnable_tasks = ComputeRunnableTasksForJob(jd_ptr);

  /*
  if (runnable_tasks.size() > 0) {
    LOG(INFO) << "Scheduling job " << jd_ptr->uuid() << ", which has "
           << runnable_tasks.size() << " runnable tasks.";
  }
  */
  JobID_t job_id = JobIDFromString(jd_ptr->uuid());
  for (unordered_set<TaskID_t>::const_iterator task_iter =
       runnable_tasks.begin();
       task_iter != runnable_tasks.end();
       ++task_iter) {
    TaskDescriptor* td = FindPtrOrNull(*task_map_, *task_iter);
    CHECK(td);
    trace_generator_->TaskSubmitted(td);
    // LOG(INFO) << "Considering task " << td->uid() << " StartTime: " << td->start_time()
    //    << " SubmitTime: " << td->submit_time() << " FinishTime: " << td->finish_time();

    ResourceID_t best_resource;
    bool success = false;
    if (FLAGS_fulcrum_randomly_place_tasks) {
      success = FindRandomResourceForTask(*td, &best_resource);
    } else {
      success = FindResourceForTask(*td, &best_resource);
    }
    if (!success) {
      VLOG(2) << "No suitable resource found, will need to try again.";
    } else {
      ResourceStatus* rp = FindPtrOrNull(*resource_map_, best_resource);
      CHECK(rp);
      VLOG(1) << "Scheduling task " << td->uid() << " on resource "
              << rp->descriptor().uuid();
      // Remove the task from the runnable set.
      runnable_tasks_[job_id].erase(td->uid());
      HandleTaskPlacement(td, rp->mutable_descriptor());
      num_scheduled_tasks++;
    }
  }
  if (num_scheduled_tasks > 0)
    jd_ptr->set_state(JobDescriptor::RUNNING);
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime_ = scheduler_timer.elapsed().wall /
      NANOSECONDS_IN_MICROSECOND;
  }
  //LOG(INFO) << "STOP SCHEDULING " << jd_ptr->uuid() << " : " << num_scheduled_tasks << " : " << runnable_tasks.size();
  return num_scheduled_tasks;
}

uint64_t FulcrumScheduler::ScheduleJobs(const vector<JobDescriptor*>& jds_ptr,
                                       SchedulerStats* scheduler_stats,
                                       vector<SchedulingDelta>* deltas) {
  boost::lock_guard<boost::recursive_mutex> lock(scheduling_lock_);
  uint64_t num_scheduled_tasks = 0;
  boost::timer::cpu_timer scheduler_timer;
  // TODO(ionel): Populate scheduling deltas!
  for (auto& jd_ptr : jds_ptr) {
    num_scheduled_tasks += ScheduleJob(jd_ptr, scheduler_stats);
  }
  if (scheduler_stats != NULL) {
    scheduler_stats->scheduler_runtime_ =
      static_cast<uint64_t>(scheduler_timer.elapsed().wall) /
      NANOSECONDS_IN_MICROSECOND;
  }
  return num_scheduled_tasks;
}

}  // namespace scheduler
}  // namespace firmament
