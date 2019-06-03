#!/usr/bin/env python
import sys
import csv
from recordclass import recordclass
from enum import Enum

class EventType(Enum):
   SUBMIT = 0
   SCHEDULE = 1
   EVICT = 2
   FAIL = 3
   FINISH = 4
   KILL = 5
   LOST = 6
   UPDATE_PENDING = 7
   UPDATE_RUNNING = 8

TaskStats = recordclass("TaskStats", "jobid taskid t_submit t_schedule t_finish")


def get_tasks_stats(task_events_file):
   '''
   1. timestamp
   2. missing info
   3. job ID
   4. task index - within the job
   5. machine ID
   6. event type
   7. user name
   8. scheduling class
   9. priority
   10. resource request for CPU cores
   11. resource request for RAM
   12. resource request for local disk space
   13. different-machine constraint
   '''
   f = open(task_events_file, 'rb')
   reader = csv.reader(f)
   retval=[]

   tasks = dict()
   for row in reader:
      sentinel_val = '-' #To match the 1-indexing of the format
      row = [sentinel_val] + row
      timestamp = int(row[1])
      job_id = str(row[3])
      task_id = int(row[4])
      eventtype = EventType(int(row[6]))
      if(eventtype == 0):
         print eventtype, EventType.SUBMIT
      if(eventtype == EventType.SUBMIT):
         if((job_id, task_id) in tasks):
            t = tasks[(job_id, task_id)]
            #print t
            #print job_id, task_id, timestamp, 0, 0
            if t.jobid == job_id and t.taskid == task_id and t.t_submit==timestamp:
               continue
         assert((job_id, task_id) not in tasks)
         tasks[(job_id, task_id)] = TaskStats(jobid=job_id, taskid=task_id, t_submit=timestamp, t_schedule=0, t_finish=0)

      elif(eventtype == EventType.SCHEDULE):
         assert((job_id, task_id) in tasks)
         taskstats = tasks[(job_id, task_id)]
         if(taskstats.t_schedule==0): #else it must have been evicted earlier
            taskstats.t_schedule = timestamp
            tasks[(job_id, task_id)] = taskstats

      elif(eventtype == EventType.FINISH):
         assert((job_id, task_id) in tasks)
         taskstats = tasks[(job_id, task_id)]
         assert(taskstats.t_finish == 0)
         taskstats.t_finish = timestamp
         tasks[(job_id, task_id)] = taskstats

      #eventtype in {EventType.EVICT, EventType.FAIL, EventType.KILL, EventType.LOST, EventType.UPDATE_RUNNING, EventType.UPDATE_PENDING}):
      #else:
      #   assert((job_id, task_id) in tasks)
   return tasks


JobStats = recordclass("JobStats", "jobid t_submit t_finish completed")


def get_jobs_stats(task_events_file):
   tasks = get_tasks_stats(task_events_file)
   jobs = dict()
   for k in tasks:
      taskstats = tasks[k]
      assert(k[0] == taskstats.jobid)
      jobid = taskstats.jobid

      if (jobid not in jobs):
         jobs[jobid] = JobStats(jobid=jobid, t_submit=0, t_finish=0, completed=True)

      jobstats = jobs[jobid]
      if jobstats.t_submit == 0:
         jobstats.t_submit = taskstats.t_submit
      else:
         jobstats.t_submit = min(jobstats.t_submit, taskstats.t_submit)

      jobstats.t_finish = max(jobstats.t_finish, taskstats.t_finish)

      if(taskstats.t_schedule == 0 or taskstats.t_finish == 0):
         jobstats.completed = False

      jobs[jobid] = jobstats
   return jobs
         

task_events_file = sys.argv[1]
jobs = get_jobs_stats(task_events_file)
for jobid in jobs:
   print jobid, jobs[jobid].t_submit, jobs[jobid].t_finish, 0




   
