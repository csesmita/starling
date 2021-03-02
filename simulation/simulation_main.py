#
# MURMURATION
#
# Copyright 2020 - Systems Research Lab, CS Department, University of Cambridge 
#
# Modified from EAGLE - Operating Systems Laboratory EPFL

import sys
import time
import math
import random
import Queue
import operator
import bitmap
import copy
import collections
import numpy
from gc import collect

from pyroaring import BitMap

class TaskDurationDistributions:
    CONSTANT, MEAN, FROM_FILE  = range(3)
class EstimationErrorDistribution:
    CONSTANT, RANDOM, MEAN  = range(3)

RATIO_SCHEDULERS_TO_CORES = 0.1

class Job(object):
    def __init__(self, task_distribution, line, estimate_distribution, off_mean_bottom, off_mean_top):
        global job_count

        job_args                    = (line.split('\n'))[0].split()
        self.start_time             = float(job_args[0])
        self.num_tasks              = int(job_args[1])
        mean_task_duration          = float(job_args[2])

        self.id = job_count
        job_count += 1
        self.completed_tasks_count = 0
        self.end_time = self.start_time
        self.unscheduled_tasks = collections.deque()
        self.actual_task_duration = collections.deque()
        self.cpu_reqs_by_tasks = collections.deque()
        #self.cpu_avg_per_task = collections.deque()
        #self.cpu_max_per_task = collections.deque()

        self.off_mean_bottom = off_mean_bottom
        self.off_mean_top = off_mean_top

        self.job_type_for_scheduling = BIG if int(mean_task_duration) > CUTOFF_THIS_EXP  else SMALL
        self.job_type_for_comparison = BIG if int(mean_task_duration) > CUTOFF_BIG_SMALL else SMALL
        

        if   task_distribution == TaskDurationDistributions.FROM_FILE: 
            self.file_task_execution_time(job_args)
        elif task_distribution == TaskDurationDistributions.CONSTANT:
            while len(self.unscheduled_tasks) < self.num_tasks:
                self.unscheduled_tasks.appendleft(int(mean_task_duration))
                self.actual_task_duration.appendleft(int(mean_task_duration))

        self.estimate_distribution = estimate_distribution

        if   estimate_distribution == EstimationErrorDistribution.MEAN:
            self.estimated_task_duration = mean_task_duration
        elif estimate_distribution == EstimationErrorDistribution.CONSTANT:
            self.estimated_task_duration = int(mean_task_duration+off_mean_top*mean_task_duration)
        elif estimate_distribution == EstimationErrorDistribution.RANDOM:
            top = off_mean_top*mean_task_duration
            bottom = off_mean_bottom*mean_task_duration
            self.estimated_task_duration = int(random.uniform(bottom,top)) 
            assert(self.estimated_task_duration <= int(top))
            assert(self.estimated_task_duration >= int(bottom))

        self.remaining_exec_time = self.estimated_task_duration*len(self.unscheduled_tasks)
        # Remember the scheduler that scheduled this job
        self.scheduled_by = None

    #Job class
    """ Returns true if the job has completed, and false otherwise. """
    def update_task_completion_details(self, completion_time):
        self.completed_tasks_count += 1
        self.end_time = max(completion_time, self.end_time)
        if self.completed_tasks_count > self.num_tasks:
            raise AssertionError('update_task_completion_details(): Completed tasks more than number of tasks!')
        return self.num_tasks == self.completed_tasks_count


    #Job class
    def file_task_execution_time(self, job_args):
        cores_needed = 0
        for i in range(self.num_tasks):
           self.unscheduled_tasks.appendleft((float(job_args[3 + i])))
           self.actual_task_duration.appendleft((float(job_args[3 + i])))
           if len(job_args) > 3 + self.num_tasks:
               # For Alibaba, normalize all cpu measurements by 100. 100 = 1 core.
               # File contains actual durations of tasks followed by
               # cpu requested per task and cpu avg and max used per task
               self.cpu_reqs_by_tasks.appendleft(int(math.ceil(float(job_args[3 + i + self.num_tasks]))))
               #self.cpu_avg_per_task.appendleft(float(job_args[3 + i + 2*self.num_tasks]))
               #self.cpu_max_per_task.appendleft(float(job_args[3 + i + 3*self.num_tasks]))
           else:
               # HACK! Instead of changing traces file, make this value multi core instead
               cores_needed += 1
               self.cpu_reqs_by_tasks.appendleft(cores_needed)
               if cores_needed == CORES_PER_MACHINE:
                   cores_needed = 0
        if len(self.unscheduled_tasks) != self.num_tasks:
            raise AssertionError('file_task_execution_time(): Number of unscheduled tasks is not the same as number of tasks')

    #Job class
    def update_remaining_time(self):
        self.remaining_exec_time -= self.estimated_task_duration
        #assert(self.remaining_exec_time >=0)
        if (len(self.unscheduled_tasks) == 0): #Improvement
            self.remaining_exec_time = -1

#####################################################################################################################
#####################################################################################################################
class Stats(object):

    STATS_STEALING_MESSAGES = 0
    STATS_SH_PROBES_QUEUED_BEHIND_BIG = 0
    STATS_TASKS_SH_EXEC_IN_BP = 0
    STATS_TOTAL_STOLEN_PROBES = 0
    STATS_TOTAL_STOLEN_B_FROM_B_PROBES = 0
    STATS_TOTAL_STOLEN_S_FROM_S_PROBES = 0
    STATS_TOTAL_STOLEN_S_FROM_B_PROBES = 0
    STATS_SUCCESSFUL_STEAL_ATTEMPTS = 0
    STATS_SHORT_TASKS_WAITED_FOR_BIG = 0
    STATS_STICKY_EXECUTIONS = 0
    STATS_STICKY_EXECUTIONS_IN_BP = 0
    STATS_REASSIGNED_PROBES=0
    STATS_TASKS_TOTAL_FINISHED = 0   
    STATS_TASKS_SHORT_FINISHED = 0   
    STATS_TASKS_LONG_FINISHED = 0   
    STATS_SHORT_UNSUCC_PROBES_IN_BIG = 0
    STATS_SH_PROBES_ASSIGNED_IN_BP = 0
    STATS_BYPASSEDBYBIG_AND_STUCK = 0
    STATS_FALLBACKS_TO_SP = 0
    STATS_RND_FIRST_ROUND_NO_LOCAL_BITMAP = 0
    STATS_ROUNDS = 0
    STATS_ROUNDS_ATTEMPTS = 0
    STATS_PERC_1ST_ROUNDS = 0

#####################################################################################################################
#####################################################################################################################
class Event(object):
    def __init__(self):
        raise NotImplementedError("Event is an abstract class and cannot be instantiated directly")

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """
        raise NotImplementedError("The run() method must be implemented by each class subclassing Event")

#####################################################################################################################
#####################################################################################################################

class JobArrival(Event, file):
    def __init__(self, task_distribution, job, jobs_file):
        self.task_distribution = task_distribution
        self.job = job
        self.jobs_file = jobs_file

    def run(self, current_time):
        global t1
        new_events = []

        long_job = self.job.job_type_for_scheduling == BIG
        worker_indices = []
        btmap = None
        if (SYSTEM_SIMULATED == "Murmuration"):
            # Short or long job, find a random scheduler node for
            # landing the job request. Note - worker queue status
            # should be updated when a decision on workers is made.
            # worker_indices for Murmuration only indicates the
            # scheduler. send_probes() does both worker selection and
            # sending probe requests.
            # TODO - Follow the pattern of rest of the schedulers and 
            # have the worker selection logic here.
            worker_indices.append(random.choice(simulation.scheduler_indices))
            if self.job.id % 100 == 0:
                print current_time, ":   Big Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration, "simulation time", time.time() - t1
                collect()
        elif not long_job:
            # Short job schedulers
            print current_time, ": Short Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration
            if SYSTEM_SIMULATED == "Hawk" or SYSTEM_SIMULATED == "Eagle":
                possible_worker_indices = simulation.small_partition_workers
            if SYSTEM_SIMULATED == "IdealEagle":
                possible_worker_indices = simulation.get_list_non_long_job_workers_from_btmap(keeper.get_btmap())

            worker_indices = simulation.find_workers_random(PROBE_RATIO, self.job.num_tasks, possible_worker_indices,MIN_NR_PROBES)
        else:
            # Long job schedulers
            #print current_time, ":   Big Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration
            btmap = keeper.get_btmap()

            if (SYSTEM_SIMULATED == "DLWL"):
                if self.job.job_type_for_comparison == SMALL:
                    possible_workers = simulation.small_partition_workers_hash
                else:
                    possible_workers = simulation.big_partition_workers_hash
                estimated_task_durations = [self.job.estimated_task_duration for i in range(len(self.job.unscheduled_tasks))]
                worker_indices = simulation.find_workers_dlwl(estimated_task_durations, simulation.shared_cluster_status, current_time, simulation, possible_workers)
                keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration, self.job.id, current_time)
            elif (simulation.SCHEDULE_BIG_CENTRALIZED):
                if SYSTEM_SIMULATED == "CLWL" and self.job.job_type_for_comparison == SMALL:
                    possible_workers = simulation.small_partition_workers_hash
                    worker_indices = simulation.find_workers_long_job_prio(self.job.num_tasks, self.job.estimated_task_duration, current_time, simulation, possible_workers)
                    keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration, self.job.id, current_time)
                else:
                    # Eagle
                    possible_workers = simulation.big_partition_workers_hash
                    worker_indices = simulation.find_workers_long_job_prio(self.job.num_tasks, self.job.estimated_task_duration, current_time, simulation, possible_workers)
                    keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration, self.job.id, current_time)

            else:
                # Long job - Sparrow
                if SYSTEM_SIMULATED == "Hawk":
                    worker_indices = simulation.find_machines_random(PROBE_RATIO, self.job.num_tasks, set(range(TOTAL_MACHINES)), MIN_NR_PROBES, self.job.cpu_reqs_by_tasks)
                else:
                    worker_indices = simulation.find_workers_random(PROBE_RATIO, self.job.num_tasks, simulation.big_partition_workers,MIN_NR_PROBES)

        new_events = simulation.send_probes(self.job, current_time, worker_indices, btmap)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if (line == ''):
            simulation.scheduled_last_job = True
            return new_events
        self.job = Job(self.task_distribution, line, self.job.estimate_distribution, self.job.off_mean_bottom, self.job.off_mean_top)
        new_events.append((self.job.start_time, self))
        simulation.jobs_scheduled += 1

        return new_events


#####################################################################################################################
#####################################################################################################################

class PeriodicTimerEvent(Event):
    def __init__(self,simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events = []

        total_load       = str(int(10000*(1-self.simulation.total_free_slots*1.0/(TOTAL_MACHINES*CORES_PER_MACHINE)))/100.0)
        small_load       = str(int(10000*(1-self.simulation.free_slots_small_partition*1.0/len(self.simulation.small_partition_workers)))/100.0)
        big_load         = str(int(10000*(1-self.simulation.free_slots_big_partition*1.0/len(self.simulation.big_partition_workers)))/100.0)
        small_not_big_load ="N/A"
        if(len(self.simulation.small_not_big_partition_workers)!=0):
            small_not_big_load        = str(int(10000*(1-self.simulation.free_slots_small_not_big_partition*1.0/len(self.simulation.small_not_big_partition_workers)))/100.0)

        #print >> load_file,"total_load: " + total_load + " small_load: " + small_load + " big_load: " + big_load + " small_not_big_load: " + small_not_big_load+" current_time: " + str(current_time) 

        if(not self.simulation.event_queue.empty()):
            new_events.append((current_time + MONITOR_INTERVAL,self))
        return new_events

#####################################################################################################################
#####################################################################################################################
#####################################################################################################################
# Class only used by DLWL
class WorkerHeartbeatEvent(Event):
    
    def __init__(self,simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events = []
        # shared_cluster_status is only used in DLWL
        if(HEARTBEAT_DELAY == 0):
            self.simulation.shared_cluster_status = keeper.get_queue_status()
        else:
            self.simulation.shared_cluster_status = copy.deepcopy(keeper.get_queue_status())

        if(HEARTBEAT_DELAY != 0):
            if(self.simulation.jobs_completed != self.simulation.jobs_scheduled or self.simulation.scheduled_last_job == False):
                new_events.append((current_time + HEARTBEAT_DELAY,self))

        return new_events

#####################################################################################################################
#####################################################################################################################
# Support for multi-core task request
class ProbeEventForMachines(Event):
    def __init__(self, machine):
        self.machine = machine

    def run(self, current_time):
        return self.machine.try_process_next_probe_in_the_queue(current_time)

#####################################################################################################################
#####################################################################################################################

class ProbeEventForWorkers(Event):
    def __init__(self, worker, job_id, task_length, job_type_for_scheduling, btmap):
        if SYSTEM_SIMULATED == "Murmuration":
            raise AssertionError("Murmuration should not invoke ProbeEventForWorkers or worker.add_probe()")
        if SYSTEM_SIMULATED == "Hawk":
            raise AssertionError("Hawk should not invoke ProbeEventForWorkers or worker.add_probe()")
        self.worker = worker
        self.job_id = job_id
        self.task_length = task_length
        self.job_type_for_scheduling = job_type_for_scheduling
        self.btmap = btmap

    def run(self, current_time):
        return self.worker.add_probe(self.job_id, self.task_length, self.job_type_for_scheduling, current_time,self.btmap, False)

#####################################################################################################################
#####################################################################################################################
# ClusterStatusKeeper: Keeps track of tasks assigned to workers and the 
# estimated start time of each task. The queue also includes currently
# executing task at the worker.
class ClusterStatusKeeper(object):
    #In Murmuration, worker is the absolute core id, unique to every core in every machine of the DC.
    def __init__(self, num_workers):
        # worker_queues is a key value pair of worker indices and queued tasks.
        # Queued tasks are array entries that each contain (arrival_time, task_duration, job_id, task_id, num_cores)
        self.worker_queues = {}
        self.worker_queues_free_time_end = {}
        self.worker_queues_free_time_start = {}
        self.btmap = bitmap.BitMap(num_workers)
        for i in range(0, num_workers):
           self.worker_queues[i] = []
           self.worker_queues_free_time_start[i] = [0]
           self.worker_queues_free_time_end[i] = [float('inf')]

    # ClusterStatusKeeper class
    # Only used by DLWL
    def get_queue_status(self):
        return self.worker_queues

    # ClusterStatusKeeper class
    # get_machine_est_wait() -
    # Returns per task estimated wait time and list of cores that can accommodate that task at that estimated time.
    # Returns - ([est_task1, est_task2, ...],[[list of cores],[list of core], [],...])
    # Might return smaller values for smaller cpu req, even if those requests come in later.
    # Also, converts holes to ints except the last entry in end which is float('inf')
    def get_machine_est_wait(self, cores, cpu_req, arrival_time, task_duration):
        num_cores = len(cores)
        if num_cores < cpu_req:
            return (float('inf'), [])

        est_time_for_tasks = []
        cores_list_for_tasks = []
        # Generate all possible holes to fit each task (D=task duration, N = num cpus needed)
        arrival_time = int(math.ceil(arrival_time))
        #Fit into smallest integral duration hole
        task_duration = int(math.ceil(task_duration))
        # Number of cores requested has to be atleast equal to the number of cores on the machine
        # Filter out machines that have less than requested number of cores
        all_slots_list = BitMap()
        all_slots_list_add = all_slots_list.add
        all_slots_list_cores = collections.defaultdict(BitMap)
        all_slots_fragmentation = collections.defaultdict(collections.defaultdict)
        inf_hole_start = {}
        for core in cores:
            core_id = core.id
            time_start = self.worker_queues_free_time_start[core_id]
            time_end = self.worker_queues_free_time_end[core_id]
            if len(time_end) != len(time_start):
                raise AssertionError('Error in get_machine_est_wait - mismatch in lengths of start and end hole arrays')
            for hole in xrange(len(time_end)):
                if time_start[hole] >= time_end[hole]:
                    print "Error in get_machine_est_wait - start of hole is equal or larger than end of hole"
                    print "Core index", core_id, "hole id ", hole, "start is ", time_start[hole], "end is ", time_end[hole]
                    raise AssertionError('Error in get_machine_est_wait - start of hole is equal or larger than end of hole')
                # Skip holes before arrival time
                if time_end[hole] < arrival_time:
                    continue
                start = time_start[hole] if arrival_time <= time_start[hole] else arrival_time
                if time_end[hole] != float('inf'):
                    # Find all possible holes at every granularity, each lasting task_duration time.
                    time_granularity = 1
                    end = time_end[hole] - task_duration + 1
                    arr = range(start, end, time_granularity)
                    for start_chunk in arr:
                        #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start_chunk
                        all_slots_list_add(start_chunk)
                        all_slots_list_cores[start_chunk].add(core_id)
                        all_slots_fragmentation[start_chunk][core_id] = max(start_chunk - time_start[hole],time_end[hole] - start_chunk - task_duration)
                else:
                    all_slots_list_add(start)
                    all_slots_list_cores[start].add(core_id)
                    all_slots_fragmentation[start][core_id] = start - time_start[hole]
                    inf_hole_start[core_id] = start

        for core, inf_start in inf_hole_start.items():
            for start in all_slots_list_cores.keys():
                if start > inf_start:
                    all_slots_list_cores[start].add(core)
                    all_slots_fragmentation[start][core] = start - inf_start

        #Assuming all_slots_list is sorted.
        for start_time in all_slots_list:
            cores_available = len(all_slots_list_cores[start_time])
            if cores_available == cpu_req:
                return (start_time, all_slots_list_cores[start_time])
            if cores_available > cpu_req:
                #Randomly sample the required number of cores.
                if POLICY == "RANDOM":
                    cores_list = random.sample(all_slots_list_cores[start_time], cpu_req)
                else:
                    cores_fragmented = all_slots_fragmentation[start_time]
                    if POLICY == "MOST_LEFTOVER":
                        #Select cores with largest available hole after allocation
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=operator.itemgetter(1), reverse=True)[0:cpu_req]
                    elif POLICY == "LEAST_LEFTOVER":
                        #Select cores with smallest available hole after allocation
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=operator.itemgetter(1), reverse=False)[0:cpu_req]
                    else:
                        raise AssertionError('Check the name of the policy')
                    cores_list = BitMap(dict(sorted_cores_fragmented).keys())
                #print "Earliest start time for task", index," (duration - ", task_duration,") needing", cpu_req,"cores is ", start_time, "with cores", cores_list
                # cpu_req is available when the fastest cpu_req number of cores is
                # available for use at or after arrival_time.
                return (start_time, cores_list)


    # ClusterStatusKeeper class
    def get_workers_queue_status(self, worker_index):
        task_index = 0
        estimated_time = 0
        while task_index < len(self.worker_queues[worker_index]):
            estimated_time += self.worker_queues[worker_index][task_index][4]
            task_index += 1
        return estimated_time


    # ClusterStatusKeeper class
    def get_btmap(self):
        return self.btmap

    # ClusterStatusKeeper class
    def update_workers_queue(self, worker_indices, increase, duration, job_id, arrival_time_at_worker):
        assert SYSTEM_SIMULATED != "Murmuration", "update_workers_queue() should not be called for Murmuration"
        assert SYSTEM_SIMULATED != "Hawk", "update_workers_queue() should not be called for Hawk"
        for worker in worker_indices:
            # tasks is of the form -  [[10, 20, 1], [20, 20, 1], ...]
            tasks = self.worker_queues[worker]
            if increase:
                # Append (arrival_time_at_worker, task_duration, job_id) to the end of tasks list
                tasks.append([arrival_time_at_worker, duration, job_id])
                #print "[", arrival_time_at_worker,"] Appending task entry", duration, job_id," to worker_queue at worker", worker
                self.btmap.set(worker)
            else:
                # Search for the first entry with this (task_duration, jobid) and pop the task
                original_task_queue_length = len(tasks)
                task_index = 0
                while task_index < original_task_queue_length:
                    if tasks[task_index][4] == duration and tasks[task_index][1] == job_id:
                        # Delete this task entry and finish
                        #print "Popping task entry", duration, job_id, " from worker_queue at worker", worker
                        tasks.pop(task_index)
                        break
                    task_index += 1
                if  task_index >= original_task_queue_length:
                    raise AssertionError(" %r %r - offending value for task_duration: %r %r %i" % (task_index, original_task_queue_length, duration,job_id,worker))
                if(len(tasks) == 0):
                    self.btmap.flip(worker)

    #This function is needed in the first place to ensure best fit algorithm does not make a false placement
    #for a hole which is non existent when cores are actually freed. This is because of difference in precision
    #between best fit time algorithm which takes ints, and actual event and task duration times which are floats.
    #Best effort adjusting holes to reflect the true best fit start time.
    #No need to check if it was updated or not. This is because the holes might have all
    #been taken up end to end. In which case, the tasks will be served on their best fit and completion times.
    def shift_holes(self, core_indices, previous_best_fit_time, task_duration, current_best_time):
        for worker_index in core_indices:
            found_hole = False
            for hole_index in range(len(self.worker_queues_free_time_end[worker_index]) - 1):
                end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                start_next_hole = self.worker_queues_free_time_start[worker_index][hole_index + 1]
                if end_hole == previous_best_fit_time and start_next_hole == (previous_best_fit_time + task_duration):
                    found_hole = True
                    self.worker_queues_free_time_end[worker_index][hole_index] = current_best_time
                    self.worker_queues_free_time_start[worker_index][hole_index + 1] = current_best_time + task_duration
                    #Are the holes of length 0? Then remove them from arrays.
                    next_index = hole_index + 1
                    if self.worker_queues_free_time_start[worker_index][hole_index] >= self.worker_queues_free_time_end[worker_index][hole_index]:
                        self.worker_queues_free_time_start[worker_index].pop(hole_index)
                        self.worker_queues_free_time_end[worker_index].pop(hole_index)
                        next_index = hole_index
                    if self.worker_queues_free_time_start[worker_index][next_index] >= self.worker_queues_free_time_end[worker_index][next_index]:
                        self.worker_queues_free_time_start[worker_index].pop(next_index)
                        self.worker_queues_free_time_end[worker_index].pop(next_index)
                    break

    #Remove outdated holes
    def update_worker_queues_free_time(self, worker_indices, best_fit_start, best_fit_end, current_time):
        if best_fit_start >= best_fit_end:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start larger than or equal to best fit end')
        if current_time > best_fit_start:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start happened before insertion into core queues')

        # Cleanup stale holes
        # TODO - Ensure holes are always ordered by time
        for worker_index in worker_indices:
            while len(self.worker_queues_free_time_start[worker_index]) > 0:
                if current_time > self.worker_queues_free_time_end[worker_index][0]:
                    #Cull this hole. No point keeping it around now.
                    self.worker_queues_free_time_start[worker_index].pop(0)
                    self.worker_queues_free_time_end[worker_index].pop(0)
                else:
                    break

        for worker_index in worker_indices:
                #Subtract
                found_hole = False
                # Order : start_hole, best_fit_start, best_fit_end, end_hole
                # Find first start just less than or equal to best_fit_start
                #print "Core", worker_index, "holes - for best fit (",best_fit_start, best_fit_end,") is "
                for hole_index in range(len(self.worker_queues_free_time_start[worker_index])):
                    start_hole = self.worker_queues_free_time_start[worker_index][hole_index]
                    end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                    #print "(",start_hole, end_hole,")"
                    if start_hole <= best_fit_start and end_hole >= best_fit_end:
                        found_hole = True
                        #print "Found hole ", start_hole, end_hole, "for serving best fit times", best_fit_start, best_fit_end
                        self.worker_queues_free_time_start[worker_index].pop(hole_index)
                        self.worker_queues_free_time_end[worker_index].pop(hole_index)
                        if start_hole == best_fit_start and end_hole == best_fit_end:
                            break
                        insert_position = hole_index
                        if start_hole < best_fit_start:
                            self.worker_queues_free_time_start[worker_index].insert(insert_position, start_hole)
                            self.worker_queues_free_time_end[worker_index].insert(insert_position, best_fit_start)
                            insert_position += 1
                        if end_hole > best_fit_end:
                            self.worker_queues_free_time_start[worker_index].insert(insert_position, best_fit_end)
                            self.worker_queues_free_time_end[worker_index].insert(insert_position, end_hole)
                        #print "Updated holes at worker", worker_index," is ", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
                        break

                if found_hole == False:
                    raise AssertionError('No hole was found for best fit start and end')
                if len(self.worker_queues_free_time_start[worker_index]) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Lengths of start and end holes are unequal')
                if len(set(self.worker_queues_free_time_start[worker_index])) != len(self.worker_queues_free_time_start[worker_index]):
                    raise AssertionError('Duplicate entries found in start hole array')
                if len(set(self.worker_queues_free_time_end[worker_index])) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Duplicate entries found in end hole array')

#####################################################################################################################
#####################################################################################################################

class NoopGetTaskResponseEvent(Event):
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        return self.worker.free_slot(current_time)

#####################################################################################################################
#####################################################################################################################

class UpdateRemainingTimeEvent(Event):
    def __init__(self, job):
        self.job = job

    def run(self, current_time):
        assert( len(self.job.unscheduled_tasks)>=0)
        self.job.update_remaining_time()
        return []

#####################################################################################################################
#####################################################################################################################

class TaskEndEvent():
    def __init__(self, worker, SCHEDULE_BIG_CENTRALIZED, status_keeper, job_id, job_type_for_scheduling, estimated_task_duration, this_task_id):
        assert SYSTEM_SIMULATED != "Murmuration", "TaskEndEvent should not be called for Murmuration"
        assert SYSTEM_SIMULATED != "Hawk", "TaskEndEvent should not be called for Hawk"
        self.worker = worker
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.status_keeper = status_keeper
        self.job_id = job_id
        self.job_type_for_scheduling = job_type_for_scheduling
        self.estimated_task_duration = estimated_task_duration
        self.this_task_id = this_task_id

    def run(self, current_time):
        assert SYSTEM_SIMULATED != "Murmuration", "TaskEndEvent should not be called for Murmuration"
        assert SYSTEM_SIMULATED != "Hawk", "TaskEndEvent should not be called for Hawk"


        if (self.job_type_for_scheduling != BIG and self.SCHEDULE_BIG_CENTRALIZED):
            self.status_keeper.update_workers_queue([self.worker.id], False, self.estimated_task_duration, self.job_id, current_time)


        self.worker.tstamp_start_crt_big_task =- 1
        return self.worker.free_slot(current_time)


class TaskEndEventForMachines():
    def __init__(self, worker_index, SCHEDULE_BIG_CENTRALIZED, task_duration):
        self.worker_index = worker_index
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.task_duration = task_duration

    def run(self, current_time):
        if SYSTEM_SIMULATED == "Hawk" and self.SCHEDULE_BIG_CENTRALIZED:
            raise AssertionError("Sparrow trying to schedule in a centralized manner?")

        worker = simulation.workers[self.worker_index]
        worker.busy_time += self.task_duration

        return worker.free_slot(current_time)


#####################################################################################################################
#####################################################################################################################
# Support for multi-core machines in Murmuration
# Core = Worker class
class Machine(object):
    def __init__(self, num_slots, id):
        self.num_cores = num_slots
        self.id = id

        self.cores = []
        while len(self.cores) < self.num_cores:
            core_id = self.id * CORES_PER_MACHINE + len(self.cores)
            core = Worker(1, core_id, core_id, core_id)
            self.cores.append(core)

        # Dictionary of core and time when it was freed (used to track the time the core spent idle).
        self.free_cores = {}
        index = 0
        while index < self.num_cores:
            core_id = self.cores[index].id
            self.free_cores[core_id] = 0
            index += 1

        #Enqueued tasks at this machine
        self.queued_probes = Queue.PriorityQueue()

    #Machine class
    def add_machine_probe(self, best_fit_time, probe_params):
        self.queued_probes.put((best_fit_time, probe_params))

    #Machine class
    def free_machine_core(self, core, current_time):
        self.free_cores[core.id] = current_time
        return self.try_process_next_probe_in_the_queue(current_time)

    #Machine class
    #Assumes best fit time is always unique across tasks on the same (machine, cores).
    #Pop best fitting tasks across different cores.
    #Check which has the fastest completion time and process that task.
    #Re-insert the rest of the tasks back into machine queue.
    #This ensures TaskEndEvents, and hence DC time, are monotonically increasing functions.
    def try_process_next_probe_in_the_queue(self, current_time):
        if SYSTEM_SIMULATED == "Hawk":
            return self.try_process_next_random_probe_single(current_time)
        events = []

        #Candidates list among all possible tasks that can execute with current free cores.
        earliest_task_completion_time = float('inf')
        candidate_best_fit_time = 0.0
        candidate_processing_time = 0.0
        candidate_task_info = None
        candidate_cores_covered = {}
        candidate_probes_covered = Queue.PriorityQueue()
        while 1:
            if self.queued_probes.empty() or len(self.free_cores) == 0:
                #Nothing to execute, or nowhere to execute
                break

            #First of the queued tasks in this iteration
            best_fit_time, task_info = self.queued_probes.get()

            # Extract all information
            core_indices = task_info[0]
            job_id = task_info[1]
            job = simulation.jobs[job_id]
            if len(job.unscheduled_tasks) <= 0:
                raise AssertionError('No redundant probes in Murmuration, yet tasks have finished?')
            task_index = task_info[2]
            probe_arrival_time = task_info[3]

            if(not all(x in self.free_cores.keys() for x in core_indices)):
                #Wait for the next event to trigger this task processing
                candidate_probes_covered.put((best_fit_time, task_info))
                #Note these cores, they are not ready to execute yet, but important to clear free_cores list.
                for core_id in core_indices:
                    if core_id in self.free_cores.keys():
                        candidate_cores_covered[core_id] = self.free_cores[core_id]
                        del self.free_cores[core_id]
                continue

            core_available_time = 0
            for core_id in core_indices:
                time = self.free_cores[core_id]
                if core_available_time < time:
                    core_available_time = time
                candidate_cores_covered[core_id] = self.free_cores[core_id]
                del self.free_cores[core_id]

            # Take the larger of the probe arrival time and core free time to determine when the task starts executing.
            # Best fit is just a time estimate, for task completion use the exact start times and durations.
            # So, processing time might be less than the current time, even though task completion will be after current time.
            processing_start_time = core_available_time if core_available_time > probe_arrival_time else probe_arrival_time
            task_actual_duration = job.actual_task_duration[task_index]
            task_completion_time = processing_start_time + task_actual_duration
            if task_completion_time < earliest_task_completion_time:
                #Replace our previous best candidate.
                if earliest_task_completion_time != float('inf'):
                    candidate_probes_covered.put((candidate_best_fit_time, candidate_task_info))
                earliest_task_completion_time = task_completion_time
                candidate_best_fit_time = best_fit_time
                candidate_task_info = task_info
                candidate_processing_time = processing_start_time
            else:
                #Not our best candidate
                candidate_probes_covered.put((best_fit_time, task_info))

        current_time = int(math.ceil(current_time))
        core_indices = []
        if earliest_task_completion_time != float('inf'):
            # Extract all information
            core_indices = candidate_task_info[0]
            job_id = candidate_task_info[1]
            job = simulation.jobs[job_id]
            if len(job.unscheduled_tasks) <= 0:
                raise AssertionError('No redundant probes in Murmuration, yet tasks have finished?')
            task_index = candidate_task_info[2]
            probe_arrival_time = candidate_task_info[3]

            task_actual_duration = job.actual_task_duration[task_index]
            if candidate_best_fit_time < current_time:
                #This can happen due to precision of best fit time being in integers, but not so of task durations.
                keeper.shift_holes(core_indices, candidate_best_fit_time, int(math.ceil(task_actual_duration)), current_time)

            # Finally, process the current task with all these parameters
            task_status, new_events = simulation.get_machine_task(self, core_indices, job_id, task_index, task_actual_duration, candidate_processing_time, probe_arrival_time)
            for new_event in new_events:
                events.append((new_event[0], new_event[1]))

        #Reinsert all free cores other than the ones needed
        for core_index in candidate_cores_covered.keys():
            if core_index not in core_indices:
                self.free_cores[core_index] = candidate_cores_covered[core_index]

        #Reinsert all queued probes that were inspected
        while not candidate_probes_covered.empty():
            best_fit_time, task_info = candidate_probes_covered.get()
            self.queued_probes.put((best_fit_time, task_info))

        return events


    # Machine class
    # Process one queued probes at a time.
    def try_process_next_random_probe_single(self, current_time):
        events = []
        while 1:
            if self.queued_probes.empty() or len(self.free_cores) == 0:
                #Nothing to execute, or nowhere to execute
                break

            #First of the queued tasks
            time, task_info = self.queued_probes.get()

            # Extract all information
            core_indices = task_info[0]
            assert len(core_indices) == 0
            job_id = task_info[1]
            task_index = task_info[2]
            task_actual_duration = task_info[3]
            probe_arrival_time = task_info[4]

            # Remove redundant probes for this task without accounting for them in response time.
            if task_actual_duration not in simulation.jobs[job_id].unscheduled_tasks:
                continue

            if len(self.free_cores.keys()) < task_cpu_request:
                # Required number of cores for this task not yet available.
                self.queued_probes.put((time, task_info))
                break

            core_indices = self.free_cores.keys()[0 :task_cpu_request]
            for core_id in core_indices:
                del self.free_cores[core_id]

            # Note - Current time is the start of the processing time. This is because -
            # 1. The probe has already arrived before now.
            # 2. The cores have been freed before now.
            # 3. There might have been redundant probes because of which this task was
            #    still enqueued till now despite cores available.

            # Finally, process the current task with all these parameters
            task_status, new_events = simulation.get_machine_task(self, core_indices, job_id, task_index, task_actual_duration, current_time, probe_arrival_time)
            for new_event in new_events:
                events.append((new_event[0], new_event[1]))
            break

        return events


#####################################################################################################################
#####################################################################################################################
# Legacy code for single slot workers - Eagle, IdealEagle, Hawk, DLWL and CLWL.
# In Murmuration, this class denotes a single core on a machine.
class Worker(object):
    def __init__(self, num_slots, id, index_last_small, index_first_big):
        self.id = id
        # Parameter to measure how long this worker is busy in the total run.
        self.busy_time = 0.0

        #Role of a scheduler?
        self.scheduler = None
        if SYSTEM_SIMULATED == "Murmuration":
            if random.random() < RATIO_SCHEDULERS_TO_CORES:
                self.scheduler = Scheduler()
        # List of times when slots were freed, for each free slot (used to track the time the worker spends idle).
        # Only in the case of Murmuration, these are managed at the machine level.
        if SYSTEM_SIMULATED != "Murmuration" and SYSTEM_SIMULATED != "Hawk":
            self.free_slots = []
            while len(self.free_slots) < num_slots:
                self.free_slots.append(0)

            self.queued_big = 0
            self.queued_probes = []
            # Some tasks have to be accounted for before they surface on the worker node
            self.executing_big = False

            self.tstamp_start_crt_big_task = -1
            self.estruntime_crt_task = -1

            self.in_small           = False
            self.in_big             = False
            self.in_small_not_big   = False

            if (id <= index_last_small):       self.in_small = True
            if (id >= index_first_big):        self.in_big = True
            if (id < index_first_big):         self.in_small_not_big = True

            self.btmap = None
            self.btmap_tstamp = -1
    #Worker class
    def add_probe(self, job_id, task_length, job_type_for_scheduling, current_time, btmap, handle_stealing):
        if SYSTEM_SIMULATED == "Murmuration":
            raise AssertionError("Murmuration should not invoke ProbeEventForWorkers or worker.add_probe()")
        if SYSTEM_SIMULATED == "Hawk":
            raise AssertionError("Hawk should not invoke ProbeEventForWorkers or worker.add_probe()")
        long_job = job_type_for_scheduling == BIG
        self.queued_probes.append([job_id,task_length,(self.executing_big == True or self.queued_big > 0), 0, False,-1, current_time])

        # Now, tasks are ready to maybe start
        if (long_job):
            self.queued_big     = self.queued_big + 1
            self.btmap          = copy.deepcopy(btmap)
            self.btmap_tstamp   = current_time

        if len(self.queued_probes) > 0 and len(self.free_slots) > 0:
            return self.process_next_probe_in_the_queue(current_time)
        else:
            return []



    #Worker class
    # In Murmuration, free_slot will report the same to the machine class.
    def free_slot(self, current_time):
        if SYSTEM_SIMULATED == "Murmuration" or SYSTEM_SIMULATED == "Hawk":
            machine_id = simulation.get_machine_id_from_worker_id(self.id)
            machine = simulation.machines[machine_id]
            return machine.free_machine_core(self, current_time)

        self.free_slots.append(current_time)
        simulation.increase_free_slots_for_load_tracking(self)
        self.executing_big = False

        if len(self.queued_probes) > 0:
            return self.process_next_probe_in_the_queue(current_time)
        
        if len(self.queued_probes) == 0 and simulation.stealing_allowed == True:
            return self.ask_probes(current_time)
        
        return []

    #Worker class
    def ask_probes(self, current_time):
        new_events  =  []
        new_probes  =  []
        ctr_it      =  0
        from_worker = -1

        while(ctr_it < STEALING_ATTEMPTS and len(new_probes) == 0):
            from_worker,new_probes = simulation.get_probes(current_time, self.id)
            ctr_it += 1

        if(from_worker!=-1 and len(new_probes)!= 0):
            print current_time, ": Worker ", self.id," Stealing: ", len(new_probes), " from: ",from_worker, " attempts: ",ctr_it
            from_worker_obj=simulation.workers[from_worker]
        else:
            print current_time, ": Worker ", self.id," failed to steal. attempts: ",ctr_it

        for job_id, task_length, behind_big, cum, sticky, handle_steal, probe_time in new_probes:
            assert (simulation.jobs[job_id].job_type_for_comparison != BIG)
            new_events.extend(self.add_probe(job_id, task_length, SMALL, current_time, None,True))

        return new_events



    #Worker class
    def get_probes_atc(self, current_time, free_worker_id):
        probes_to_give = []

        i = 0
        skipped_small = 0
        skipped_big = 0

        if not self.executing_big:
            while (i < len(self.queued_probes) and simulation.jobs[self.queued_probes[i][0]].job_type_for_comparison != BIG): #self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                i += 1

        skipped_short = i

        while (i < len(self.queued_probes) and simulation.jobs[self.queued_probes[i][0]].job_type_for_comparison == BIG): #self.queued_probes[i][1] > CUTOFF_THIS_EXP):
            i += 1

        skipped_big = i - skipped_short

        total_time_probes = 0

        nr_short_chosen = 0
        if (i < len(self.queued_probes)):
            while (len(self.queued_probes) > i and simulation.jobs[self.queued_probes[i][0]].job_type_for_scheduling != BIG and nr_short_chosen < STEALING_LIMIT):
                nr_short_chosen += 1
                probes_to_give.append(self.queued_probes.pop(i))
                total_time_probes += probes_to_give[-1][1]

        return probes_to_give



    #Worker class
    def get_probes_random(self, current_time, free_worker_id):
        probes_to_give = []
        all_positions_of_short_tasks = []
        randomly_chosen_short_task_positions = []
        i = 0

        #record the ids (in queued_probes) of the queued short tasks
        while (i < len(self.queued_probes)):
            big_job = simulation.jobs[self.queued_probes[i][0]].job_type_for_scheduling == BIG
            if (not big_job): #self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                all_positions_of_short_tasks.append(i)
            i += 1


        #select the probes to steal
        i = 0
        while (len(all_positions_of_short_tasks) > 0 and len(randomly_chosen_short_task_positions) < STEALING_LIMIT):
            rnd_index = random.randint(0,len(all_positions_of_short_tasks)-1)
            randomly_chosen_short_task_positions.append(all_positions_of_short_tasks.pop(rnd_index))
        randomly_chosen_short_task_positions.sort()

        #remove the selected probes from the worker queue in decreasing order of IDs
        decreasing_ids = len(randomly_chosen_short_task_positions)-1
        total_time_probes = 0
        while (decreasing_ids >= 0):
            probes_to_give.append(self.queued_probes.pop(randomly_chosen_short_task_positions[decreasing_ids]))
            total_time_probes += probes_to_give[-1][1]
            decreasing_ids -= 1
        probes_to_give = probes_to_give[::-1]  #reverse the list of tuples

        return probes_to_give



    #Worker class
    def process_next_probe_in_the_queue(self, current_time):
        if SYSTEM_SIMULATED == "Murmuration":
            raise AssertionError("Murmuration shouldn't be invoking worker.process_next_probe_in_the_queue()")
        if SYSTEM_SIMULATED == "Hawk":
            raise AssertionError("Hawk shouldn't be invoking worker.process_next_probe_in_the_queue()")
        self.free_slots.pop(0)
        simulation.decrease_free_slots_for_load_tracking(self)
        
        if (SRPT_ENABLED):
            if (SYSTEM_SIMULATED == "Eagle" or SYSTEM_SIMULATED == "Hawk"):
                pos = self.get_next_probe_acc_to_sbp_srpt(current_time)
                if (pos == -1):
                    assert (len(self.queued_probes) == 0)
                    #Cannot just return [], free_slot would not be called again
                    return [(current_time, NoopGetTaskResponseEvent(self))]
            else:
                pos = self.get_next_probe_acc_to_srpt(current_time)
        else:
            pos = 0

        job_id = self.queued_probes[pos][0]
        estimated_task_duration = self.queued_probes[pos][1]
        probe_arrival_time = self.queued_probes[pos][6]

        self.executing_big = simulation.jobs[job_id].job_type_for_scheduling == BIG
        if self.executing_big:
            self.queued_big                 = self.queued_big -1
            self.tstamp_start_crt_big_task  = current_time
            self.estruntime_crt_task        = estimated_task_duration
        else:
            self.tstamp_start_crt_big_task = -1

        was_successful = True
        task_status = True
        # This ensures only outstanding jobs are processed.
        # Redundant probes are removed later.
        if len(simulation.jobs[job_id].unscheduled_tasks) == 0 :
            get_task_response_time = current_time + 2 * NETWORK_DELAY
            was_successful = False
            events = [(get_task_response_time, NoopGetTaskResponseEvent(self))]
        else :
            task_status, events = simulation.get_task(job_id, self, current_time, probe_arrival_time)

        job_bydef_big = (simulation.jobs[job_id].job_type_for_comparison == BIG)


        if(SBP_ENABLED==True and was_successful and task_status and not job_bydef_big):
            self.queued_probes[pos][4] = True

        if(SBP_ENABLED==False or not was_successful or job_bydef_big):
            #so the probe remains if SBP is on, task was succesful and is small
            #if task status unexpectedly failed then also probe stays
            #This also removes all redundant probes without accounting for them
            #in response times
            self.queued_probes.pop(pos)

        if(SRPT_ENABLED==True and (SYSTEM_SIMULATED=="Eagle" or SYSTEM_SIMULATED=="Hawk")):
            if(was_successful and task_status and job_bydef_big):
                for delay_it in range(0,pos):
                    self.queued_probes[delay_it][5]=1

            if(was_successful and task_status and not job_bydef_big):
                # Add estimated_delay to probes in front
                for delay_it in range(0,pos):
                    job_id        = self.queued_probes[delay_it][0]
                    new_acc_delay = self.queued_probes[delay_it][3] + estimated_task_duration
                    self.queued_probes[delay_it][3] = new_acc_delay

                    #Debug
                    if(SYSTEM_SIMULATED=="Eagle"):
                        job_bydef_big = simulation.jobs[job_id].job_type_for_comparison == BIG
                        assert(not job_bydef_big)
                        task_duration = simulation.jobs[job_id].estimated_task_duration
                        if (CAP_SRPT_SBP != float('inf')):
                            assert(new_acc_delay <= CAP_SRPT_SBP * task_duration), " Chosen position: %r, length %r , offending  %r" % (pos,len(self.queued_probes), new_acc_delay)
                        else:
                            assert(new_acc_delay <= CAP_SRPT_SBP and new_acc_delay >= 0), " Chosen position: %r, length %r , offending  %r" % (pos,len(self.queued_probes), new_acc_delay)


        return events

    #Worker class
    def get_remaining_exec_time_for_job_dist(self, job_id, current_time):
        job = simulation.jobs[job_id]
        remaining_exec_time = job.remaining_exec_time
        return remaining_exec_time

    #Worker class
    def get_remaining_exec_time_for_job(self, job_id, current_time):
        job = simulation.jobs[job_id]
        if (len(job.unscheduled_tasks) == 0): #Improvement
            remaining_exec_time = -1
        else:
            remaining_exec_time =  len(job.unscheduled_tasks)*job.estimated_task_duration
        return remaining_exec_time

    #Worker class
    def get_next_probe_acc_to_sbp_srpt(self, current_time):
        min_remaining_exec_time = float('inf')
        position_in_queue       = -1
        estimated_delay         = 0
        chosen_is_big           = False    

        shortest_slack_in_front_short = float('inf')

        len_queue = len(self.queued_probes)
        position_it = 0
        while position_it < len_queue:
            job_id    = self.queued_probes[position_it][0]
            remaining = self.get_remaining_exec_time_for_job_dist(job_id, current_time)

            # Improvement: taking out probes of finished jobs
            if (remaining == -1):
                self.queued_probes.pop(position_it)
                len_queue = len(self.queued_probes)
            else:
                job_bydef_big = simulation.jobs[job_id].job_type_for_comparison == BIG
                estimated_task_duration = simulation.jobs[job_id].estimated_task_duration

                jump_ok = False
                if(job_bydef_big and position_it == 0):
                    jump_ok = True
                    chosen_is_big = True
                elif(remaining < min_remaining_exec_time):
                    assert(not chosen_is_big)
                    # Check CAP jobs in front, different for short
                    jump_ok = estimated_task_duration <= shortest_slack_in_front_short

                if(jump_ok):
                    position_in_queue = position_it
                    estimated_delay = estimated_task_duration
                    min_remaining_exec_time = remaining
                    chosen_is_big = job_bydef_big
                    if (chosen_is_big):
                        break;

                    # calculate shortest slack for next probes
                if (CAP_SRPT_SBP != float('inf')):
                    slack = CAP_SRPT_SBP*estimated_task_duration - self.queued_probes[position_it][3]
                    if(CAP_SRPT_SBP == 0):
                        assert(self.queued_probes[position_it][3]==0)
                    assert (slack>=0), " offending value slack: %r CAP %r" % (slack, CAP_SRPT_SBP)

                    if(not job_bydef_big and slack < shortest_slack_in_front_short):
                        shortest_slack_in_front_short = slack
                position_it += 1

        #assert(position_in_queue>=0)

        return position_in_queue

    #Worker class
    def get_next_probe_acc_to_srpt(self, current_time):
        min_remaining_exec_time = float('inf')
        position_in_queue       = -1
        estimated_delay         = 0        

        shortest_slack_in_front = float('inf')
        for position_it in range(0,len(self.queued_probes)):
            job_id    = self.queued_probes[position_it][0]
            remaining = self.get_remaining_exec_time_for_job(job_id, current_time)
            assert(remaining >= 0)

            estimated_task_duration = simulation.jobs[job_id].estimated_task_duration

            if(remaining < min_remaining_exec_time):
                # Check CAP jobs in front
                if(estimated_task_duration < shortest_slack_in_front):        
                    position_in_queue = position_it
                    estimated_delay = simulation.jobs[job_id].estimated_task_duration
                    min_remaining_exec_time = remaining # Optimization

            # calculate shortest slack for next probes
            if (CAP_SRPT_SBP != float('inf')):
                slack = CAP_SRPT_SBP*estimated_task_duration - self.queued_probes[position_it][3] # will be negative is estimated_task_duration = 0
                if(slack < shortest_slack_in_front): #improvement: if its 0, no task will be allowed to bypass it
                    shortest_slack_in_front = slack

        assert(position_in_queue >= 0)
        # Add estimated_delay to probes in front        
        for delay_it in range(0,position_in_queue):
            new_acc_delay = self.queued_probes[delay_it][3] + estimated_delay            
            self.queued_probes[delay_it][3] = new_acc_delay

        return position_in_queue

        

class Scheduler(object):
    def __init__(self):
        pass

#####################################################################################################################
#####################################################################################################################

class Simulation(object):
    def __init__(self, monitor_interval, stealing_allowed, SCHEDULE_BIG_CENTRALIZED, WORKLOAD_FILE,small_job_th,cutoff_big_small,ESTIMATION,off_mean_bottom,off_mean_top,nr_workers):

        CUTOFF_THIS_EXP = float(small_job_th)
        TOTAL_MACHINES = int(nr_workers)
        self.total_free_slots = CORES_PER_MACHINE * TOTAL_MACHINES
        self.jobs = {}
        self.event_queue = Queue.PriorityQueue()
        self.workers = []
        self.machines = []
        self.scheduler_indices = []

        #Do not work with indexes. This is because small and big partition
        #workers may not always be contiguous. Change logic here to store
        #indices instead.
        self.index_last_worker_of_small_partition = int(SMALL_PARTITION*TOTAL_MACHINES*CORES_PER_MACHINE/100)-1
        self.index_first_worker_of_big_partition  = int((100-BIG_PARTITION)*TOTAL_MACHINES*CORES_PER_MACHINE/100)
        # Only for Murmuration, first simulate machines which will simulate worker cores, which will simulate schedulers.
        if SYSTEM_SIMULATED != "Murmuration" and SYSTEM_SIMULATED != "Hawk":
            while len(self.workers) < TOTAL_MACHINES:
                worker = Worker(CORES_PER_MACHINE, len(self.workers),self.index_last_worker_of_small_partition,self.index_first_worker_of_big_partition)
                self.workers.append(worker)
        else:
            # Murmuration and Hawk
            while len(self.machines) < TOTAL_MACHINES:
                machine = Machine(CORES_PER_MACHINE, len(self.machines))
                self.machines.append(machine)
                workers = machine.cores
                self.workers.extend(workers)
                for worker in workers:
                    if worker.scheduler is not None:
                        # Directly access scheduler indices in Simulation class
                        self.scheduler_indices.append(worker.id)
            if SYSTEM_SIMULATED == "Murmuration":
                print "Number of schedulers ", len(self.scheduler_indices)

        self.worker_indices = range(TOTAL_MACHINES)
        self.off_mean_bottom = off_mean_bottom
        self.off_mean_top = off_mean_top
        self.ESTIMATION = ESTIMATION
        self.shared_cluster_status = {}

        #print "self.index_last_worker_of_small_partition:         ", self.index_last_worker_of_small_partition
        #print "self.index_first_worker_of_big_partition:          ", self.index_first_worker_of_big_partition

        self.small_partition_workers_hash =  {}
        self.big_partition_workers_hash = {}

        self.small_partition_workers = self.worker_indices[:self.index_last_worker_of_small_partition+1]    # so not including the worker after :
        for node in self.small_partition_workers:
            self.small_partition_workers_hash[node] = 1

        self.big_partition_workers = self.worker_indices[self.index_first_worker_of_big_partition:]         # so including the worker before :
        for node in self.big_partition_workers:
            self.big_partition_workers_hash[node] = 1

        self.small_not_big_partition_workers = self.worker_indices[:self.index_first_worker_of_big_partition]  # so not including the worker after: 

        #print "Size of self.small_partition_workers_hash:         ", len(self.small_partition_workers_hash)
        #print "Size of self.big_partition_workers_hash:           ", len(self.big_partition_workers_hash)


        self.free_slots_small_partition = len(self.small_partition_workers)
        self.free_slots_big_partition = len(self.big_partition_workers)
        self.free_slots_small_not_big_partition = len(self.small_not_big_partition_workers)
 
        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False

        # Only for Murmuration will CORES_PER_MACHINE be >= 1. For the rest, it is = 1.
        self.stealing_allowed = stealing_allowed
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.WORKLOAD_FILE = WORKLOAD_FILE
        self.btmap_tstamp = -1
        self.clusterstatus_from_btmap = None

        self.btmap = None

    #Simulation class
    def get_machine_id_from_worker_id(self, worker_id):
        return worker_id / CORES_PER_MACHINE

    #Simulation class
    def find_machines_random(self, probe_ratio, nr_tasks, possible_machine_indices, min_probes, cores_requested):
        if possible_machine_indices == []:
            return []
        assert len(cores_requested) == nr_tasks
        chosen_machine_indices = []
        nr_probes = max(probe_ratio*nr_tasks,min_probes)
        # Recalculate probe_ratio incase min_probes are chosen.
        probe_ratio = nr_probes / nr_tasks
        #print "Number of machines for Sparrow job", len(possible_machine_indices), "and num tasks", nr_tasks, "probe ratio", probe_ratio
        task_index = 0
        for task_index in range(0, nr_tasks):
            nr_cores_requested = cores_requested[task_index]
            probe_index = 0
            while probe_index < probe_ratio:
                chosen_machine_id = random.sample(possible_machine_indices, 1)[0]
                if len(self.machines[chosen_machine_id].cores) >= nr_cores_requested:
                    chosen_machine_indices.append(chosen_machine_id)
                    probe_index += 1
        return chosen_machine_indices



    #Simulation class
    def find_workers_random(self, probe_ratio, nr_tasks, possible_worker_indices, min_probes):
        #print "Number of workers for short job", len(possible_worker_indices), "and num tasks", nr_tasks
        chosen_worker_indices = []
        if possible_worker_indices == []:
            return []
        nr_probes = max(probe_ratio*nr_tasks,min_probes)
        for it in range(0,nr_probes):
            rnd_index = random.randint(0,len(possible_worker_indices)-1)
            chosen_worker_indices.append(possible_worker_indices[rnd_index])
        return chosen_worker_indices

    #Simulation class
    def find_workers_long_job_prio(self, num_tasks, estimated_task_duration, current_time, simulation, hash_workers_considered):
        assert SYSTEM_SIMULATED != "Murmuration", "find_workers_long_job_prio() should not be called for Murmuration"
        if hash_workers_considered == []:
            return []
        chosen_worker_indices = []
        workers_needed = num_tasks
        prio_queue = Queue.PriorityQueue()
        empty_nodes = []  #performance optimization
        for index in hash_workers_considered:
            qlen          = keeper.get_workers_queue_status(index)

            worker_obj    = simulation.workers[index]
            worker_id     = index
            if qlen == 0 :
                empty_nodes.append(worker_id)
                if(len(empty_nodes) == workers_needed):
                    break
            else:
                start_of_crt_big_task = worker_obj.tstamp_start_crt_big_task
                assert(current_time >= start_of_crt_big_task)
                adjusted_waiting_time = qlen

                #If the qlen is > 0 and running a long task
                if(start_of_crt_big_task != -1):
                    executed_so_far = current_time - start_of_crt_big_task
                    estimated_crt_task = worker_obj.estruntime_crt_task
                    adjusted_waiting_time = 2*NETWORK_DELAY + qlen - min(executed_so_far,estimated_crt_task)

                assert adjusted_waiting_time >= 0, " offending value for adjusted_waiting_time: %r" % adjusted_waiting_time                 
                prio_queue.put((adjusted_waiting_time,worker_id))

        #performance optimization
        #If there are num_tasks empty nodes, then go ahead allocate them all
        if(len(empty_nodes) == workers_needed):
            return empty_nodes
        else:
            chosen_worker_indices = empty_nodes
            for nodeid in chosen_worker_indices:
                prio_queue.put((estimated_task_duration,nodeid))

        # For nodes that are running long tasks, fill them up on the order of their
        # estimated times
        queue_length,worker = prio_queue.get()
        while (workers_needed > len(chosen_worker_indices)):
            if prio_queue.empty():
                prio_queue.put((queue_length,worker))
                continue
            next_queue_length,next_worker = prio_queue.get()
            while(queue_length <= next_queue_length and len(chosen_worker_indices) < workers_needed):
                chosen_worker_indices.append(worker)
                queue_length += estimated_task_duration

            prio_queue.put((queue_length,worker))
            queue_length = next_queue_length
            worker = next_worker 

        assert workers_needed == len(chosen_worker_indices)
        return chosen_worker_indices


    #Simulation class
    # In Murmuration, workers are multi-core machines
    # Returns list of pair of machine and absolute core ids. [(m1, [c1, c2]), (m2, [c1, c2]),..]
    # Length of list of cores = corresponding cpu_req for the task
    # Ranking and update go hand-in-hand unlike in find_workers_long_job_prio() owing to different cpu requirements
    # for different tasks.
    # TODO: Other strategies - bulk allocation using well-fit, not best fit for tasks
    # Hole filling strategies, etc
    def find_workers_long_job_prio_murmuration(self, job_id, num_tasks, current_time, cpu_reqs_by_tasks, task_actual_durations):
        if num_tasks != len(cpu_reqs_by_tasks):
            raise AssertionError('Number of tasks provided not equal to length of cpu_reqs_by_tasks list')
        hash_machines_considered = set(range(TOTAL_MACHINES))
        global placement_total_time
        est_time_machine_array = numpy.zeros(shape=(TOTAL_MACHINES))
        cores_lists_for_reqs_to_machine_matrix = collections.defaultdict()
        # best_fit_for_tasks = [[ma, [cores]], [mb, [cores]], .... ]
        best_fit_for_tasks = []
        current_time += NETWORK_DELAY
        get_machine_time = keeper.get_machine_est_wait
        machines = self.machines
        placement_start_time = time.time()
        for task_index in xrange(num_tasks):
            cpu_req = cpu_reqs_by_tasks[task_index]
            for machine_id in hash_machines_considered:
                est_time, core_list = get_machine_time(self.machines[machine_id].cores, cpu_req, current_time, task_actual_durations[task_index])
                est_time_machine_array[machine_id] = est_time
                cores_lists_for_reqs_to_machine_matrix[machine_id] = core_list
            best_fit_time = int(numpy.sort(est_time_machine_array)[0])
            if best_fit_time == float('inf'):
                raise AssertionError('Error - Got best fit time that is infinite!')
            chosen_machine = numpy.where(est_time_machine_array == best_fit_time)[0][0]
            cores_at_chosen_machine = cores_lists_for_reqs_to_machine_matrix[chosen_machine]
            #print"Choosing machine", chosen_machine,":[",cores_at_chosen_machine,"] with best fit time ",best_fit_time,"for task #", task_index, " task_duration", task_actual_durations[task_index]," arrival time ", current_time, "requesting", cpu_req, "cores"
            best_fit_for_tasks.append([chosen_machine, cores_at_chosen_machine, best_fit_time])

            #Update est time at this machine and its cores
            probe_params = [cores_at_chosen_machine, job_id, task_index, current_time]
            self.machines[chosen_machine].add_machine_probe(best_fit_time, probe_params)
            keeper.update_worker_queues_free_time(cores_at_chosen_machine, best_fit_time, best_fit_time + int(math.ceil(task_actual_durations[task_index])), current_time)
        cores_lists_for_reqs_to_machine_matrix.clear()
        placement_total_time += time.time() - placement_start_time
        # best_fit_for_tasks = [[ma, [cores]], [mb, [cores]], .... ]
        return best_fit_for_tasks


    #Simulation class
    def find_workers_dlwl(self, estimated_task_durations, workers_queue_status, current_time,simulation, hash_workers_considered):

        chosen_worker_indices = []
        workers_needed = len(estimated_task_durations)
        prio_queue = Queue.PriorityQueue()

        for index in hash_workers_considered:
            qlen          = keeper.get_workers_queue_status(index)
            worker_obj    = simulation.workers[index]

            adjusted_waiting_time = qlen
            start_of_crt_big_task = worker_obj.tstamp_start_crt_big_task # PAMELA: it will always be considered big for DLWL
            assert(current_time >= start_of_crt_big_task)

            if(start_of_crt_big_task != -1):
                executed_so_far = current_time - start_of_crt_big_task
                estimated_crt_task = worker_obj.estruntime_crt_task
                adjusted_waiting_time = max(2*NETWORK_DELAY + qlen - min(executed_so_far,estimated_crt_task),0)

            rand_dlwl = random.randint(0, HEARTBEAT_DELAY)
            adjusted_waiting_time += rand_dlwl
            prio_queue.put((adjusted_waiting_time,index))

        # For now all estimated task duration are the same , optimize performance
        #copy_estimated_task_durations = list(estimated_task_durations)

        queue_length,worker = prio_queue.get()
        while (workers_needed > len(chosen_worker_indices)):

            next_queue_length,next_worker = prio_queue.get()
            assert queue_length >= 0
            assert next_queue_length >= 0

            while(queue_length <= next_queue_length and len(chosen_worker_indices) < workers_needed):
                chosen_worker_indices.append(worker)
                queue_length += estimated_task_durations[0] #copy_estimated_task_durations.pop(0)

            prio_queue.put((queue_length,worker))
            queue_length = next_queue_length
            worker = next_worker 

        assert workers_needed == len(chosen_worker_indices)
        return chosen_worker_indices

    #Simulation class
    def send_probes(self, job, current_time, worker_indices, btmap):
        if SYSTEM_SIMULATED == "Hawk" or SYSTEM_SIMULATED == "IdealEagle":
            return self.send_machine_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "Eagle":
            return self.send_probes_eagle(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "CLWL":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "DLWL":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "Murmuration":
            return self.send_probes_murmuration(job, current_time, worker_indices, btmap)


    #Simulation class
    def send_machine_probes_hawk(self, job, current_time, machine_indices, btmap):
        self.jobs[job.id] = job
        task_arrival_events = []
        nr_probes = max(PROBE_RATIO * job.num_tasks, MIN_NR_PROBES)
        #Recalculate for the case when nr_probes == min_probes.
        probe_ratio = nr_probes / job.num_tasks
        assert probe_ratio * job.num_tasks == len(machine_indices)
        machine_ids = set()
        current_time += NETWORK_DELAY
        for index in range(len(machine_indices)):
            machine_id = machine_indices[index]
            task_index = index / probe_ratio
            # The exact cores are a matter of availability at the machine.
            machine_ids.add(machine_id)
            self.machines[machine_id].add_machine_probe([[], job.id, task_index, job.actual_task_duration[task_index], current_time, current_time])

        for machine_id in machine_ids:
            task_arrival_events.append((current_time, ProbeEventForMachines(self.machines[machine_id])))

        return task_arrival_events

    #Simulation class
    def send_probes_hawk(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job

        probe_events = []
        for worker_index in worker_indices:
            probe_events.append((current_time + NETWORK_DELAY, ProbeEventForWorkers(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, btmap)))
            job.probed_workers.add(worker_index)
        return probe_events


    #Simulation class
    def try_round_of_probing(self, current_time, job, worker_list, probe_events, roundnr):
        successful_worker_indices = []
        id_worker_with_newest_btmap = -1
        freshest_btmap_tstamp = self.btmap_tstamp

        for worker_index in worker_list:
            if(not self.workers[worker_index].executing_big and not self.workers[worker_index].queued_big):
                probe_time = current_time + NETWORK_DELAY*(roundnr/2+1)
                probe_events.append((probe_time, ProbeEventForWorkers(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, None)))
                successful_worker_indices.append(worker_index)
            if (self.workers[worker_index].btmap_tstamp > freshest_btmap_tstamp):
                id_worker_with_newest_btmap = worker_index
                freshest_btmap_tstamp       = self.workers[worker_index].btmap_tstamp

        if (id_worker_with_newest_btmap != -1):                    
            self.btmap = copy.deepcopy(self.workers[id_worker_with_newest_btmap].btmap)
            self.btmap_tstamp = self.workers[id_worker_with_newest_btmap].btmap_tstamp

        missing_probes = len(worker_list)-len(successful_worker_indices)
        return len(successful_worker_indices), successful_worker_indices
        

    #Simulation class
    def get_list_non_long_job_workers_from_btmap(self,btmap):
        non_long_job_workers = []
            
        non_long_job_workers = self.small_not_big_partition_workers[:]

        for index in self.big_partition_workers:
            if not btmap.test(index):
                non_long_job_workers.append(index)
        return non_long_job_workers


    def get_list_non_long_job_workers_from_bp_from_btmap(self,btmap):
        non_long_job_workers = []
            
        for index in self.big_partition_workers:
            if not btmap.test(index):
                non_long_job_workers.append(index)
        return non_long_job_workers

    #Simulation class
    def send_probes_eagle(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job
        probe_events = []

        if (job.job_type_for_scheduling == BIG):
            for worker_index in worker_indices:
                probe_events.append((current_time + NETWORK_DELAY, ProbeEventForWorkers(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, btmap)))

        else:
            missing_probes = len(worker_indices)
            self.btmap_tstamp = -1
            self.btmap = None
            ROUNDS_SCHEDULING = 2 

            for i in range(0,ROUNDS_SCHEDULING):
                ok_nr, ok_nodes = self.try_round_of_probing(current_time,job,worker_indices,probe_events,i+1)  
                missing_probes -= ok_nr

                if(missing_probes == 0):
                    return probe_events
 
                assert(missing_probes >= 0)               
 
                list_non_long_job_workers = self.get_list_non_long_job_workers_from_btmap(self.btmap)
                #small
                #list_non_long_job_workers = self.get_list_non_long_job_workers_from_bp_from_btmap(self.btmap)
                #if(len(list_non_long_job_workers)==0):
                 #   break
                worker_indices = self.find_workers_random(1, missing_probes, list_non_long_job_workers,0)

            if(missing_probes > 0):
                worker_indices = self.find_workers_random(1, missing_probes, self.small_not_big_partition_workers,0)
                self.try_round_of_probing(current_time,job,worker_indices,probe_events,ROUNDS_SCHEDULING+1)  
        
        return probe_events

    # Simulation class
    # Contains optimization to sort workers just once
    def send_probes_murmuration(self, job, current_time, worker_indices, btmap):
        long_job = job.job_type_for_scheduling == BIG
        if not long_job:
            raise AssertionError('Murmuration received a short job?')

        self.jobs[job.id] = job
        task_arrival_events = []
 
        # scheduler_index denotes the exactly one scheduler node ID where this job request lands.
        scheduler_index = worker_indices[0]
        scheduler = self.workers[scheduler_index].scheduler
        job.scheduled_by = scheduler

        # Some safety checks
        if len(worker_indices) != 1:
            raise AssertionError('Murmuration received more than one scheduler for the job?')
        if scheduler is None:
            raise AssertionError('Murmuration received a None scheduler?')

        # Sort all workers running long jobs in this DC according to their estimated times.
        # Ranking policy used - Least estimated time and hole duration > estimted task time.
        # btmap indicates workers where long jobs are running
        # Find workers and update the cluster status for long jobs
        # Returns - [[m1,[cores]], [m2,[cores]],...]
        machine_indices = self.find_workers_long_job_prio_murmuration(job.id, job.num_tasks, current_time, job.cpu_reqs_by_tasks, job.actual_task_duration)
        if len(machine_indices) != job.num_tasks:
            raise AssertionError('Send probes received unequal number of machine indices and tasks')
        current_time += NETWORK_DELAY
        machine_ids = BitMap()
        for index in range(len(machine_indices)):
            machine_allocation = machine_indices[index]
            machine_id = machine_allocation[0]
            core_ids = machine_allocation[1]
            best_fit_time = machine_allocation[2]
            if len(core_ids) != job.cpu_reqs_by_tasks[index]:
                raise AssertionError("Not enough machines that pass filter requirement of job")
            machine_ids.add(machine_id)

        for machine_id in machine_ids:
            task_arrival_events.append((current_time, ProbeEventForMachines(self.machines[machine_id])))

        # Return task arrival events for long jobs
        return task_arrival_events

    #Simulation class
    #bookkeeping for tracking the load
    def increase_free_slots_for_load_tracking(self, worker):
        self.total_free_slots += 1
        if(worker.in_small):                self.free_slots_small_partition += 1
        if(worker.in_big):                  self.free_slots_big_partition += 1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition += 1


    #Simulation class
    #bookkeeping for tracking the load
    def decrease_free_slots_for_load_tracking(self, worker):
        self.total_free_slots -= 1
        if(worker.in_small):                self.free_slots_small_partition -= 1
        if(worker.in_big):                  self.free_slots_big_partition -= 1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition -= 1


    #Simulation class
    def get_task(self, job_id, worker, current_time, probe_arrival_time):
        job = self.jobs[job_id]
        #account for the fact that this is called when the probe is launched but it needs an RTT to talk to the scheduler
        response_time = 2 * NETWORK_DELAY
        get_task_response_time = current_time + response_time
        task_wait_time = current_time - probe_arrival_time
        scheduler_algorithm_time = probe_arrival_time - job.start_time

        this_task_id=job.completed_tasks_count
        events = []
        task_duration = job.unscheduled_tasks.pop()
        worker.busy_time += task_duration
        task_completion_time = task_duration + get_task_response_time
        #print current_time, " worker:", worker.id, " task from job ", job_id, " task duration: ", task_duration, " will finish at time ", task_completion_time
        is_job_complete = job.update_task_completion_details(task_completion_time)

        if is_job_complete:
            self.jobs_completed += 1;
            # Task's total time = Scheduler queue time (=0) + Scheduler Algorithm time + Worker queue wait time + Task processing time
            print >> finished_file, task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " by_def: ",job.job_type_for_comparison, " total_job_running_time: ",(job.end_time - job.start_time), " job_id", job_id, " scheduler_algorithm_time ", scheduler_algorithm_time, " task_wait_time ", task_wait_time, " task_processing_time ", task_duration

        events.append((task_completion_time, TaskEndEvent(worker, self.SCHEDULE_BIG_CENTRALIZED, keeper, job.id, job.job_type_for_scheduling, job.estimated_task_duration, this_task_id)))
        
        if SRPT_ENABLED and SYSTEM_SIMULATED == "Eagle":
                events.append((current_time + 2*NETWORK_DELAY, UpdateRemainingTimeEvent(job)))

        return True, events

    #Simulation class
    def get_machine_task(self, machine, core_indices, job_id, task_index, task_duration, current_time, probe_arrival_time):
        job = self.jobs[job_id]

        task_wait_time = current_time - probe_arrival_time
        scheduler_algorithm_time = probe_arrival_time - job.start_time

        events = []
        job.unscheduled_tasks.remove(task_duration)
        # Machine busy time should be a sum of worker busy times.
        workers = self.workers
        task_completion_time = task_duration + current_time
        if SYSTEM_SIMULATED == "Hawk":
            #Account for time for the probe to get task data and details.
            task_completion_time += 2 * NETWORK_DELAY
        #print current_time, " machine ", machine.id, "cores ",core_indices, " job id ", job_id, " task index: ", task_index," task duration: ", task_duration, " arrived at ", probe_arrival_time, "and will finish at time ", task_completion_time

        is_job_complete = job.update_task_completion_details(task_completion_time)

        if is_job_complete:
            self.jobs_completed += 1
            # Task's total time = Scheduler queue time (=0) + Scheduler Algorithm time + Machine queue wait time + Task processing time
            try:
                print >> finished_file, task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " by_def: ",job.job_type_for_comparison, " total_job_running_time: ",(job.end_time - job.start_time), " job_id", job_id, " scheduler_algorithm_time ", scheduler_algorithm_time, " task_wait_time ", task_wait_time, " task_processing_time ", task_duration
            except IOError, e:
                print "Failed writing to output file due to ", e

        for core_index in core_indices:
            events.append((task_completion_time, TaskEndEventForMachines(core_index, self.SCHEDULE_BIG_CENTRALIZED, task_duration)))

        if is_job_complete:
            del job.unscheduled_tasks
            del job.actual_task_duration
            del job.cpu_reqs_by_tasks
            del self.jobs[job.id]

        return True, events

    #Simulation class
    def get_probes(self, current_time, free_worker_id):
        worker_index = random.choice(list(self.big_partition_workers_hash.keys()))
        if     (STEALING_STRATEGY ==  "ATC"):       probes = self.workers[worker_index].get_probes_atc(current_time, free_worker_id)
        elif (STEALING_STRATEGY == "RANDOM"):       probes = self.workers[worker_index].get_probes_random(current_time, free_worker_id)

        return worker_index,probes

    #Simulation class
    def run(self):
        global utilization
        last_time = 0

        self.jobs_file = open(self.WORKLOAD_FILE, 'r')
        line = self.jobs_file.readline()
        first_time = float(line.split()[0])

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        estimate_distribution = EstimationErrorDistribution.MEAN
        if(self.ESTIMATION == "MEAN"):
            estimate_distribution = EstimationErrorDistribution.MEAN
            self.off_mean_bottom = self.off_mean_top = 0
        elif(self.ESTIMATION == "CONSTANT"):
            estimate_distribution = EstimationErrorDistribution.CONSTANT
            assert(self.off_mean_bottom == self.off_mean_top)
        elif(self.ESTIMATION == "RANDOM"):
            estimate_distribution = EstimationErrorDistribution.RANDOM
            assert(self.off_mean_bottom > 0)
            assert(self.off_mean_top > 0)
            assert(self.off_mean_top>self.off_mean_bottom)

        if(SYSTEM_SIMULATED == "DLWL"):
            first_time = 0
            self.shared_cluster_status = keeper.get_queue_status()
            self.event_queue.put((0, WorkerHeartbeatEvent(self)))

        new_job = Job(self.task_distribution, line, estimate_distribution, self.off_mean_bottom, self.off_mean_top)
        self.event_queue.put((float(line.split()[0]), JobArrival(self.task_distribution, new_job, self.jobs_file)))
        self.jobs_scheduled = 1
        #self.event_queue.put((float(line.split()[0]), PeriodicTimerEvent(self)))

        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            if current_time < last_time:
                raise AssertionError("Got current time "+ str(current_time)+" less than last time "+ str(last_time))
            last_time = current_time
            last_event = event
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        #Free up all memory
        del self.machines[:]
        del self.scheduler_indices[:]
        del self.workers[:]
        print "Simulation ending, no more events. Jobs completed", self.jobs_completed
        self.jobs_file.close()

        # Calculate utilizations of worker machines in DC
        time_elapsed_in_dc = current_time - first_time
        print "Total time elapsed in the DC is", time_elapsed_in_dc, "s" 
        total_busyness = 0
        for worker in self.workers:
            total_busyness += worker.busy_time
        utilization = 100 * (float(total_busyness) / float(time_elapsed_in_dc * TOTAL_MACHINES*CORES_PER_MACHINE))
        #TODO - Bug - Average utilization in  Murmuration  with  2 machines and  2  cores/machine  LEAST_LEFTOVER  hole fitting policy is  0.0
        print "Average utilization in ", SYSTEM_SIMULATED, " with ", TOTAL_MACHINES,"machines and ",CORES_PER_MACHINE, " cores/machine is ", utilization

#####################################################################################################################
#globals

finished_file   = open('finished_file', 'w')

NETWORK_DELAY = 0.0005
BIG = 1
SMALL = 0

job_count = 1

random.seed(123456798)
if(len(sys.argv) != 24):
    print "Incorrent number of parameters."
    sys.exit(1)

utilization = 0
placement_total_time = 0

WORKLOAD_FILE                   = sys.argv[1]
stealing                        = (sys.argv[2] == "yes")
SCHEDULE_BIG_CENTRALIZED        = (sys.argv[3] == "yes")
CUTOFF_THIS_EXP                 = float(sys.argv[4])        #
CUTOFF_BIG_SMALL                = float(sys.argv[5])        #
SMALL_PARTITION                 = float(sys.argv[6])          #from the start of worker_indices
BIG_PARTITION                   = float(sys.argv[7])          #from the end of worker_indices
CORES_PER_MACHINE               = int(sys.argv[8])
PROBE_RATIO                     = int(sys.argv[9])
MONITOR_INTERVAL                = int(sys.argv[10])
ESTIMATION                      = sys.argv[11]              #MEAN, CONSTANT or RANDOM
OFF_MEAN_BOTTOM                 = float(sys.argv[12])       # > 0
OFF_MEAN_TOP                    = float(sys.argv[13])       # >= OFF_MEAN_BOTTOM
STEALING_STRATEGY               = sys.argv[14]
STEALING_LIMIT                  = int(sys.argv[15])         #cap on the nr of tasks to steal from one node
STEALING_ATTEMPTS               = int(sys.argv[16])         #cap on the nr of nodes to contact for stealing
TOTAL_MACHINES                   = int(sys.argv[17])
SRPT_ENABLED                    = (sys.argv[18] == "yes")
HEARTBEAT_DELAY                 = int(sys.argv[19])
MIN_NR_PROBES                   = int(sys.argv[20])
SBP_ENABLED                     = (sys.argv[21] == "yes")
SYSTEM_SIMULATED                = sys.argv[22]  
POLICY                          = sys.argv[23]              #RANDOM, LEAST_LEFTOVER, MOST_LEFTOVER

#MIN_NR_PROBES = 20 #1/100*TOTAL_MACHINES
CAP_SRPT_SBP = 5 #cap on the % of slowdown a job can tolerate for SRPT and SBP

t1 = time.time()
keeper = ClusterStatusKeeper(TOTAL_MACHINES * CORES_PER_MACHINE)
simulation = Simulation(MONITOR_INTERVAL, stealing, SCHEDULE_BIG_CENTRALIZED, WORKLOAD_FILE,CUTOFF_THIS_EXP,CUTOFF_BIG_SMALL,ESTIMATION,OFF_MEAN_BOTTOM,OFF_MEAN_TOP,TOTAL_MACHINES)
simulation.run()

print "Simulation ended in ", (time.time() - t1), " s "
print "Placement total time ", placement_total_time
print >> finished_file, "Average utilization in ", SYSTEM_SIMULATED, " with ", TOTAL_MACHINES,"machines and ",CORES_PER_MACHINE, " cores/machine ", POLICY," hole fitting policy is ", utilization

finished_file.close()

# Generate CDF data
import os; os.system("python process.py finished_file "+ SYSTEM_SIMULATED + " " + "long" + " " + WORKLOAD_FILE + " " + str(TOTAL_MACHINES))
