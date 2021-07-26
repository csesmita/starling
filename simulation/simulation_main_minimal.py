#
# MURMURATION
#
# Copyright 2021 - Systems Research Lab, CS Department, University of Cambridge

import sys
import random

from time import time
from math import ceil
from Queue import PriorityQueue
from operator import itemgetter
from collections import deque, defaultdict
from copy import deepcopy

import matplotlib.pyplot as plt
import numpy as np

class Job(object):
    def __init__(self, line):
        global job_count

        job_args                    = (line.split('\n'))[0].split()
        self.start_time             = float(job_args[0])
        self.num_tasks              = int(job_args[1])
        mean_task_duration          = float(job_args[2])

        self.id = job_count
        job_count += 1
        self.completed_tasks_count = 0
        self.end_time = self.start_time
        self.unscheduled_tasks = deque()
        self.actual_task_duration = deque()
        self.cpu_reqs_by_tasks = deque()
        #self.cpu_avg_per_task = deque()
        #self.cpu_max_per_task = deque()

        self.file_task_execution_time(job_args)
        self.estimated_task_duration = mean_task_duration
        self.remaining_exec_time = self.estimated_task_duration*len(self.unscheduled_tasks)

        self.should_finish_time = self.start_time

        self.num_collisions = 0

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
               self.cpu_reqs_by_tasks.appendleft(int(ceil(float(job_args[3 + i + self.num_tasks]))))
               #self.cpu_avg_per_task.appendleft(float(job_args[3 + i + 2*self.num_tasks]))
               #self.cpu_max_per_task.appendleft(float(job_args[3 + i + 3*self.num_tasks]))
           else:
               # For Yahoo workload.
               # HACK! Instead of changing traces file, make this value multi core instead
               if CORE_DISTRIBUTION == "STATIC":
                   cores_needed += 1
                   self.cpu_reqs_by_tasks.appendleft(cores_needed)
                   if cores_needed == CORES_PER_MACHINE:
                       cores_needed = 0
               elif CORE_DISTRIBUTION == "GAUSSIAN":
                   cores_needed = float('inf')
                   #Cap number of cores
                   while cores_needed > 128 or cores_needed < 2:
                       cores_needed = pow(2,int(random.gauss(CORES_PER_MACHINE, CORE_DISTRIBUTION_DEVIATION)))
                   self.cpu_reqs_by_tasks.appendleft(cores_needed)

        if len(self.unscheduled_tasks) != self.num_tasks:
            raise AssertionError('file_task_execution_time(): Number of unscheduled tasks is not the same as number of tasks')

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
    def __init__(self, job, jobs_file):
        self.job = job
        self.jobs_file = jobs_file

    def run(self, current_time):
        global t1
        new_events = []

        worker_indices = []
        if (SYSTEM_SIMULATED == "Murmuration"):
            # Find a random scheduler node for landing the job request.
            # worker_indices for Murmuration only indicates the
            # scheduler. send_probes() does both worker selection and
            # sending of task requests.
            worker_indices.append(random.choice(simulation.scheduler_indices))
            if self.job.id % 100 == 0:
                print current_time, ":   Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration, "simulation time", time() - t1
        else:
            # Sparrow
            if self.job.id % 100 == 0:
                print current_time, ":   Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration, "simulation time", time() - t1
            worker_indices = simulation.find_machines_random(PROBE_RATIO, self.job.num_tasks, set(range(TOTAL_MACHINES)), self.job.cpu_reqs_by_tasks)

        new_events = simulation.send_probes(self.job, current_time, worker_indices)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if (line == ''):
            simulation.scheduled_last_job = True
            return new_events
        self.job = Job(line)
        new_events.append((self.job.start_time, self))
        simulation.jobs_scheduled += 1

        return new_events

#####################################################################################################################
#####################################################################################################################
# ClusterStatusKeeper: Keeps track of tasks assigned to workers and the 
# estimated start time of each task. The queue also includes currently
# executing task at the worker.
class ClusterStatusKeeperCentral(object):
    #In Murmuration, worker is the absolute core id, unique to every core in every machine of the DC.
    def __init__(self, num_workers):
        self.worker_queues_free_time_end = {}
        self.worker_queues_free_time_start = {}
        #Array of history in order to simulate delayed updates
        self.worker_queues_history = {}
        for i in range(0, num_workers):
           self.worker_queues_free_time_start[i] = [0]
           self.worker_queues_free_time_end[i] = [float('inf')]
           #History will be {insertion_time: [start_task_time, end_task_time, scheduler_index]} format
           #Positive task times indicate holes to be put back in.
           #Negative task times indicate holes to be removed.
           self.worker_queues_history[i] = defaultdict(list)

    def print_holes(self, worker_index):
        print "Actual holes are", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]

    def print_history(self, worker_index):
        print "History of placements - ", self.worker_queues_history[worker_index]

    def adjust_propagation_delay(self, core_id, current_time, delay, new_scheduler_index):
        return self.worker_queues_free_time_start[core_id], self.worker_queues_free_time_end[core_id]

    # ClusterStatusKeeper class
    # get_machine_est_wait() -
    # Returns per task estimated wait time and list of cores that can accommodate that task at that estimated time.
    # Returns - ([est_task1, est_task2, ...],[[list of cores],[list of core], [],...])
    # Might return smaller values for smaller cpu req, even if those requests come in later.
    # Also, converts holes to ints except the last entry in end which is float('inf')
    def get_machine_est_wait(self, cores, cpu_req, arrival_time, task_duration, delay, best_current_time, scheduler_index, to_print = False):
        num_cores = len(cores)
        if num_cores < cpu_req:
            return (float('inf'), [])

        est_time_for_tasks = []
        cores_list_for_tasks = []
        # Generate all possible holes to fit each task (D=task duration, N = num cpus needed)
        arrival_time = int(ceil(arrival_time))
        #Fit into smallest integral duration hole
        task_duration = int(ceil(task_duration))
        # Number of cores requested has to be atleast equal to the number of cores on the machine
        # Filter out machines that have less than requested number of cores
        all_slots_list = set()
        all_slots_list_add = all_slots_list.add
        all_slots_list_cores = defaultdict(set)
        all_slots_fragmentation = defaultdict(dict)
        inf_hole_start = {}
        #max_time_start = best_current_time
        for core in cores:
            core_id = core.id
            time_start, time_end = self.adjust_propagation_delay(core_id, arrival_time, delay, scheduler_index)
            if to_print:
                print "For core", core_id,"current holes are", self.worker_queues_free_time_start[core_id], self.worker_queues_free_time_end[core_id],"adjusted holes - ", time_start, time_end, "task duration", task_duration
            if len(time_end) != len(time_start):
                raise AssertionError('Error in get_machine_est_wait - mismatch in lengths of start and end hole arrays')
            for hole in range(len(time_end)):
                if time_start[hole] >= time_end[hole]:
                    print "Error in get_machine_est_wait - start of hole is equal or larger than end of hole"
                    print "Core index", core_id, "hole id ", hole, "start is ", time_start[hole], "end is ", time_end[hole]
                    print "Holes are", time_start, time_end
                    raise AssertionError('Error in get_machine_est_wait - start of hole is equal or larger than end of hole')
                #Be done if time exceeds the present best fit
                if time_start[hole] > best_current_time:
                    break
                # Skip holes that are too small
                if time_end[hole] - time_start[hole] < task_duration:
                    continue
                # Skip holes before arrival time
                if time_end[hole] < arrival_time:
                    continue
                start = time_start[hole] if arrival_time <= time_start[hole] else arrival_time
                if time_end[hole] != float('inf'):
                    # Find all possible holes at every granularity, each lasting task_duration time.
                    end = time_end[hole] - task_duration + 1
                    end = min(end, best_current_time)
                    #time granularity of 1.
                    arr = range(start, end, 1)
                    for start_chunk in arr:
                        #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start_chunk
                        all_slots_list_add(start_chunk)
                        all_slots_list_cores[start_chunk].add(core_id)
                        all_slots_fragmentation[start_chunk][core_id] = max(start_chunk - start, time_end[hole] - start_chunk - task_duration)
                        #if max_time_start > start_chunk and len(all_slots_list_cores[start_chunk]) >= cpu_req:
                        #    max_time_start = start_chunk
                        #    break
                else:
                    all_slots_list_add(start)
                    all_slots_list_cores[start].add(core_id)
                    all_slots_fragmentation[start][core_id] = 0
                    inf_hole_start[core_id] = start
                    #if max_time_start > start and len(all_slots_list_cores[start]) >= cpu_req:
                    #    max_time_start = start
                    #    break
                    #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start

        all_slots_list = sorted(all_slots_list)
        for core, inf_start in inf_hole_start.items():
            for start in all_slots_list:
                if start > inf_start:
                    all_slots_list_cores[start].add(core)
                    all_slots_fragmentation[start][core] = start - inf_start

        if to_print:
            print "Got all possible start times", all_slots_list
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
                    if POLICY == "WORST_FIT":
                        #Select cores with largest available hole after allocation
                        #sorted_cores_fragmented = sorted(cores_fragmented.items(), key=lambda v: (v[1], random.random()), reverse=True)[0:cpu_req]
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=True)[0:cpu_req]
                    elif POLICY == "BEST_FIT":
                        #Select cores with smallest available hole after allocation
                        #sorted_cores_fragmented = sorted(cores_fragmented.items(), key=lambda v: (v[1], random.random()), reverse=False)[0:cpu_req]
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=False)[0:cpu_req]
                    else:
                        raise AssertionError('Check the name of the policy. Should be RANDOM, WORST_FIT OR BEST_FIT')
                    cores_list = set(dict(sorted_cores_fragmented).keys())
                #print "Earliest start time for task duration", task_duration,"needing", cpu_req,"cores is ", start_time, "with core", list(cores_list)[0]
                # cpu_req is available when the fastest cpu_req number of cores is
                # available for use at or after arrival_time.
                return (start_time, cores_list)
        return (float('inf'), [])

    def update_history_holes(self, worker_index, current_time, best_fit_start, best_fit_end, task_is_leaving, scheduler_index, has_collision):
        current_time = int(ceil(current_time))
        if task_is_leaving:
            best_fit_start = -1 * best_fit_start
            best_fit_end   = -1 * best_fit_end
        #print "Updating history hole - Core", worker_index,"best fit times for task that just got allocated", best_fit_start, best_fit_end, "its current holes", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index], "current time",  current_time
        #print "core",worker_index,"scheduler", scheduler_index," allocated core", worker_index,"for best fit times", best_fit_start, best_fit_end, "current time",  current_time, "when holes are", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
        self.worker_queues_history[worker_index][current_time].append([best_fit_start, best_fit_end, scheduler_index, has_collision])

    def update_worker_queues_free_time(self, worker_indices, best_fit_start, best_fit_end, current_time, delay, scheduler_index):
        if best_fit_start >= best_fit_end:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start larger than or equal to best fit end')
        if current_time > best_fit_start:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start happened before insertion into core queues')
        # Cleanup stale holes
        for worker_index in worker_indices:
            while len(self.worker_queues_free_time_start[worker_index]) > 0:
                if current_time - 2*UPDATE_DELAY > self.worker_queues_free_time_end[worker_index][0]:
                    #Cull this hole. No point keeping it around now.
                    self.worker_queues_free_time_start[worker_index].pop(0)
                    self.worker_queues_free_time_end[worker_index].pop(0)
                else:
                    break
            history = self.worker_queues_history[worker_index]
            for history_time in history.keys():
                if current_time - 2*UPDATE_DELAY > history_time:
                    del history[history_time]

        #Ensure all workers are available at the best fit time.
        hole_indices = {}
        found_hole = False
        for worker_index in worker_indices:
            # Order : start_hole, best_fit_start, best_fit_end, end_hole
            # Find first start just less than or equal to best_fit_start
            for hole_index in range(len(self.worker_queues_free_time_start[worker_index])):
                start_hole = self.worker_queues_free_time_start[worker_index][hole_index]
                end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                if start_hole <= best_fit_start and end_hole >= best_fit_end:
                    hole_indices[worker_index] = hole_index
                    break

        if len(hole_indices.keys()) == len(worker_indices):
            found_hole = True

        # A real time update at worker should always find the hole.
        if found_hole == False:
            print "Could not find hole for worker(s)", str(worker_index), "for best fit times ", best_fit_start, best_fit_end
            raise AssertionError('No hole was found for best fit start and end at worker(s)')

        if found_hole:
            for worker_index in worker_indices:
                # Order : start_hole, best_fit_start, best_fit_end, end_hole
                # Find first start just less than or equal to best_fit_start
                #print "Core", worker_index, "holes - for best fit (",best_fit_start, best_fit_end,") is "
                hole_index = hole_indices[worker_index]
                start_hole = self.worker_queues_free_time_start[worker_index][hole_index]
                end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                #print "(",start_hole, end_hole,")"
                #print "Found hole ", start_hole, end_hole, "for serving best fit times", best_fit_start, best_fit_end
                self.worker_queues_free_time_start[worker_index].pop(hole_index)
                self.worker_queues_free_time_end[worker_index].pop(hole_index)
                if start_hole == best_fit_start and end_hole == best_fit_end:
                    self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index, False)
                    continue
                insert_position = hole_index
                if start_hole < best_fit_start:
                    self.worker_queues_free_time_start[worker_index].insert(insert_position, start_hole)
                    self.worker_queues_free_time_end[worker_index].insert(insert_position, best_fit_start)
                    insert_position += 1
                if end_hole > best_fit_end:
                    self.worker_queues_free_time_start[worker_index].insert(insert_position, best_fit_end)
                    self.worker_queues_free_time_end[worker_index].insert(insert_position, end_hole)
                #print "Updated holes at Core", worker_index," is ", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
                self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index, False)
                if len(self.worker_queues_free_time_start[worker_index]) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Lengths of start and end holes are unequal')
                if len(set(self.worker_queues_free_time_start[worker_index])) != len(self.worker_queues_free_time_start[worker_index]):
                    raise AssertionError('Duplicate entries found in start hole array')
                if len(set(self.worker_queues_free_time_end[worker_index])) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Duplicate entries found in end hole array')
            return best_fit_start, worker_indices, False


#####################################################################################################################
#####################################################################################################################
# ClusterStatusKeeper: Keeps track of tasks assigned to workers and the 
# estimated start time of each task. The queue also includes currently
# executing task at the worker.
class ClusterStatusKeeper(object):
    #In Murmuration, worker is the absolute core id, unique to every core in every machine of the DC.
    def __init__(self, num_workers):
        self.worker_queues_free_time_end = {}
        self.worker_queues_free_time_start = {}
        #Array of history in order to simulate delayed updates
        self.worker_queues_history = {}
        for i in range(0, num_workers):
           self.worker_queues_free_time_start[i] = [0]
           self.worker_queues_free_time_end[i] = [float('inf')]
           #History will be {insertion_time: [start_task_time, end_task_time, scheduler_index]} format
           #Positive task times indicate holes to be put back in.
           #Negative task times indicate holes to be removed.
           self.worker_queues_history[i] = defaultdict(list)

    def print_holes(self, worker_index):
        print "Actual holes are", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]

    def print_history(self, worker_index):
        print "History of placements - ", self.worker_queues_history[worker_index]

    def adjust_propagation_delay(self, core_id, current_time, delay, new_scheduler_index, to_print = False):
        if not delay:
            return self.worker_queues_free_time_start[core_id], self.worker_queues_free_time_end[core_id]
        #Adjustment for DECENTRALIZED system
        current_time = int(ceil(current_time))
        worker_time_limit = current_time - 2*UPDATE_DELAY if current_time > 2*UPDATE_DELAY else 0
        scheduler_time_limit = current_time - UPDATE_DELAY if current_time > UPDATE_DELAY else 0
        copied = False
        time_start = (self.worker_queues_free_time_start[core_id])
        time_end = (self.worker_queues_free_time_end[core_id])
        #scheduler_placement = [] 
        scheduler_placement = {} 
        #print "Starting print for adjust_propagation_delay()"
        if to_print:
            print "core ", core_id, "scheduler", new_scheduler_index, "applying histories", self.worker_queues_history[core_id], "to starting holes are ", time_start, time_end
        #Apply reversals in the reverse order, starting fron the most recent.
        for history_time, history_lists in sorted(self.worker_queues_history[core_id].items(), reverse=True):
            #Worker placements are seen after 2*Update_Delay.
            #Other Scheduler placements are seen after Update_Delay.
            #Self Scheduler placements are seen with 0 delay.
            if history_time < worker_time_limit:
                break
            for history_list in reversed(history_lists):
                history_hole_start = history_list[0]
                history_hole_end = history_list[1]
                scheduler_index = history_list[2]
                has_collision = history_list[3]
                #Placement by self scheduler is known.
                if scheduler_index is not None and scheduler_index == new_scheduler_index:
                    #The same scheduler had placed this history placement, so it knows about it.
                    #If there was a collision, then it will be replaced with an earlier placement.
                    #scheduler_placement.append((history_hole_start, history_hole_end))
                    '''
                    if history_hole_start in scheduler_placement.keys():
                        print "core", core_id,"Scheduler", new_scheduler_index," sees these scheduler placements -", dict(sorted(scheduler_placement.items()))
                        print "Wants to insert", history_hole_start, history_hole_end, "from time", history_time
                        raise AssertionError('debug')
                    '''
                    scheduler_placement[history_hole_start] = history_hole_end
                    continue
                #Placement by other schedulers beyond scheduler limit is also known, even if colliding.
                if scheduler_index is not None and history_time < scheduler_time_limit:
                    '''
                    if history_hole_start in scheduler_placement.keys():
                        print "core", core_id,"Scheduler", new_scheduler_index," sees these scheduler placements -", dict(sorted(scheduler_placement.items()))
                        print "Wants to insert", history_hole_start, history_hole_end, "from time", history_time, "current time", current_time
                        raise AssertionError('debug')
                    '''
                    scheduler_placement[history_hole_start] = history_hole_end
                    continue
                #Recent colliding placement by other schedulers is not seen, or recorded in worker holes.
                #Therefore, no-op.
                if scheduler_index is not None and history_time >= scheduler_time_limit and has_collision:
                    continue
                #Placement by workers within worker limit and by other schedulers within scheduler limit    
                #is not seen (ones without collision). So, have to be reversed in the scheduler view.
                for hole_index in range(len(time_start)):
                    if time_end[hole_index] < history_hole_start:
                        continue
                    #New tasks assigned reflect as positive holes.
                    #History hole cannot start in a currently existing hole,
                    #else it would've been captured in history.
                    if time_start[hole_index] <= history_hole_start and history_hole_start < time_end[hole_index]:
                        print "Got history hole in between an existing hole. History start and end is ", history_hole_start, history_hole_end,
                        print "Time start and end are", time_start, time_end
                        raise AssertionError('Got a strange case, when history starts in between an existing hole.') 
                    if time_start[hole_index] < history_hole_end and history_hole_end <= time_end[hole_index]:
                        print "Got history hole in between an existing hole. History start and end is ", history_hole_start, history_hole_end,
                        print "Time start and end are", time_start, time_end
                        raise AssertionError('Got a strange case, when history starts in between an existing hole.') 
                    if not copied:
                        time_start = deepcopy(self.worker_queues_free_time_start[core_id])
                        time_end = deepcopy(self.worker_queues_free_time_end[core_id])
                        copied = True
                    #Here, time_start > history_hole_start or time_end == history_hole_start
                    if time_start[hole_index] > history_hole_start:
                        if time_start[hole_index] == history_hole_end:
                            time_start.pop(hole_index)
                            time_start.insert(hole_index, history_hole_start)
                        else:
                            time_start.insert(hole_index, history_hole_start)
                            time_end.insert(hole_index, history_hole_end)
                    if time_end[hole_index] == history_hole_start:
                        time_end.pop(hole_index)
                        time_end.insert(hole_index, history_hole_end)
                    if (hole_index < len(time_end) - 1) and time_end[hole_index] > time_start[hole_index + 1]:
                        print "Got history hole in between an existing hole. History start and end is ", history_hole_start, history_hole_end,
                        print "Time start and end are", time_start, time_end
                        raise AssertionError('Got a strange case, when history starts in between an existing hole.') 
                    #Collapse holes if need be
                    if (hole_index < len(time_end) - 1) and time_end[hole_index] == time_start[hole_index + 1]:
                        time_end.pop(hole_index)
                        time_start.pop(hole_index + 1)
                    if to_print:    
                        print "Reversed ", history_list," from time", history_time, "which was made by scheduler", scheduler_index,"and got new time start and end", time_start, time_end
                    break
                    #Holes are never updated when tasks leave. Neither current, nor history.
                    #Therefore, they will show up as occupied.
        if to_print:            
            print "core", core_id,"Scheduler", new_scheduler_index," sees these scheduler placements -", sorted(scheduler_placement.items())
        for place_start, place_end in sorted(scheduler_placement.items()):
        #while scheduler_placement:
            hole_index = 0
            #placement = scheduler_placement.pop()
            #place_start = placement[0]
            #place_end = placement[1]
            #=:Covers the case when place_start and end are exactly the placement in holes.    
            while time_end[hole_index] <= place_start:
                hole_index += 1
            # Check if the placement already exists
            if time_start[hole_index] >= place_end:
                if to_print:
                    print place_start, place_end,": Placement exists"
                continue
            #Resolve any collisions by placing it at the end of the queue.    
            if not (time_start[hole_index] <= place_start and place_end <= time_end[hole_index]):
                if to_print:
                    print place_start, place_end,"Collision with current holes", time_start, time_end
                width = place_end - place_start
                hole_index = len(time_start) -1
                place_start = time_start[hole_index]
                place_end = place_start + width

            '''
            #Scheduler placements might collide. The scheduler might now know of some other
            #scheduler's placement that collides with its own placment. In such a case,
            #ditch proceeding further.
            if time_start[hole_index] > place_start or place_end > time_end[hole_index]:
                #print 'Got a scheduler placement in between an existing hole. Placement - ', place_start, place_end,
                #print 'Hole', time_start[hole_index], time_end[hole_index]
                if to_print:
                    print place_start, place_end,"Collision3 with current holes", time_start, time_end
                raise AssertionError('Got a scheduler placement in between an existing hole')
                #break
            '''
            #At this point place_start and end occur inside a hole.
            #So, time_start[hole_index] <= place_start and place_end <= time_end[hole_index]
            start_hole = time_start[hole_index]
            end_hole = time_end[hole_index]
            time_start.pop(hole_index)
            time_end.pop(hole_index)
            if start_hole == place_start and end_hole == place_end:
                continue
            if start_hole < place_start:
                time_start.insert(hole_index, start_hole)
                time_end.insert(hole_index, place_start)
                hole_index += 1 
            if end_hole > place_end:
                time_start.insert(hole_index, place_end)
                time_end.insert(hole_index, end_hole) 
            if to_print:
                print "Added placement", place_start, place_end,"and produced current holes", time_start, time_end
        return time_start, time_end


    # ClusterStatusKeeper class
    # get_machine_est_wait() -
    # Returns per task estimated wait time and list of cores that can accommodate that task at that estimated time.
    # Returns - ([est_task1, est_task2, ...],[[list of cores],[list of core], [],...])
    # Might return smaller values for smaller cpu req, even if those requests come in later.
    # Also, converts holes to ints except the last entry in end which is float('inf')
    def get_machine_est_wait(self, cores, cpu_req, arrival_time, task_duration, delay, best_current_time, scheduler_index, to_print = False):
        num_cores = len(cores)
        if num_cores < cpu_req:
            return (float('inf'), [])

        est_time_for_tasks = []
        cores_list_for_tasks = []
        # Generate all possible holes to fit each task (D=task duration, N = num cpus needed)
        arrival_time = int(ceil(arrival_time))
        #Fit into smallest integral duration hole
        task_duration = int(ceil(task_duration))
        # Number of cores requested has to be atleast equal to the number of cores on the machine
        # Filter out machines that have less than requested number of cores
        all_slots_list = set()
        all_slots_list_add = all_slots_list.add
        all_slots_list_cores = defaultdict(set)
        all_slots_fragmentation = defaultdict(dict)
        inf_hole_start = {}
        #max_time_start = best_current_time
        for core in cores:
            core_id = core.id
            time_start, time_end = self.adjust_propagation_delay(core_id, arrival_time, delay, scheduler_index)
            if to_print:
                print "For core", core_id,"current holes are", self.worker_queues_free_time_start[core_id], self.worker_queues_free_time_end[core_id],"adjusted holes - ", time_start, time_end, "task duration", task_duration
            if len(time_end) != len(time_start):
                raise AssertionError('Error in get_machine_est_wait - mismatch in lengths of start and end hole arrays')
            for hole in range(len(time_end)):
                if time_start[hole] >= time_end[hole]:
                    print "Error in get_machine_est_wait - start of hole is equal or larger than end of hole"
                    print "Core index", core_id, "hole id ", hole, "start is ", time_start[hole], "end is ", time_end[hole]
                    print "Holes are", time_start, time_end
                    raise AssertionError('Error in get_machine_est_wait - start of hole is equal or larger than end of hole')
                #Be done if time exceeds the present best fit
                if time_start[hole] >= best_current_time:
                    break
                # Skip holes that are too small
                if time_end[hole] - time_start[hole] < task_duration:
                    continue
                # Skip holes before arrival time
                if time_end[hole] < arrival_time:
                    continue
                start = time_start[hole] if arrival_time <= time_start[hole] else arrival_time
                if time_end[hole] != float('inf'):
                    # Find all possible holes at every granularity, each lasting task_duration time.
                    end = time_end[hole] - task_duration + 1
                    end = min(end, best_current_time)
                    #time granularity of 1.
                    arr = range(start, end, 1)
                    for start_chunk in arr:
                        #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start_chunk
                        all_slots_list_add(start_chunk)
                        all_slots_list_cores[start_chunk].add(core_id)
                        all_slots_fragmentation[start_chunk][core_id] = max(start_chunk - start, time_end[hole] - start_chunk - task_duration)
                        #if max_time_start > start_chunk and len(all_slots_list_cores[start_chunk]) >= cpu_req:
                        #    max_time_start = start_chunk
                        #    break
                else:
                    all_slots_list_add(start)
                    all_slots_list_cores[start].add(core_id)
                    all_slots_fragmentation[start][core_id] = 0
                    inf_hole_start[core_id] = start
                    #if max_time_start > start and len(all_slots_list_cores[start]) >= cpu_req:
                    #    max_time_start = start
                    #    break
                    #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start

        all_slots_list = sorted(all_slots_list)
        for core, inf_start in inf_hole_start.items():
            for start in all_slots_list:
                if start > inf_start:
                    all_slots_list_cores[start].add(core)
                    all_slots_fragmentation[start][core] = start - inf_start

        if to_print:
            print "Got all possible start times", all_slots_list
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
                    if POLICY == "WORST_FIT":
                        #Select cores with largest available hole after allocation
                        #sorted_cores_fragmented = sorted(cores_fragmented.items(), key=lambda v: (v[1], random.random()), reverse=True)[0:cpu_req]
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=True)[0:cpu_req]
                    elif POLICY == "BEST_FIT":
                        #Select cores with smallest available hole after allocation
                        #sorted_cores_fragmented = sorted(cores_fragmented.items(), key=lambda v: (v[1], random.random()), reverse=False)[0:cpu_req]
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=False)[0:cpu_req]
                    else:
                        raise AssertionError('Check the name of the policy. Should be RANDOM, WORST_FIT OR BEST_FIT')
                    cores_list = set(dict(sorted_cores_fragmented).keys())
                #print "Earliest start time for task duration", task_duration,"needing", cpu_req,"cores is ", start_time, "with core", list(cores_list)[0]
                # cpu_req is available when the fastest cpu_req number of cores is
                # available for use at or after arrival_time.
                return (start_time, cores_list)
        return (float('inf'), [])

    def update_history_holes(self, worker_index, current_time, best_fit_start, best_fit_end, task_is_leaving, scheduler_index, has_collision):
        current_time = int(ceil(current_time))
        if task_is_leaving:
            best_fit_start = -1 * best_fit_start
            best_fit_end   = -1 * best_fit_end
        #print "Updating history hole - Core", worker_index,"best fit times for task that just got allocated", best_fit_start, best_fit_end, "its current holes", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index], "current time",  current_time
        #print "core",worker_index,"scheduler", scheduler_index," allocated core", worker_index,"for best fit times", best_fit_start, best_fit_end, "current time",  current_time, "when holes are", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
        self.worker_queues_history[worker_index][current_time].append([best_fit_start, best_fit_end, scheduler_index, has_collision])

    def update_worker_queues_free_time(self, worker_indices, best_fit_start, best_fit_end, current_time, delay, scheduler_index):
        global scheduler_collision_dist
        if best_fit_start >= best_fit_end:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start larger than or equal to best fit end')
        if current_time > best_fit_start:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start happened before insertion into core queues')
        # Cleanup stale holes
        for worker_index in worker_indices:
            while len(self.worker_queues_free_time_start[worker_index]) > 0:
                if current_time - 2*UPDATE_DELAY > self.worker_queues_free_time_end[worker_index][0]:
                    #Cull this hole. No point keeping it around now.
                    self.worker_queues_free_time_start[worker_index].pop(0)
                    self.worker_queues_free_time_end[worker_index].pop(0)
                else:
                    break
            history = self.worker_queues_history[worker_index]
            for history_time in history.keys():
                if current_time - 2*UPDATE_DELAY > history_time:
                    del history[history_time]

        #Ensure all workers are available at the best fit time.
        hole_indices = {}
        found_hole = False
        for worker_index in worker_indices:
            # Order : start_hole, best_fit_start, best_fit_end, end_hole
            # Find first start just less than or equal to best_fit_start
            for hole_index in range(len(self.worker_queues_free_time_start[worker_index])):
                start_hole = self.worker_queues_free_time_start[worker_index][hole_index]
                end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                if start_hole <= best_fit_start and end_hole >= best_fit_end:
                    hole_indices[worker_index] = hole_index
                    break

        if len(hole_indices.keys()) == len(worker_indices):
            found_hole = True

        # A real time update at worker should always find the hole.
        if found_hole == False and not delay:
            print "Could not find hole for worker(s)", str(worker_index), "for best fit times ", best_fit_start, best_fit_end
            raise AssertionError('No hole was found for best fit start and end at worker(s)')

        if found_hole:
            for worker_index in worker_indices:
                # Order : start_hole, best_fit_start, best_fit_end, end_hole
                # Find first start just less than or equal to best_fit_start
                #print "Core", worker_index, "holes - for best fit (",best_fit_start, best_fit_end,") is "
                hole_index = hole_indices[worker_index]
                start_hole = self.worker_queues_free_time_start[worker_index][hole_index]
                end_hole = self.worker_queues_free_time_end[worker_index][hole_index]
                #print "(",start_hole, end_hole,")"
                #print "Found hole ", start_hole, end_hole, "for serving best fit times", best_fit_start, best_fit_end
                self.worker_queues_free_time_start[worker_index].pop(hole_index)
                self.worker_queues_free_time_end[worker_index].pop(hole_index)
                if start_hole == best_fit_start and end_hole == best_fit_end:
                    self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index, False)
                    continue
                insert_position = hole_index
                if start_hole < best_fit_start:
                    self.worker_queues_free_time_start[worker_index].insert(insert_position, start_hole)
                    self.worker_queues_free_time_end[worker_index].insert(insert_position, best_fit_start)
                    insert_position += 1
                if end_hole > best_fit_end:
                    self.worker_queues_free_time_start[worker_index].insert(insert_position, best_fit_end)
                    self.worker_queues_free_time_end[worker_index].insert(insert_position, end_hole)
                #print "Updated holes at Core", worker_index," is ", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
                self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index, False)
                if len(self.worker_queues_free_time_start[worker_index]) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Lengths of start and end holes are unequal')
                if len(set(self.worker_queues_free_time_start[worker_index])) != len(self.worker_queues_free_time_start[worker_index]):
                    raise AssertionError('Duplicate entries found in start hole array')
                if len(set(self.worker_queues_free_time_end[worker_index])) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Duplicate entries found in end hole array')
            return best_fit_start, worker_indices, False
        #Some core(s) had a collision in this placement.
        #Update the scheduler placement and proceed towards resolution.
        for worker_index in worker_indices:
            self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index, True)

        task_duration = best_fit_end - best_fit_start
        cpu_req = len(worker_indices)
        machine_id = simulation.workers[worker_index].machine_id
        current_time += NETWORK_DELAY

        scheduler_collision_dist[scheduler_index] += 1
        #print "Collision detected at core", list(worker_indices)[0], "for best fit times ", best_fit_start, best_fit_end, "at current time", current_time, "at machine", machine_id, "for num cores = ", cpu_req, "new best fit start is",
        est_time, core_list = keeper.get_machine_est_wait(simulation.machines[machine_id].cores, cpu_req, current_time, task_duration, False, float('inf'), None)
        #This update happens at the worker, so schedulers don't yet know about it.
        best_fit_start, worker_indices, collision = keeper.update_worker_queues_free_time(core_list, est_time, est_time + task_duration, current_time, False, None)
        if collision != False:
            raise AssertionError('worker resolution returned a collision!')
        #print best_fit_start, "at cores", worker_indices

        return best_fit_start, worker_indices, True

#####################################################################################################################
#####################################################################################################################
# Support for multi-core machines in Murmuration
# Core = Worker class
class Machine(object):
    def __init__(self, num_cores, id, worker_id_start):
        self.num_cores = num_cores
        self.id = id

        #Role of a scheduler?
        self.scheduler = False
        if SYSTEM_SIMULATED == "Murmuration":
            if random.random() < RATIO_SCHEDULERS_TO_WORKERS:
                self.scheduler = True

        self.cores = []
        while len(self.cores) < self.num_cores:
            core_id = worker_id_start + len(self.cores)
            core = Worker(core_id, self.id)
            self.cores.append(core)

        # Dictionary of core and time when it was freed (used to track the time the core spent idle).
        self.free_cores = {}
        index = 0
        while index < self.num_cores:
            core_id = self.cores[index].id
            self.free_cores[core_id] = 0
            index += 1

        #Enqueued tasks at this machine
        self.queued_probes = PriorityQueue()

        #self.num_tasks_processed = 0

#####################################################################################################################
#####################################################################################################################
# This class denotes a single core on a machine.
class Worker(object):
    def __init__(self, id, machine_id):
        self.id = id
        self.machine_id = machine_id
        # Parameter to measure how long this worker is busy in the total run.
        self.busy_time = 0.0
#####################################################################################################################
#####################################################################################################################

class Simulation(object):
    def __init__(self, WORKLOAD_FILE, nr_workers):
        TOTAL_MACHINES = int(nr_workers)
        self.jobs = {}
        self.event_queue = PriorityQueue()
        self.workers = []
        self.machines = []
        self.scheduler_indices = []

        # Murmuration and Sparrow 
        while len(self.machines) < TOTAL_MACHINES:
            cores_needed = float('inf')
            if CORE_DISTRIBUTION == "STATIC":
                cores_needed = CORES_PER_MACHINE
            elif CORE_DISTRIBUTION == "GAUSSIAN":
                #Cap number of cores
                while cores_needed > 128 or cores_needed < 2:
                   cores_needed = pow(2,int(random.gauss(CORES_PER_MACHINE, CORE_DISTRIBUTION_DEVIATION)))
            machine = Machine(cores_needed, len(self.machines), len(self.workers))
            self.machines.append(machine)
            workers = machine.cores
            self.workers.extend(workers)
            if machine.scheduler:
                # Directly access scheduler indices in Simulation class
                self.scheduler_indices.append(machine.id)
                
        if SYSTEM_SIMULATED == "Murmuration":
            if len(self.scheduler_indices) == 0:
                scheduler_machine = random.choice(self.machines)
                self.scheduler_indices.append(scheduler_machine.id)
            print "Number of schedulers ", len(self.scheduler_indices)

        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False
        self.WORKLOAD_FILE = WORKLOAD_FILE

    #Simulation class
    def find_machines_random(self, probe_ratio, nr_tasks, possible_machine_indices, cores_requested):
        if possible_machine_indices == []:
            return []
        if len(cores_requested) != nr_tasks:
            raise AssertionError('List of cores requested is not the same as number of tasks')
        chosen_machine_indices = []
        nr_probes = (probe_ratio*nr_tasks)
        task_index = 0
        #print "Number of machines for Sparrow job", len(possible_machine_indices), "and num tasks", nr_tasks, "probe ratio", probe_ratio, "chose ", chosen_machine_indices
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
    # In Murmuration, workers are multi-core machines
    # Returns list of pair of machine and absolute core ids. [(m1, [c1, c2]), (m2, [c1, c2]),..]
    # Length of list of cores = corresponding cpu_req for the task
    # Ranking and update go hand-in-hand unlike in find_workers_long_job_prio() owing to different cpu requirements
    # for different tasks.
    # TODO: Other strategies - bulk allocation using well-fit, not best fit for tasks
    # Hole filling strategies, etc
    def find_machines_murmuration(self, job, current_time, scheduler_index):
        global num_collisions
        global time_elapsed_in_dc
        if job.num_tasks != len(job.cpu_reqs_by_tasks):
            raise AssertionError('Number of tasks provided not equal to length of cpu_reqs_by_tasks list')
        # best_fit_for_tasks = (ma, mb, .... )
        best_fit_for_tasks = set()
        best_fit_for_tasks_central = set()
        machines = self.machines
        delay = True if DECENTRALIZED else False
        should_finish_central = job.start_time
        wait_time = 0.0
        wait_time_central = 0.0
        to_print = False
        if job.id == 11 or job.id == 12 or job.id == 13:
            to_print = False 
        for task_index in range(job.num_tasks):
            best_fit_time = float('inf')
            chosen_machine = None
            cores_at_chosen_machine = None
            cpu_req = job.cpu_reqs_by_tasks[task_index]
            machines_not_yet_processed = range(TOTAL_MACHINES)
            '''
            print current_time, "Decentral - When processing task", task_index, "scheduler is", scheduler_index
            for temp_machine in range(TOTAL_MACHINES):
                print "For machine", temp_machine
                keeper.print_holes(temp_machine)
                print "Scheduler view is ", keeper.adjust_propagation_delay(temp_machine, current_time, True, scheduler_index)
                #keeper.print_history(temp_machine)
                if to_print:
                    print "Detailed explanation", keeper.adjust_propagation_delay(temp_machine, current_time, True, scheduler_index, True)
            '''

            while 1:
                if not machines_not_yet_processed:
                    break
                #machine_id = random.choice(machines_not_yet_processed)
                machine_id = machines_not_yet_processed[0]
                machines_not_yet_processed.remove(machine_id)
                est_time, core_list = keeper.get_machine_est_wait(self.machines[machine_id].cores, cpu_req, current_time, job.actual_task_duration[task_index], delay, best_fit_time, scheduler_index)
                if est_time < best_fit_time:
                    best_fit_time = est_time
                    chosen_machine = machine_id
                    cores_at_chosen_machine = core_list
                '''    
                elif est_time == best_fit_time:
                    new_choice = random.choice([machine_id, chosen_machine])
                    if new_choice == machine_id:
                        chosen_machine = machine_id
                        cores_at_chosen_machine = core_list
                '''    
                if best_fit_time == int(ceil(current_time)):
                    #Can't do any better
                    break
            if best_fit_time == float('inf') or chosen_machine == None or cores_at_chosen_machine == None:
                raise AssertionError('Error - Got best fit time that is infinite!')
            if len(cores_at_chosen_machine) != cpu_req:
                raise AssertionError("Not enough machines that pass filter requirement of job")
            #print "Job id",job.id,"Choosing machine", chosen_machine,":[",cores_at_chosen_machine,"] with best fit time ",best_fit_time,"for task #", task_index, " task_duration", job.actual_task_duration[task_index]," arrival time ", current_time, "requesting", cpu_req, "cores"
            best_fit_for_tasks.add(chosen_machine)
            #self.machines[chosen_machine].num_tasks_processed += 1

            #Update est time at this machine and its cores
            #print "Picked machine", chosen_machine," for job", job.id,"task", task_index, "duration",job.actual_task_duration[task_index],"with best fit scheduler view", best_fit_time,
            best_fit_time, cores_at_chosen_machine, has_collision = keeper.update_worker_queues_free_time(cores_at_chosen_machine, best_fit_time, best_fit_time + int(ceil(job.actual_task_duration[task_index])), current_time, delay, scheduler_index)
            #if has_collision:
            #    print "Adjusted after collision to", best_fit_time,
            #print "finishes at", best_fit_time + int(ceil(job.actual_task_duration[task_index]))    
            job.should_finish_time = max(job.should_finish_time, best_fit_time + int(ceil(job.actual_task_duration[task_index])))
            if has_collision:
                num_collisions += 1
            
            '''
            '''
            '''
            best_fit_time1 = float('inf')
            chosen_machine1 = None
            cores_at_chosen_machine1 = None
            machines_not_yet_processed1 = range(TOTAL_MACHINES)
            while 1:
                if not machines_not_yet_processed1:
                    break
                machine_id1 = random.choice(machines_not_yet_processed1)
                machines_not_yet_processed1.remove(machine_id1)
                est_time1, core_list1 = keeper_central.get_machine_est_wait(self.machines[machine_id1].cores, cpu_req, current_time, job.actual_task_duration[task_index], False, best_fit_time1, scheduler_index)
                if est_time1 < best_fit_time1:
                    best_fit_time1 = est_time1
                    chosen_machine1 = machine_id1
                    cores_at_chosen_machine1 = core_list1
                if best_fit_time1 == int(ceil(current_time)):
                    #Can't do any better
                    break
            if best_fit_time1 == float('inf') or chosen_machine1 == None or cores_at_chosen_machine1 == None:
                raise AssertionError('Error - Got best fit time that is infinite!')
            if len(cores_at_chosen_machine1) != cpu_req:
                raise AssertionError("Not enough machines that pass filter requirement of job")

            best_fit_for_tasks_central.add(chosen_machine1)
            #self.machines[chosen_machine1].num_tasks_processed += 1
            #Update est time at this machine and its cores
            best_fit_time1, cores_at_chosen_machine1, has_collision = keeper_central.update_worker_queues_free_time(cores_at_chosen_machine1, best_fit_time1, best_fit_time1 + int(ceil(job.actual_task_duration[task_index])), current_time, False, scheduler_index)
            should_finish_central = max(should_finish_central, best_fit_time1 + int(ceil(job.actual_task_duration[task_index])))
            #if chosen_machine1 != chosen_machine:
            #    print "Central chose machine", chosen_machine1
            '''
            '''
            '''
            prev_wait = wait_time
            wait_time = best_fit_time - job.start_time
            #print >> finished_file, "Task", task_index,"task_duration", job.actual_task_duration[task_index],"wait_time", wait_time
            #if prev_wait > wait_time:
            #    print "Anomaly - previous wait time", prev_wait,"is larger than current wait time", wait_time
            #prev_wait_central = wait_time_central
            #wait_time_central = best_fit_time1 - job.start_time
            #print >> finished_file1, "Task",task_index,"task_duration", job.actual_task_duration[task_index],"wait_time", wait_time_central
        '''
        print "End of job",job.id,"by scheduler", scheduler_index,". Updated machine holes at decentral"
        for temp_machine in range(TOTAL_MACHINES):
            keeper.print_holes(temp_machine)
        if job.id == 3:    
            print "Scheduler view is ", keeper.adjust_propagation_delay(0, current_time, True, scheduler_index)
        print "Updated machine holes at central"
        for temp_machine in range(TOTAL_MACHINES):
            keeper_central.print_holes(temp_machine)
        print "Decentral",job.should_finish_time, " total_job_running_time: ",(job.should_finish_time - job.start_time), " job_id", job.id
        print "Central",should_finish_central, " total_job_running_time: ",(should_finish_central - job.start_time), " job_id", job.id
        '''
        print >> finished_file, job.should_finish_time, " total_job_running_time: ",(job.should_finish_time - job.start_time), " job_id", job.id
        #print >> finished_file1, should_finish_central, " total_job_running_time: ",(should_finish_central - job.start_time), " job_id", job.id
        time_elapsed_in_dc = max(time_elapsed_in_dc, job.should_finish_time)
        '''
            if job.id == 7: 
                print "----JOB", job.id,"----"
                print "current time", current_time, "Job id", job.id, "task index", task_index, "duration", job.actual_task_duration[task_index], "scheduler index", scheduler_index
                print "Wait times - central", wait_time_central, "decentral", wait_time
                print "Best fits central",should_finish_central,"machines",best_fit_for_tasks_central,"and delayed", job.should_finish_time,"machine",best_fit_for_tasks
                sum_central = 0
                sum_decentral = 0
                for i in range(TOTAL_MACHINES):
                    print "Core", i
                    print "------------------"
                    l1 = len(keeper_central.worker_queues_free_time_end[i])
                    l2 = len(keeper.worker_queues_free_time_end[i])
                    if keeper_central.worker_queues_free_time_start[i][l1-1] > keeper.worker_queues_free_time_start[i][l2-1]:
                        print "(Central has a LARGER end hole here)"
                    sum_central += keeper_central.worker_queues_free_time_start[i][l1-1]
                    sum_decentral += keeper.worker_queues_free_time_start[i][l2-1]
                    print "---------Debugging central---------"
                    keeper_central.print_holes(i)
                    keeper_central.print_history(i)
                    print "Scheduler view is ", keeper_central.adjust_propagation_delay(i, current_time, False, scheduler_index)
                    print "---------Debugging delayed---------"
                    keeper.print_holes(i)
                    keeper.print_history(i)
                    print "Scheduler view is ", keeper.adjust_propagation_delay(i, current_time, delay, scheduler_index)
                    print "------------------"
                print "Sums are", sum_central, sum_decentral
                raise AssertionError('debug')
        '''

        '''
        #if best_fit_time1 > best_fit_time:
        if len(best_fit_for_tasks_central) > 1:
            print "Central",job.id, best_fit_for_tasks_central
            #raise AssertionError('yes central')
        if len(best_fit_for_tasks_central) > 1:
            print "Decentral",job.id, best_fit_for_tasks
            #raise AssertionError('yes decentral')
        ''' 
        ''' 
        print "----JOB", job.id,"----"
        print "current time", current_time, "Job id", job.id, "scheduler index", scheduler_index
        print "Best fits central",should_finish_central,"machines",best_fit_for_tasks_central,"and delayed", job.should_finish_time,"machine",best_fit_for_tasks
        sum_central = 0
        sum_decentral = 0
        for i in range(TOTAL_MACHINES):
            print "Core", i
            print "------------------"
            l1 = len(keeper_central.worker_queues_free_time_end[i])
            l2 = len(keeper.worker_queues_free_time_end[i])
            if keeper_central.worker_queues_free_time_start[i][l1-1] > keeper.worker_queues_free_time_start[i][l2-1]:
                print "(Central has a LARGER end hole here)"
            sum_central += keeper_central.worker_queues_free_time_start[i][l1-1]
            sum_decentral += keeper.worker_queues_free_time_start[i][l2-1]
            print "---------Debugging central---------"
            keeper_central.print_holes(i)
            keeper_central.print_history(i)
            print "Scheduler view is ", keeper_central.adjust_propagation_delay(i, current_time, False, scheduler_index)
            print "---------Debugging delayed---------"
            keeper.print_holes(i)
            keeper.print_history(i)
            print "Scheduler view is ", keeper.adjust_propagation_delay(i, current_time, delay, scheduler_index)
            print "------------------"
            print "Sums are", sum_central, sum_decentral
        ''' 

        #return best_fit_for_tasks
        return []

    #Simulation class
    def send_probes(self, job, current_time, worker_indices):
        if SYSTEM_SIMULATED == "Sparrow":
            return self.send_machine_probes_sparrow(job, current_time, worker_indices)
        #Murmuration
        return self.send_tasks_murmuration(job, current_time, worker_indices)

    #Simulation class
    def send_machine_probes_sparrow(self, job, current_time, machine_indices):
        self.jobs[job.id] = job
        task_arrival_events = []
        machine_ids = set()
        current_time += NETWORK_DELAY
        for index in range(len(machine_indices)):
            machine_id = machine_indices[index]
            task_index = index / PROBE_RATIO
            # The exact cores are a matter of availability at the machine.
            machine_ids.add(machine_id)
            self.machines[machine_id].add_machine_probe(current_time, [[], job.id, task_index, current_time])

        for machine_id in machine_ids:
            task_arrival_events.append((current_time, ProbeEventForMachines(self.machines[machine_id])))

        return task_arrival_events

    # Simulation class
    def send_tasks_murmuration(self, job, current_time, scheduler_indices):
        global scheduler_task_dist
        self.jobs[job.id] = job
        # Some safety checks
        if len(scheduler_indices) != 1:
            raise AssertionError('Murmuration received more than one scheduler for the job?')
        # scheduler_index denotes the exactly one scheduler node ID where this job request lands.
        scheduler_index = scheduler_indices[0]
        scheduler_task_dist[scheduler_index] += 1
        #.append(job.estimated_task_duration)

        # Sort all workers running long jobs in this DC according to their estimated times.
        # Ranking policy used - Least estimated time and hole duration > estimted task time.
        # Find machines for tasks and trigger events.
        # Returns a set of machines to service tasks of the job - (m1, m2, ...).
        # May be less than the number of tasks due to same machines hosting more than one task.
        self.find_machines_murmuration(job, current_time, scheduler_index)
        return []

    #Simulation class
    def run(self):
        global utilization
        global time_elapsed_in_dc 
        global total_busyness
        global start_time_in_dc
        global drift
        last_time = 0

        self.jobs_file = open(self.WORKLOAD_FILE, 'r')
        line = self.jobs_file.readline()
        start_time_in_dc = float(line.split()[0])

        new_job = Job(line)
        self.event_queue.put((float(line.split()[0]), JobArrival(new_job, self.jobs_file)))
        self.jobs_scheduled = 1

        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            if current_time < last_time:
                raise AssertionError("Got current time "+ str(current_time)+" less than last time "+ str(last_time))
            last_time = current_time
            last_event = event
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        #for machine in self.machines:
        #    print >> load_file, machine.id, machine.num_tasks_processed
        #Free up all memory
        del self.machines[:]
        del self.scheduler_indices[:]
        num_workers = len(self.workers)
        for worker in self.workers:
            total_busyness += worker.busy_time
        del self.workers[:]
        print "Simulation ending, no more events. Jobs completed", self.jobs_completed
        print "Drift is", drift
        self.jobs_file.close()

        # Calculate utilizations of worker machines in DC
        time_elapsed_in_dc = current_time - start_time_in_dc
        print "Total time elapsed in the DC is", time_elapsed_in_dc, "s"
        utilization = 100 * (float(total_busyness) / float(time_elapsed_in_dc * num_workers))

#####################################################################################################################
#globals
utilization = 0.0
time_elapsed_in_dc = 0.0
total_busyness = 0.0
start_time_in_dc = 0.0
num_collisions = 0

#0.5ms delay on each link.
NETWORK_DELAY = 0.0005
job_count = 1
#Randomize the run.
random.seed(123456789)

if(len(sys.argv) != 11):
    print "Incorrect number of parameters."
    sys.exit(1)

WORKLOAD_FILE                   = sys.argv[1]
CORES_PER_MACHINE               = int(sys.argv[2])
PROBE_RATIO                     = int(sys.argv[3])
TOTAL_MACHINES                  = int(sys.argv[4])
SYSTEM_SIMULATED                = sys.argv[5]
POLICY                          = sys.argv[6]                      #RANDOM, BEST_FIT, WORST_FIT
DECENTRALIZED                   = (sys.argv[7] == "DECENTRALIZED") #CENTRALIZED, DECENTRALIZED
CORE_DISTRIBUTION               = sys.argv[8]                      #STATIC, GAUSSIAN ON CORES_PER_MACHINE
CORE_DISTRIBUTION_DEVIATION     = 2                                #if CORE_DISTRIBUTION == "GAUSSIAN"
RATIO_SCHEDULERS_TO_WORKERS     = float(sys.argv[9])
UPDATE_DELAY                    = float(sys.argv[10])              #in seconds

if RATIO_SCHEDULERS_TO_WORKERS > 1:
    print "Scheduler to Cores ratio cannot exceed 1"
    sys.exit(1)

if not DECENTRALIZED:
    UPDATE_DELAY = 0

#log_file is used for logging information on individual jobs, for JCT to be calculated later.
file_name = ['finished_file', sys.argv[2], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[9],sys.argv[10]]
separator = '_'
log_file = (separator.join(file_name))
finished_file   = open(log_file, 'w')

file_name = ['finished_file', sys.argv[2], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[9],sys.argv[10],"temp"]
separator = '_'
log_file = (separator.join(file_name))
finished_file1   = open(log_file, 'w')

#file_name = ['finished_file', sys.argv[2], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[9],sys.argv[10], 'load']
#load_file_name = separator.join(file_name)
#load_file = open(load_file_name,'w')

scheduler_task_dist = defaultdict(int)
scheduler_collision_dist = defaultdict(int)

drift = 0.0
t1 = time()
simulation = Simulation(WORKLOAD_FILE, TOTAL_MACHINES)
num_workers = len(simulation.workers)
keeper = ClusterStatusKeeper(num_workers)
keeper_central = ClusterStatusKeeperCentral(num_workers)
simulation.run()

simulation_time = (time() - t1)
print "Simulation ended in ", simulation_time, " s "
print "Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS,"total collisions", num_collisions
print >> finished_file, "(Multi core) Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS, "total collisions", num_collisions


finished_file.close()
finished_file1.close()
'''
print scheduler_task_dist
print "Task dist in schedulers"
for scheduler_index in scheduler_task_dist.keys():
    scheduler_task_dist[scheduler_index].sort()
    print "For scheduler", scheduler_index,
    print "50th -",
    print np.percentile(scheduler_task_dist[scheduler_index], 50),
    print "90th -",
    print np.percentile(scheduler_task_dist[scheduler_index], 90),
    print "99th -",
    print np.percentile(scheduler_task_dist[scheduler_index], 99)
'''


print "Task and Collision dist in schedulers"
for scheduler_index in scheduler_task_dist.keys():
    print "For scheduler", scheduler_index, "num tasks scheduled",scheduler_task_dist[scheduler_index], "collisions faced", scheduler_collision_dist[scheduler_index] 
'''
for scheduler_index in scheduler_task_dist.keys():
    scheduler_task_dist[scheduler_index].sort()
    scheduler_distb = scheduler_task_dist[scheduler_index]
    plt.bar(range(len(scheduler_distb)), scheduler_distb, color = 'g', label = 'Schedulers and distribution of tasks')
    plt.xlabel('Jobs', fontsize = 12)
    plt.ylabel('Estimated Task Durations', fontsize = 12)
    plt.title('Schedulers and distribution of tasks', fontsize = 20)
    plt.legend()
    plt.show()
'''


#load_file.close()
# Generate CDF data
import os; os.system("python process.py " + log_file + " " + SYSTEM_SIMULATED + " " + WORKLOAD_FILE + " " + str(TOTAL_MACHINES)); #os.remove(log_file)
#import os; os.system("python  load_murmuration.py " + load_file_name) ; #os.remove(load_file_name)
