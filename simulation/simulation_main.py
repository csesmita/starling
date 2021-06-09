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
# Support for multi-core task request
class ProbeEventForMachines(Event):
    def __init__(self, machine):
        self.machine = machine

    def run(self, current_time):
        return self.machine.try_process_next_probe_murmuration(current_time)

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
        print self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]

    def adjust_propagation_delay(self, core_id, current_time, delay, new_scheduler_index):
        if not delay:
            return self.worker_queues_free_time_start[core_id], self.worker_queues_free_time_end[core_id]
        #Adjustment for DECENTRALIZED system
        time_limit = current_time - UPDATE_DELAY
        copied = False
        time_start = (self.worker_queues_free_time_start[core_id])
        time_end = (self.worker_queues_free_time_end[core_id])
        for history_list in sorted(self.worker_queues_history[core_id].items(), reverse=True):
            history_time = history_list[0]
            history_hole = history_list[1]
            if history_time < time_limit:
                continue
            scheduler_index = history_hole[2]
            if scheduler_index is not None and scheduler_index == new_scheduler_index:
                #The same scheduler had placed this history placement, so it knows about it.
                continue
            for hole_index in range(len(time_start)):
                if time_end[hole_index] < abs(history_hole[0]):
                    continue
                if history_hole[0] >= 0 and history_hole[1] >= 0:
                    history_start_hole = history_hole[0]
                    history_end_hole = history_hole[1]
                    #New tasks assigned reflect as positive holes.
                    #History hole cannot start in a currently existing hole,
                    #else it would've been captured in history.
                    if time_start[hole_index] <= history_start_hole and history_start_hole < time_end[hole_index]:
                        #This might be the case if an update was sent by the worker. So, worker's update was applied.
                        continue
                    if not copied:
                        time_start = deepcopy(self.worker_queues_free_time_start[core_id])
                        time_end = deepcopy(self.worker_queues_free_time_end[core_id])
                        copied = True
                    #Here, time_start > history_start_hole or time_end == history_start_hole
                    if time_start[hole_index] > history_start_hole:
                        if time_start[hole_index] == history_end_hole:
                            time_start.pop(hole_index)
                            time_start.insert(hole_index, history_start_hole)
                        else:
                            time_start.insert(hole_index, history_start_hole)
                            time_end.insert(hole_index, history_end_hole)
                    if time_end[hole_index] == history_start_hole:
                        time_end.pop(hole_index)
                        time_end.insert(hole_index, history_end_hole)
                    #Collapse holes if need be
                    if (hole_index < len(time_end) - 1) and time_end[hole_index] == time_start[hole_index + 1]:
                        time_end.pop(hole_index)
                        time_start.pop(hole_index + 1)
                    break
                #Holes are never updated when tasks leave. Neither current, nor history.
                #Therefore, they will show up as occupied.
        return time_start, time_end

    # ClusterStatusKeeper class
    # get_machine_est_wait() -
    # Returns per task estimated wait time and list of cores that can accommodate that task at that estimated time.
    # Returns - ([est_task1, est_task2, ...],[[list of cores],[list of core], [],...])
    # Might return smaller values for smaller cpu req, even if those requests come in later.
    # Also, converts holes to ints except the last entry in end which is float('inf')
    def get_machine_est_wait(self, cores, cpu_req, arrival_time, task_duration, delay, best_current_time, scheduler_index):
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
        max_time_start = best_current_time
        for core in cores:
            core_id = core.id
            time_start, time_end = self.adjust_propagation_delay(core_id, arrival_time, delay, scheduler_index)
            #print "For core", core_id,"got holes - ", time_start, time_end
            if len(time_end) != len(time_start):
                raise AssertionError('Error in get_machine_est_wait - mismatch in lengths of start and end hole arrays')
            for hole in range(len(time_end)):
                if time_start[hole] >= time_end[hole]:
                    print "Error in get_machine_est_wait - start of hole is equal or larger than end of hole"
                    print "Core index", core_id, "hole id ", hole, "start is ", time_start[hole], "end is ", time_end[hole]
                    print "Holes are", time_start, time_end
                    raise AssertionError('Error in get_machine_est_wait - start of hole is equal or larger than end of hole')
                #Be done if time exceeds the present best fit
                if time_start[hole] > max_time_start:
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
                    end = min(end, max_time_start)
                    #time granularity of 1.
                    arr = range(start, end, 1)
                    for start_chunk in arr:
                        #print "[t=",arrival_time,"] For core ", core_id," fitting task of duration", task_duration,"into hole = ", time_start[hole], time_end[hole], "starting at", start_chunk
                        all_slots_list_add(start_chunk)
                        all_slots_list_cores[start_chunk].add(core_id)
                        all_slots_fragmentation[start_chunk][core_id] = max(start_chunk - start, time_end[hole] - start_chunk - task_duration)
                        if max_time_start > start_chunk and len(all_slots_list_cores[start_chunk]) >= cpu_req:
                            max_time_start = start_chunk
                            break
                else:
                    all_slots_list_add(start)
                    all_slots_list_cores[start].add(core_id)
                    all_slots_fragmentation[start][core_id] = 0
                    inf_hole_start[core_id] = start
                    if max_time_start > start and len(all_slots_list_cores[start]) >= cpu_req:
                        max_time_start = start
                        break

        all_slots_list = sorted(all_slots_list)
        for core, inf_start in inf_hole_start.items():
            for start in all_slots_list:
                if start > inf_start:
                    all_slots_list_cores[start].add(core)
                    all_slots_fragmentation[start][core] = start - inf_start

        #print "Available time slots are - ", all_slots_list
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
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=True)[0:cpu_req]
                    elif POLICY == "BEST_FIT":
                        #Select cores with smallest available hole after allocation
                        sorted_cores_fragmented = sorted(cores_fragmented.items(), key=itemgetter(1), reverse=False)[0:cpu_req]
                    else:
                        raise AssertionError('Check the name of the policy. Should be RANDOM, WORST_FIT OR BEST_FIT')
                    cores_list = set(dict(sorted_cores_fragmented).keys())
                #print "Earliest start time for task duration", task_duration,"needing", cpu_req,"cores is ", start_time, "with cores", cores_list 
                # cpu_req is available when the fastest cpu_req number of cores is
                # available for use at or after arrival_time.
                return (start_time, cores_list)
        return (float('inf'), [])

    def update_history_holes(self, worker_index, current_time, best_fit_start, best_fit_end, task_is_leaving, scheduler_index):
        if task_is_leaving:
            best_fit_start = -1 * best_fit_start
            best_fit_end   = -1 * best_fit_end
        #print "Updating history hole - Core", worker_index,"best fit times for task that just got allocated", best_fit_start, best_fit_end, "its current holes", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index], "current time",  current_time
        self.worker_queues_history[worker_index][current_time] = [best_fit_start, best_fit_end, scheduler_index]

    def update_worker_queues_free_time(self, worker_indices, best_fit_start, best_fit_end, current_time, delay, scheduler_index):
        if best_fit_start >= best_fit_end:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start larger than or equal to best fit end')
        if current_time > best_fit_start:
            raise AssertionError('Error in update_worker_queues_free_time - best fit start happened before insertion into core queues')

        # Cleanup stale holes
        for worker_index in worker_indices:
            while len(self.worker_queues_free_time_start[worker_index]) > 0:
                if current_time > self.worker_queues_free_time_end[worker_index][0]:
                    #Cull this hole. No point keeping it around now.
                    self.worker_queues_free_time_start[worker_index].pop(0)
                    self.worker_queues_free_time_end[worker_index].pop(0)
                else:
                    break
            history = self.worker_queues_history[worker_index]
            for history_time in history.keys():
                if current_time - UPDATE_DELAY > history_time:
                    del history[history_time]

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
                        #print "Updated holes at Core", worker_index," is ", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index]
                        self.update_history_holes(worker_index, current_time, best_fit_start, best_fit_end, False, scheduler_index)
                        break

                # A real time update at worker should always find the hole.
                if found_hole == False and not delay:
                    print "Could not find hole for worker", str(worker_index), "for best fit times ", best_fit_start, best_fit_end, " its holes being", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index], "and history", self.worker_queues_history[worker_index]
                    raise AssertionError('No hole was found for best fit start and end at worker ' + str(worker_index))
                elif found_hole == False:
                    task_duration = best_fit_end - best_fit_start
                    cpu_req = len(worker_indices)
                    machine_id = simulation.workers[worker_index].machine_id
                    current_time += NETWORK_DELAY
                    #print "Collision detected at worker", str(worker_index), "for best fit times ", best_fit_start, best_fit_end, " its holes being", self.worker_queues_free_time_start[worker_index], self.worker_queues_free_time_end[worker_index], "and history", self.worker_queues_history[worker_index],"at current time", current_time, "at machine", machine_id, "for num cores = ", cpu_req
                    est_time, core_list = keeper.get_machine_est_wait(simulation.machines[machine_id].cores, cpu_req, current_time, task_duration, False, float('inf'), None)
                    #This update happens at the worker, so schedulers don't yet know about it.
                    best_fit_start, worker_indices = keeper.update_worker_queues_free_time(core_list, est_time, est_time + task_duration, current_time, False, None)
                    break

                if len(self.worker_queues_free_time_start[worker_index]) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Lengths of start and end holes are unequal')
                if len(set(self.worker_queues_free_time_start[worker_index])) != len(self.worker_queues_free_time_start[worker_index]):
                    raise AssertionError('Duplicate entries found in start hole array')
                if len(set(self.worker_queues_free_time_end[worker_index])) != len(self.worker_queues_free_time_end[worker_index]):
                    raise AssertionError('Duplicate entries found in end hole array')
        return best_fit_start, worker_indices

#####################################################################################################################
#####################################################################################################################
class TaskEndEventForMachines(object):
    def __init__(self, worker_index, task_duration):
        self.worker_index = worker_index
        self.task_duration = task_duration

    def run(self, current_time):
        global start_time_in_dc
        worker = simulation.workers[self.worker_index]
        worker.busy_time += self.task_duration
        # Task leaving is an event registered at the worker. None of the schedulers know about it, till the worker updates.
        keeper.update_history_holes(self.worker_index, current_time, int(ceil(current_time - self.task_duration)), int(ceil(current_time)), True, None)
        #keeper.update_history_holes(self.worker_index, current_time, (current_time - self.task_duration), (current_time), True, None)

        total_busyness = 0.0
        num_workers = len(simulation.workers)
        for temp_worker in simulation.workers:
            total_busyness += temp_worker.busy_time
        # Calculate utilizations of worker machines in DC
        time_elapsed_in_dc = current_time - start_time_in_dc
        utilization = 100 * (float(total_busyness) / float(time_elapsed_in_dc * num_workers))
        #print "Task duration", self.task_duration,"Average utilization with ",num_workers, "total workers is", utilization, "(total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness

        return worker.free_slot(current_time)


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

        self.num_tasks_processed = 0

    #Machine class
    def add_machine_probe(self, best_fit_time, probe_params):
        self.queued_probes.put((best_fit_time, probe_params))

    #Machine class
    def free_machine_core(self, core, current_time):
        self.free_cores[core.id] = current_time
        return self.try_process_next_probe_murmuration(current_time)

    #Machine class
    #Assumes best fit time is always unique across tasks on the same (machine, cores).
    #Pop best fitting tasks across different cores.
    #Check which has the fastest completion time and process that task.
    #Re-insert the rest of the tasks back into machine queue.
    #This ensures TaskEndEvents, and hence DC time, are monotonically increasing functions.
    def try_process_next_probe_murmuration(self, current_time):
        if SYSTEM_SIMULATED == "Sparrow":
            return self.try_process_next_probe_sparrow(current_time)
        events = []
        #Candidates list among all possible tasks that can execute with current free cores.
        earliest_task_completion_time = float('inf')
        candidate_best_fit_time = 0.0
        candidate_processing_time = 0.0
        candidate_task_info = None
        candidate_cores = {}
        candidate_probes_covered = PriorityQueue()
        while 1:
            if self.queued_probes.empty() or len(self.free_cores) == 0:
                #Nothing to execute, or nowhere to execute
                break
            #print current_time, ":This iteration free cores are ", self.free_cores.keys()
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
                          candidate_cores[core_id] = self.free_cores[core_id]
                          del self.free_cores[core_id]
                #print "Best fit time", best_fit_time, "Task info", task_info, "reason - cores not yet free"
                continue

            core_available_time = 0
            for core_id in core_indices:
                time = self.free_cores[core_id]
                if core_available_time < time:
                    core_available_time = time
                candidate_cores[core_id] = self.free_cores[core_id]
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
                    #print "Best fit time", candidate_best_fit_time, "Task info", candidate_task_info, "reason - got a task with earlier finish"
                earliest_task_completion_time = task_completion_time
                candidate_best_fit_time = best_fit_time
                candidate_task_info = task_info
                candidate_processing_time = processing_start_time
            else:
                #Not our best candidate
                candidate_probes_covered.put((best_fit_time, task_info))
                #print "Best fit time", best_fit_time, "Task info", task_info, "reason - not the best candidate"

        core_indices = []
        if earliest_task_completion_time == float('inf'):
            #Reinsert all free cores other than the ones needed
            for core_index in candidate_cores.keys():
                if core_index not in core_indices:
                    self.free_cores[core_index] = candidate_cores[core_index]

            #Reinsert all queued probes that were inspected
            while not candidate_probes_covered.empty():
                best_fit_time, task_info = candidate_probes_covered.get()
                self.queued_probes.put((best_fit_time, task_info))
            return []

        # Extract all information
        core_indices = candidate_task_info[0]
        job_id = candidate_task_info[1]
        job = simulation.jobs[job_id]
        if len(job.unscheduled_tasks) <= 0:
            raise AssertionError('No redundant probes in Murmuration, yet tasks have finished?')
        task_index = candidate_task_info[2]
        probe_arrival_time = candidate_task_info[3]

        task_actual_duration = job.actual_task_duration[task_index]
        #print current_time,": Got candidate best fit time", candidate_best_fit_time, "Task duration", task_actual_duration, "Cores", core_indices, "due to finish at ", earliest_task_completion_time

        if earliest_task_completion_time < current_time:
            print current_time,": Got candidate best fit time", candidate_best_fit_time, "Task duration", task_actual_duration, "Cores", core_indices, "due to finish at ", earliest_task_completion_time
            raise AssertionError('Unprocessed task with completion time before current time.')

        #Reinsert all free cores other than the ones needed
        for core_index in candidate_cores.keys():
            if core_index not in core_indices:
                self.free_cores[core_index] = candidate_cores[core_index]

        #Reinsert all queued probes that were inspected
        while not candidate_probes_covered.empty():
            best_fit_time, task_info = candidate_probes_covered.get()
            self.queued_probes.put((best_fit_time, task_info))

        # Finally, process the current task with all these parameters
        task_status, new_events = simulation.process_machine_task(self, core_indices, job_id, task_index, task_actual_duration, candidate_processing_time, probe_arrival_time)
        for new_event in new_events:
            events.append((new_event[0], new_event[1]))

        return events


    # Machine class
    def try_process_next_probe_sparrow(self, current_time):
        events = []
        while 1:
            if self.queued_probes.empty() or len(self.free_cores) == 0:
                #Nothing to execute, or nowhere to execute
                break

            #First of the queued tasks
            time, task_info = self.queued_probes.get()

            # Extract all information
            core_indices = task_info[0]
            if len(core_indices) != 0:
                raise AssertionError('Sparrow coming with pre-assigned cores?')
            job_id = task_info[1]
            if not job_id in simulation.jobs.keys():
                continue
            job = simulation.jobs[job_id]
            task_index = task_info[2]
            probe_arrival_time = task_info[3]
            task_actual_duration = job.actual_task_duration[task_index]
            task_cpu_request = job.cpu_reqs_by_tasks[task_index]

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
            task_status, new_events = simulation.process_machine_task(self, core_indices, job_id, task_index, task_actual_duration, current_time, probe_arrival_time)
            for new_event in new_events:
                events.append((new_event[0], new_event[1]))
            break

        return events

#####################################################################################################################
#####################################################################################################################
# This class denotes a single core on a machine.
class Worker(object):
    def __init__(self, id, machine_id):
        self.id = id
        self.machine_id = machine_id
        # Parameter to measure how long this worker is busy in the total run.
        self.busy_time = 0.0

    #Worker class
    # In Murmuration, free_slot will report the same to the machine class.
    def free_slot(self, current_time):
        machine = simulation.machines[self.machine_id]
        return machine.free_machine_core(self, current_time)

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
            print "Number of schedulers ", len(self.scheduler_indices)

        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False
        self.WORKLOAD_FILE = WORKLOAD_FILE

    #Simulation class
    def find_machines_random(self, probe_ratio, nr_tasks, possible_machine_indices, cores_requested):
        if possible_machine_indices == []:
            return []
        assert len(cores_requested) == nr_tasks
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
        if job.num_tasks != len(job.cpu_reqs_by_tasks):
            raise AssertionError('Number of tasks provided not equal to length of cpu_reqs_by_tasks list')
        # best_fit_for_tasks = (ma, mb, .... )
        best_fit_for_tasks = set()
        machines = self.machines
        delay = True if DECENTRALIZED else False
        for task_index in range(job.num_tasks):
            best_fit_time = float('inf')
            chosen_machine = None
            cores_at_chosen_machine = None
            cpu_req = job.cpu_reqs_by_tasks[task_index]
            machines_not_yet_processed = range(TOTAL_MACHINES)
            while 1:
                if not machines_not_yet_processed:
                    break
                machine_id = random.choice(machines_not_yet_processed)
                machines_not_yet_processed.remove(machine_id)
                est_time, core_list = keeper.get_machine_est_wait(self.machines[machine_id].cores, cpu_req, current_time, job.actual_task_duration[task_index], delay, best_fit_time, scheduler_index)
                if est_time < best_fit_time:
                    best_fit_time = est_time
                    chosen_machine = machine_id
                    cores_at_chosen_machine = core_list
                if best_fit_time == int(ceil(current_time)):
                    #Can't do any better
                    break
            if best_fit_time == float('inf') or chosen_machine == None or cores_at_chosen_machine == None:
                raise AssertionError('Error - Got best fit time that is infinite!')
            if len(cores_at_chosen_machine) != cpu_req:
                raise AssertionError("Not enough machines that pass filter requirement of job")
            #print "Job id",job.id,"Choosing machine", chosen_machine,":[",cores_at_chosen_machine,"] with best fit time ",best_fit_time,"for task #", task_index, " task_duration", job.actual_task_duration[task_index]," arrival time ", current_time, "requesting", cpu_req, "cores", "core 24 has current holes - ", keeper.worker_queues_free_time_start[24], keeper.worker_queues_free_time_end[24]
            best_fit_for_tasks.add(chosen_machine)

            #Update est time at this machine and its cores
            best_fit_time, cores_at_chosen_machine = keeper.update_worker_queues_free_time(cores_at_chosen_machine, best_fit_time, best_fit_time + int(ceil(job.actual_task_duration[task_index])), current_time, delay, scheduler_index)
            probe_params = [cores_at_chosen_machine, job.id, task_index, current_time]
            self.machines[chosen_machine].add_machine_probe(best_fit_time, probe_params)
        return best_fit_for_tasks

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
        nr_probes = (PROBE_RATIO * job.num_tasks)
        assert nr_probes == len(machine_indices)
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
        self.jobs[job.id] = job
        task_arrival_events = []
 
        # Some safety checks
        if len(scheduler_indices) != 1:
            raise AssertionError('Murmuration received more than one scheduler for the job?')
        # scheduler_index denotes the exactly one scheduler node ID where this job request lands.
        scheduler_index = scheduler_indices[0]

        # Sort all workers running long jobs in this DC according to their estimated times.
        # Ranking policy used - Least estimated time and hole duration > estimted task time.
        # Find machines for tasks and trigger events.
        # Returns a set of machines to service tasks of the job - (m1, m2, ...).
        # May be less than the number of tasks due to same machines hosting more than one task.
        machine_indices = self.find_machines_murmuration(job, current_time, scheduler_index)
        # Return task arrival events for long jobs
        for machine_id in machine_indices:
            task_arrival_events.append((current_time, ProbeEventForMachines(self.machines[machine_id])))
        return task_arrival_events

    #Simulation class
    def process_machine_task(self, machine, core_indices, job_id, task_index, task_duration, current_time, probe_arrival_time):
        job = self.jobs[job_id]
        task_wait_time = current_time - probe_arrival_time
        scheduler_algorithm_time = probe_arrival_time - job.start_time

        #Collect load statistics
        machine.num_tasks_processed += 1

        events = []
        job.unscheduled_tasks.remove(task_duration)
        # Machine busy time should be a sum of worker busy times.
        workers = self.workers
        task_completion_time = task_duration + current_time
        if SYSTEM_SIMULATED == "Sparrow":
            #Account for time for the probe to get task data and details.
            task_completion_time += 2 * NETWORK_DELAY
        #print "Job id", job_id, current_time, " machine ", machine.id, "cores ",core_indices, " job id ", job_id, " task index: ", task_index," task duration: ", task_duration, " arrived at ", probe_arrival_time, "and will finish at time ", task_completion_time, "core 24 has holes", keeper.worker_queues_free_time_start[24], keeper.worker_queues_free_time_end[24]

        is_job_complete = job.update_task_completion_details(task_completion_time)

        if is_job_complete:
            self.jobs_completed += 1
            # Task's total time = Scheduler queue time (=0) + Scheduler Algorithm time + Machine queue wait time + Task processing time
            try:
                print >> finished_file, task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " total_job_running_time: ",(job.end_time - job.start_time), " job_id", job_id, " scheduler_algorithm_time ", scheduler_algorithm_time, " task_wait_time ", task_wait_time, " task_processing_time ", task_duration
                #print "job id", job_id,task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " total_job_running_time: ",(job.end_time - job.start_time), " job_id", job_id, " scheduler_algorithm_time ", scheduler_algorithm_time, " task_wait_time ", task_wait_time, " task_processing_time ", task_duration
            except IOError, e:
                print "Failed writing to output file due to ", e

        for core_index in core_indices:
            events.append((task_completion_time, TaskEndEventForMachines(core_index, task_duration)))
        if is_job_complete:
            del job.unscheduled_tasks
            del job.actual_task_duration
            del job.cpu_reqs_by_tasks
            del self.jobs[job.id]

        return True, events

    #Simulation class
    def run(self):
        global utilization
        global time_elapsed_in_dc 
        global total_busyness
        global start_time_in_dc
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

        for machine in self.machines:
            print >> load_file, machine.id, machine.num_tasks_processed
        #Free up all memory
        del self.machines[:]
        del self.scheduler_indices[:]
        num_workers = len(self.workers)
        for worker in self.workers:
            total_busyness += worker.busy_time
        del self.workers[:]
        print "Simulation ending, no more events. Jobs completed", self.jobs_completed
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

#0.5ms delay on each link.
NETWORK_DELAY = 0.0005
job_count = 1
#Randomize the run.
random.seed()

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

file_name = ['finished_file', sys.argv[2], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[9],sys.argv[10], 'load']
load_file_name = separator.join(file_name)
load_file = open(load_file_name,'w')


t1 = time()
simulation = Simulation(WORKLOAD_FILE, TOTAL_MACHINES)
num_workers = len(simulation.workers)
keeper = ClusterStatusKeeper(num_workers)
simulation.run()

simulation_time = (time() - t1)
print "Simulation ended in ", simulation_time, " s "
import subprocess ; label = subprocess.check_output(["git", "describe"]).strip()
print "Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS,".Git version -", label
print >> finished_file, "Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS,".Git version -", label


finished_file.close()
load_file.close()
# Generate CDF data
import os; os.system("python process.py " + log_file + " " + SYSTEM_SIMULATED + " " + WORKLOAD_FILE + " " + str(TOTAL_MACHINES)); os.remove(log_file)
#os.system("python  load_murmuration.py " + load_file_name) ; os.remove(load_file_name)
