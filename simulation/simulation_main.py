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

        self.file_task_execution_time(job_args)
        self.estimated_task_duration = mean_task_duration
        self.remaining_exec_time = self.estimated_task_duration*len(self.unscheduled_tasks)

        self.should_finish_time = self.start_time

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
            worker_indices = simulation.find_machines_random(PROBE_RATIO, self.job.num_tasks, range(TOTAL_MACHINES))

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
class ApplySchedulerUpdates:
    def __init__(self, machine_id, origin_scheduler_index, history_time, duration):
        self.origin_scheduler_index  = origin_scheduler_index
        self.machine_id = machine_id
        self.history_time = history_time
        self.duration = duration

    def run(self, current_time):
        current_time = int(ceil(current_time))
        for rx_scheduler_id in simulation.scheduler_indices:
            keeper.update_scheduler_view(self.origin_scheduler_index, rx_scheduler_id, self.machine_id, current_time, self.history_time, self.duration)
        return []

#####################################################################################################################
#####################################################################################################################
# ClusterStatusKeeper: Keeps track of tasks assigned to workers and the 
# estimated start time of each task. The queue also includes currently
# executing task at the worker.
class ClusterStatusKeeper(object):
    #In Murmuration, worker is the absolute core id, unique to every core in every machine of the DC.
    def __init__(self, num_workers, scheduler_indices):
        self.worker_queues_free_time = {}
        #Array of history in order to simulate delayed updates
        self.worker_queues_history = {}
        self.scheduler_view = {}
        for i in range(0, num_workers):
           self.worker_queues_free_time[i] = 0 
           #History will be {insertion_time: [start_task_time, end_task_time, scheduler_index]} format
           #Positive task times indicate holes to be put back in.
           #Negative task times indicate holes to be removed.
           self.worker_queues_history[i] = defaultdict(list)
        for i in scheduler_indices:
           self.scheduler_view[i] = defaultdict(int)

    def print_holes(self, worker_index):
        print "Actual hole starts from", self.worker_queues_free_time[worker_index]

    def print_history(self, worker_index):
        print "History of placements - ", self.worker_queues_history[worker_index]

    def update_scheduler_view(self, origin_scheduler_index, rx_scheduler_index, core_id,current_time, history_time, duration):
        if origin_scheduler_index == rx_scheduler_index:
            return
        availability_at_cores = self.scheduler_view[rx_scheduler_index]
        last_updated_time = 0
        core_availability = 0
        if core_id in availability_at_cores.keys():
            core_availability = availability_at_cores[core_id]
        #Update whatever is received from other schedulers and earlier placements by self.
        #This is the complete truth.
        if history_time > core_availability:
            #Placement happened after previous tasks had completed.
            #This advancement cannot be reversed.
            #All durations get added beyond this hole.
            core_availability = history_time
        core_availability += duration
        availability_at_cores[core_id] = core_availability

    def get_machine_with_shortest_wait(self, scheduler_index):
        availability_at_cores = self.scheduler_view[scheduler_index]
        if len(availability_at_cores) == 0:
            return 0,0
        return dict(sorted(availability_at_cores.items(), key=itemgetter(1))).items()[0]

    def get_machine_est_wait(self, cores, current_time, best_current_time, scheduler_index):
        current_time = int(ceil(current_time))
        earliest_available_time = float('inf')
        selected_core_id = None
        for core in cores:
            core_available = keeper.get_updated_scheduler_view(core.id, scheduler_index, current_time)
            if core_available <= current_time:
                return (current_time, core.id)
            if core_available < earliest_available_time:
                earliest_available_time = core_available
                selected_core_id = core.id
        if earliest_available_time == float('inf'):
            raise AssertionError('debug')
        return (earliest_available_time, selected_core_id)

    def update_worker_queues_free_time(self, core_id, start_time, end_time, current_time, scheduler_index):
        current_time = int(ceil(current_time))

        duration = end_time - start_time
        updated_view = 0
        availability_at_cores = self.scheduler_view[scheduler_index]
        if core_id in availability_at_cores.keys():
            updated_view = availability_at_cores[core_id]

        if start_time > updated_view:
            #Placement happened after previous tasks had completed.
            #This advancement cannot be reversed.
            #All durations get added beyond this hole.
            updated_view = start_time
        updated_view += duration
        availability_at_cores[core_id] = updated_view

        #Update the actual worker's queue.
        actual_start_at_worker = self.worker_queues_free_time[core_id] if self.worker_queues_free_time[core_id] > current_time else current_time
        if actual_start_at_worker < start_time:
            print "Actual start at worker is",actual_start_at_worker, "while scheduler predicted",start_time
            print "(Actual queue length at worker is",self.worker_queues_free_time[core_id], "at time",current_time,")"
            raise AssertionError('Unexpected - Scheduler sees a more delayed view than actual queue length')
        self.worker_queues_free_time[core_id] = actual_start_at_worker + duration
        has_collision = False if start_time == actual_start_at_worker else True
        return actual_start_at_worker, has_collision

#####################################################################################################################
#####################################################################################################################
class TaskEndEvent(object):
    def __init__(self, worker_index, task_duration, job_id, task_wait_time):
        self.worker_index = worker_index
        self.task_duration = task_duration
        self.job_id = job_id
        self.task_wait_time = task_wait_time

    def run(self, current_time):
        job = simulation.jobs[self.job_id]
        is_job_complete = job.update_task_completion_details(current_time)

        if is_job_complete:
            simulation.jobs_completed += 1
            # Task's total time = Scheduler queue time (=0) + Scheduler Algorithm time(=0) + Machine queue wait time + Task processing time
            try:
                print >> finished_file, current_time,"total_job_running_time:",(job.end_time - job.start_time), "job_id", job.id
            except IOError, e:
                print "Failed writing to output file due to ", e


        events = []
        worker = simulation.workers[self.worker_index]
        worker.busy_time += self.task_duration

        new_events = worker.free_slot(current_time)
        for new_event in new_events:
            events.append(new_event)

        if is_job_complete:
            del job.unscheduled_tasks
            del job.actual_task_duration
            del simulation.jobs[job.id]

        return events


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
            core_id = task_info[0]
            job_id = task_info[1]
            job = simulation.jobs[job_id]
            if len(job.unscheduled_tasks) <= 0:
                raise AssertionError('No redundant probes in Murmuration, yet tasks have finished?')
            task_index = task_info[2]
            probe_arrival_time = task_info[3]
            if core_id not in self.free_cores.keys():
                #Wait for the next event to trigger this task processing
                candidate_probes_covered.put((best_fit_time, task_info))
                #Note these cores, they are not ready to execute yet, but important to clear free_cores list.
                #print "Best fit time", best_fit_time, "Task info", task_info, "reason - cores not yet free"
                continue

            core_available_time = self.free_cores[core_id]
            candidate_cores[core_id] = core_available_time
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
                #print current_time,": Got candidate best fit time", candidate_best_fit_time, "Task info", task_info, "Cores", core_id, "due to finish at ", earliest_task_completion_time
            else:
                #Not our best candidate
                candidate_probes_covered.put((best_fit_time, task_info))
                #print "Best fit time", best_fit_time, "Task info", task_info, "reason - not the best candidate"

        if earliest_task_completion_time == float('inf'):
            #Reinsert all free cores
            for core_index in candidate_cores.keys():
                self.free_cores[core_index] = candidate_cores[core_index]
            #Reinsert all queued probes that were inspected
            while not candidate_probes_covered.empty():
                best_fit_time, task_info = candidate_probes_covered.get()
                self.queued_probes.put((best_fit_time, task_info))
            return []

        # Extract all information
        core_id = candidate_task_info[0]
        job_id = candidate_task_info[1]
        job = simulation.jobs[job_id]
        if len(job.unscheduled_tasks) <= 0:
            raise AssertionError('No redundant probes in Murmuration, yet tasks have finished?')
        task_index = candidate_task_info[2]
        probe_arrival_time = candidate_task_info[3]

        task_actual_duration = job.actual_task_duration[task_index]
        #print current_time,": Got candidate best fit time", candidate_best_fit_time, "Task duration", task_actual_duration, "Cores", core_id, "due to finish at ", earliest_task_completion_time

        if earliest_task_completion_time < current_time:
            print current_time,": Got candidate best fit time", candidate_best_fit_time, "Task duration", task_actual_duration, "Cores", core_id, "due to finish at ", earliest_task_completion_time
            raise AssertionError('Unprocessed task with completion time before current time.')

        #Reinsert all free cores other than the ones needed
        for core_index in candidate_cores.keys():
            if core_index != core_id:
                self.free_cores[core_index] = candidate_cores[core_index]

        #Reinsert all queued probes that were inspected
        while not candidate_probes_covered.empty():
            best_fit_time, task_info = candidate_probes_covered.get()
            self.queued_probes.put((best_fit_time, task_info))

        # Finally, process the current task with all these parameters
        #print "Picked machine", self.id," for job", job_id,"task", task_index, "duration", task_actual_duration,"with best fit scheduler view", candidate_processing_time
        new_events = simulation.process_machine_task(self, core_id, job_id, task_index, task_actual_duration, candidate_processing_time, probe_arrival_time)
        for new_event in new_events:
            events.append(new_event)

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
            job_id = task_info[1]
            if not job_id in simulation.jobs.keys():
                continue
            job = simulation.jobs[job_id]
            task_index = task_info[2]
            probe_arrival_time = task_info[3]
            task_actual_duration = job.actual_task_duration[task_index]

            # Remove redundant probes for this task without accounting for them in response time.
            if task_actual_duration not in simulation.jobs[job_id].unscheduled_tasks:
                continue

            core_id = self.free_cores.keys()[0]
            del self.free_cores[core_id]

            # Note - Current time is the start of the processing time. This is because -
            # 1. The probe has already arrived before now.
            # 2. The cores have been freed before now.
            # 3. There might have been redundant probes because of which this task was
            #    still enqueued till now despite cores available.

            # Finally, process the current task with all these parameters
            new_events = simulation.process_machine_task(self, core_id, job_id, task_index, task_actual_duration, current_time, probe_arrival_time)
            for new_event in new_events:
                events.append(new_event)
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
    def __init__(self):
        self.jobs = {}
        self.event_queue = PriorityQueue()
        self.workers = []
        self.machines = []
        self.scheduler_indices = []

        # Murmuration and Sparrow 
        while len(self.machines) < TOTAL_MACHINES:
            machine = Machine(CORES_PER_MACHINE, len(self.machines), len(self.workers))
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

    #Simulation class
    def find_machines_random(self, probe_ratio, nr_tasks, possible_machine_indices):
        if possible_machine_indices == []:
            return []
        chosen_machine_indices = []
        nr_probes = (probe_ratio*nr_tasks)
        task_index = 0
        #print "Number of machines for Sparrow job", len(possible_machine_indices), "and num tasks", nr_tasks, "probe ratio", probe_ratio, "chose ", chosen_machine_indices
        for task_index in range(0, nr_tasks):
            probe_index = 0
            while probe_index < probe_ratio:
                chosen_machine_id = random.choice(possible_machine_indices)
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
        global time_elapsed_in_dc
        global num_collisions
        # best_fit_for_tasks = (ma, mb, .... )
        best_fit_for_tasks = set()
        for task_index in range(job.num_tasks):
            duration = int(ceil(job.actual_task_duration[task_index]))
            chosen_machine, best_fit_time = keeper.get_machine_with_shortest_wait(scheduler_index)
            best_fit_for_tasks.add((chosen_machine, duration))
            #Update est time at this machine and its cores
            #print "Picked machine", chosen_machine," for job", job.id,"task", task_index, "duration", duration,"with best fit scheduler view", best_fit_time,
            best_fit_time, has_collision = keeper.update_worker_queues_free_time(chosen_machine, best_fit_time, best_fit_time + duration, current_time, scheduler_index)
            probe_params = [chosen_machine, job.id, task_index, current_time]
            self.machines[chosen_machine].add_machine_probe(best_fit_time, probe_params)
            if has_collision:
                num_collisions += 1
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
        #.append(job.estimated_task_duration)

        # Sort all workers running long jobs in this DC according to their estimated times.
        # Ranking policy used - Least estimated time and hole duration > estimted task time.
        # Find machines for tasks and trigger events.
        # Returns a set of machines to service tasks of the job - (m1, m2, ...).
        # May be less than the number of tasks due to same machines hosting more than one task.
        machine_indices_duration = self.find_machines_murmuration(job, current_time, scheduler_index)
        for machine_id, duration in machine_indices_duration:
            task_arrival_events.append((current_time, ProbeEventForMachines(self.machines[machine_id])))
            task_arrival_events.append((current_time + UPDATE_DELAY, ApplySchedulerUpdates(machine_id, scheduler_index, current_time, duration)))
        return task_arrival_events

    #Simulation class
    def process_machine_task(self, machine, core_id, job_id, task_index, task_duration, current_time, probe_arrival_time):
        job = self.jobs[job_id]
        task_wait_time = current_time - probe_arrival_time

        events = []
        job.unscheduled_tasks.remove(task_duration)
        task_completion_time = task_duration + current_time
        if SYSTEM_SIMULATED == "Sparrow":
            #Account for time for the probe to get task data and details.
            task_completion_time += 2 * NETWORK_DELAY
        #print "Job id", job_id, current_time, " machine ", machine.id, "cores ",core_id, " job id ", job_id, " task index: ", task_index," task duration: ", task_duration, " arrived at ", probe_arrival_time, "and will finish at time ", task_completion_time

        events.append((task_completion_time, TaskEndEvent(core_id, task_duration, job_id, task_wait_time)))
        return events

    #Simulation class
    def run(self):
        global utilization
        global time_elapsed_in_dc 
        global total_busyness
        global start_time_in_dc
        last_time = 0

        self.jobs_file = open(WORKLOAD_FILE, 'r')
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

if CORES_PER_MACHINE != 1:
    print "Number of cores per machine != 1"
    sys.exit(1)

#log_file is used for logging information on individual jobs, for JCT to be calculated later.
file_name = ['finished_file', sys.argv[2], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[9],sys.argv[10]]
separator = '_'
log_file = (separator.join(file_name))
finished_file   = open(log_file, 'w')

t1 = time()
simulation = Simulation()
num_workers = len(simulation.workers)
keeper = ClusterStatusKeeper(num_workers, simulation.scheduler_indices)
simulation.run()

simulation_time = (time() - t1)
print "Simulation ended in ", simulation_time, " s "
print "Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS, "total collisions", num_collisions
print >> finished_file, "(Single Core) Average utilization in", SYSTEM_SIMULATED, "with", TOTAL_MACHINES,"machines and",num_workers, "total workers", POLICY, "hole fitting policy and", sys.argv[7],"system is", utilization, "(simulation time:", simulation_time," total DC time:",time_elapsed_in_dc, ")", "total busyness", total_busyness, "update delay is", UPDATE_DELAY, "scheduler:cores ratio", RATIO_SCHEDULERS_TO_WORKERS,"total collisions", num_collisions

finished_file.close()
# Generate CDF data
import os; os.system("python process.py " + log_file + " " + SYSTEM_SIMULATED + " " + WORKLOAD_FILE + " " + str(TOTAL_MACHINES)); os.remove(log_file)
