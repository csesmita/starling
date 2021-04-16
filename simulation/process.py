# This file is used for generating CDF graphs of various JRT data collected,
# and comparing them against various systems.
#
# Command run in the following format -
# pypy process.py finished_file eagle short yahoo 4000 
#
# This indicates processing of Yahoo traces by Eagle simulator for short jobs
# with number of nodes in the cluster = 4000 and job response times given in
# finished file. This script gets called within the simulation_main.py file 
# when the finished file is generated for a run. All parameters are passed 
# from within it.
#
# Name of the Output file - eagle_short_yahoo
# This file gets overwritten for every run for the same scheduler design for
# the same set of traces for the same type of job for different cluster sizes.
#
# Format of the output files -
# ---------------------------
# Percentile file:
#
# Cluster Size 4000
# 50th Percentile - ..
# 90th Percentile - ..
# 99th Percentile - ..
#
# Cluster Size 8000
# ...
#
# Time comparison
# jobid : response_time
# jobid : response_time
# ...


import sys
import numpy as np
import operator

if(len(sys.argv) != 7):
    print "Incorrent number of parameters."
    sys.exit(1)

# Process the finished file which has the job response times recorded
# pypy process.py finished_file_5000_8_d murmuration YH.tr 5000 8 True
infile = open(sys.argv[1], 'r')
jobrunningtime = []
schedulertime = []
waittime = []
processingtime = []
utilization = ""
for line in infile:
    if "utilization" in line:
        #Copy the line over as is
        utilization = line
        continue

    runningtime = float((line.split('total_job_running_time: ')[1]).split()[0])
    jobid = int(((line.split('job_id ')[1]).split())[0])
    
    scheduler_time = float(((line.split('scheduler_algorithm_time ')[1]).split())[0])
    task_wait_time = float(((line.split('task_wait_time ')[1]).split())[0])
    processing_time = float(((line.split('task_processing_time ')[1]).split())[0])

    jobrunningtime.append(runningtime)
    schedulertime.append(scheduler_time)
    waittime.append(task_wait_time)
    processingtime.append(processing_time)

infile.close()

jobrunningtime.sort()
schedulertime.sort()
waittime.sort()
processingtime.sort()
if len(jobrunningtime) > 0:
    outfile = open(sys.argv[2].lower()+"_"+sys.argv[3]+"_"+sys.argv[4]+"_"+sys.argv[5]+"_"+sys.argv[6].lower(), 'a+')
    outfile.write("%s\t%s\t(%s\t%s\t%s)\n"% ("Cluster Size ", sys.argv[4], "(Scheduler Time", "Task Wait Time", "Task Processing Time)"))
    outfile.write("%s\t%s\t(%s\t%s\t%s)\n"% ("50th percentile: ",  np.percentile(jobrunningtime, 50), np.percentile(schedulertime, 50), np.percentile(waittime, 50), np.percentile(processingtime, 50))) 
    outfile.write("%s\t%s\t(%s\t%s\t%s)\n" % ("90th percentile: ", np.percentile(jobrunningtime, 90), np.percentile(schedulertime, 90), np.percentile(waittime, 90), np.percentile(processingtime, 90))) 
    outfile.write("%s\t%s\t(%s\t%s\t%s)\n" % ("99th percentile: ", np.percentile(jobrunningtime, 99), np.percentile(schedulertime, 99), np.percentile(waittime, 99), np.percentile(processingtime, 99)))

    # Copy in utilization informartion
    if utilization != "":
        outfile.write("%s\n"% (utilization))
    outfile.close()
