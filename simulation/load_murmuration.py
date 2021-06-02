import matplotlib.pyplot as plt
import sys

machine_id = []
load = []
f = open(sys.argv[1], 'r')
for row in f:
    row = row.split(' ')
    machine_id.append(int(row[0]))
    load.append(int(row[1]))
plt.bar(machine_id, load, color = 'g', label = 'Load Data')
plt.xlabel('Machine Ids', fontsize = 12)
plt.ylabel('Load', fontsize = 12)
  
plt.title('Load on Machines', fontsize = 20)
plt.legend()
plt.show()
