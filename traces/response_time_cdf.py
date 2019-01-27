import numpy as np
import sys
import matplotlib
matplotlib.use('Agg') 
import matplotlib.pyplot as plt


files = sys.argv[1:]
# create plot
fig, ax1 = plt.subplots()
#ax.set_xscale("log", basex=2)
ax1.set_yscale("log", basey=2)

pcolors = ['#028413', '#0e326d', '#af0505', 'black']

ctr=0
for filename in files:
    response_times = []
    with open(filename) as f:
        for line in f:
            tokens = line.split()
            jobid = tokens[0]
            start_time = int(tokens[1])
            finish_time = int(tokens[2])
            ideal_runtime = int(tokens[3])
            response_times.append((finish_time - start_time)/1000000.0) #to seconds
    response_times.sort()
    percentiles = range(0, len(response_times))
    percentiles = [float(x)/len(response_times) for x in percentiles]
    pcolor = pcolors[ctr]
    ax1.plot(percentiles, response_times, '-', color=pcolor,  lw=2.5, mew = 2, markerfacecolor='none', markeredgecolor=pcolor, dash_capstyle='round', label=filename)
    ctr = ctr+1


pcolor = '#028413'
#pcolor = '#a5669f'
pcolor = '#db850d'
pcolor = '#00112d'


def string(x):
    if x >= 1024:
    		return str(x/1024) + 'K'
    else:
        return str(x)


# ticklabelcolor = 'grey'
# logps = np.arange(0,5,1)
# ax.xaxis.set_ticks(np.array([2**x for x in logps]))
# xticks = np.array([2**x for x in logps])
# xticks = np.array([string(x) for x in xticks])
# ax.set_xticklabels(xticks, color=ticklabelcolor)

label_fontsize=20
plt.xlabel('Percentiles', fontsize=label_fontsize)
#plt.xlim(xmin=min(k)-0.5, xmax=max(k)+0.5)
#ax1.set_ylim(ymin=50)
ax1.set_ylabel('Job Response Time', fontsize=label_fontsize)


ticklabelcolor = 'grey'
yticks = np.array([10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
#ax2.yaxis.set_ticks(yticks)
#yticklabels = np.array([string(y) for y in yticks])
#ax2.set_yticklabels(yticklabels, color=ticklabelcolor)
#ax2.set_ylim(ymin=20, ymax=180)
#ax2.set_ylabel('% Utilization')

ax1.grid(linestyle=':', linewidth=1, color='grey')
#ax2.grid(linestyle=':', linewidth=1, color='grey')

#plt.title('Max possible throughput - random perm')
#plt.xticks(index + bar_width, ('160/80', '250/125', '540/180', '720/240'))
#ax.set_xticklabels(('160/80', '250/125', '540/180', '720/240'), rotation=45 )
leg = ax1.legend(bbox_to_anchor=(0.45, 0.43), borderaxespad=0, loc=2, numpoints=2, handlelength=2, fontsize=label_fontsize)
#leg = plt.legend(bbox_to_anchor=(0.27, 0.26), borderaxespad=0, loc=2, numpoints=3, handlelength=5)
#leg.get_frame().set_linewidth(0.0)
#leg.get_frame().set_alpha(0.1)
plt.tick_params(labelsize=label_fontsize)

 
plt.tight_layout()
plt.savefig("job_response_dist_synthetic_trace.pdf")
#plt.show()
