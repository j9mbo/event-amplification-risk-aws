import matplotlib.pyplot as plt
import numpy as np

# 1. Data from Table 2
labels =['Normal', 'Poison', 'Fan-out', 'Loop']
baseline_af =[1.0, 4.0, 16.6, 5.0]
guarded_af =[1.0, 4.0, 0.0, 5.0]

# 2. Bar positions
x = np.arange(len(labels))  # x-axis positions
width = 0.35  # bar width

# 3. Academic plot style
plt.rcParams.update({
    'font.family': 'serif',  # serif font (Times-like, standard for papers)
    'font.size': 12,
    'axes.axisbelow': True   # grid behind the bars
})

fig, ax = plt.subplots(figsize=(8, 5))  # good proportions for a text column

# 4. Bars (A/B)
rects1 = ax.bar(x - width/2, baseline_af, width,
                label='Baseline Mode', color='#1f77b4', edgecolor='black', linewidth=1)
rects2 = ax.bar(x + width/2, guarded_af, width,
                label='Guarded Mode', color='#ff7f0e', edgecolor='black', linewidth=1)

# 5. Axes and titles
ax.set_ylabel('Amplification Factor (AF)', fontweight='bold', fontsize=12)
# ax.set_title(...) usually goes in the LaTeX caption; uncomment to draw it on the figure
ax.set_xticks(x)
ax.set_xticklabels(labels, fontweight='bold')
ax.legend(loc='upper left', frameon=True, edgecolor='black')

# light horizontal grid
ax.yaxis.grid(True, linestyle='--', alpha=0.7)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# 6. Data labels
def add_labels(rects):
    for rect in rects:
        height = rect.get_height()
        # for a 0.0 value, place the label just above the zero line
        y_pos = height if height > 0 else 0.2

        ax.annotate(f'{height:.1f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 4),  # 4 px up
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=11, fontweight='bold', color='black')

add_labels(rects1)
add_labels(rects2)

# extend the Y axis a bit so the 16.6 label does not touch the frame
ax.set_ylim(0, 19)

# 7. Save (PNG at 300 dpi)
fig.tight_layout()
plt.savefig('Figure_2_AF_Comparison.png', format='png', bbox_inches='tight', dpi=300)

plt.show()
