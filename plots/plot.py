import matplotlib.pyplot as plt
import numpy as np

# 1. Дані з Таблиці 2
labels =['Normal', 'Poison', 'Fan-out', 'Loop']
baseline_af =[1.0, 4.0, 16.6, 5.0]
guarded_af =[1.0, 4.0, 0.0, 5.0]

# 2. Налаштування позицій стовпчиків
x = np.arange(len(labels))  # позиції по осі X
width = 0.35  # ширина стовпчиків

# 3. Академічний стиль графіка
plt.rcParams.update({
    'font.family': 'serif', # Шрифт із засічками (як Times New Roman, стандарт для статей)
    'font.size': 12,
    'axes.axisbelow': True  # Сітка ховається за стовпчиками
})

fig, ax = plt.subplots(figsize=(8, 5)) # Оптимальні пропорції для колонки тексту

# 4. Побудова стовпчиків (A/B)
rects1 = ax.bar(x - width/2, baseline_af, width, 
                label='Baseline Mode', color='#1f77b4', edgecolor='black', linewidth=1)
rects2 = ax.bar(x + width/2, guarded_af, width, 
                label='Guarded Mode', color='#ff7f0e', edgecolor='black', linewidth=1)

# 5. Оформлення осей та заголовків
ax.set_ylabel('Amplification Factor (AF)', fontweight='bold', fontsize=12)
# ax.set_title('Figure 2. Amplification Factor by Workload Profile', pad=15, fontweight='bold') # Заголовок зазвичай пишуть у caption в LaTeX, але якщо треба на графіку - розкоментуй
ax.set_xticks(x)
ax.set_xticklabels(labels, fontweight='bold')
ax.legend(loc='upper left', frameon=True, edgecolor='black')

# Додаємо легку горизонтальну сітку
ax.yaxis.grid(True, linestyle='--', alpha=0.7)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# 6. Додавання підписів даних (Data Labels)
def add_labels(rects):
    for rect in rects:
        height = rect.get_height()
        # Для значення 0.0 робимо підпис прямо над нульовою лінією
        y_pos = height if height > 0 else 0.2 
        
        ax.annotate(f'{height:.1f}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 4),  # Зсув на 4 пікселі вгору
                    textcoords="offset points",
                    ha='center', va='bottom', 
                    fontsize=11, fontweight='bold', color='black')

add_labels(rects1)
add_labels(rects2)

# Розширюємо вісь Y трохи вгору, щоб підпис 16.6 не врізався в рамку
ax.set_ylim(0, 19)

# 7. Збереження у векторному форматі PDF для LaTeX / Word
fig.tight_layout()
plt.savefig('Figure_2_AF_Comparison.png', format='png', bbox_inches='tight', dpi=300)

plt.show()