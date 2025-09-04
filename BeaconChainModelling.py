import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

import seaborn as sns
from scipy import stats

# Ensure the interactive backend for matplotlib
# Uncomment the following line if running in a Jupyter notebook
# %matplotlib inline
  
MIN_PER_EPOCH_CHURN_LIMIT = 4
CHURN_LIMIT_QUOTIENT = 65536
DANEB_ACTIVATION_EPOCH = 269568
MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT = 8
MAX_PER_EPOCH_EXIT_CHURN_LIMIT = 16


# Load the CSV file
data = pd.read_csv('beacon_chain_stats.csv')

def activation_churn_limit(epoch, nr_validators):
    churn_limit_pre_daneb = max(MIN_PER_EPOCH_CHURN_LIMIT, nr_validators // CHURN_LIMIT_QUOTIENT)
    if epoch < DANEB_ACTIVATION_EPOCH:
        return churn_limit_pre_daneb
    else:
        return min(MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT, churn_limit_pre_daneb)

data['Churn_limit'] = data.apply(lambda x: activation_churn_limit(x['epoch'], x['active']), axis=1)

# Initialize the new column
data['activations'] = 0
data['newly_queued_entries'] = 0
data['newly_queued_exits'] = 0
data['newly_queued_cum_exits'] = 0


# Perform the calculations for each row
for i in range(1, len(data)):

    # Get current and previous rows
    current_row = data.iloc[i]
    previous_row = data.iloc[i - 1]
    
    if current_row['slot'] % 32 == 0:
        # Calculate the differences as specified
        active_diff = current_row['active'] - previous_row['active']
        exit_diff = current_row['exit_queue'] - previous_row['exit_queue']
        entry_diff = current_row['entry_queue'] - previous_row['entry_queue']
        activations = max(active_diff+max(exit_diff, 0), -entry_diff)
        
        # Sum up the differences
        data.at[i, 'activations'] = activations
        data.at[i, 'newly_queued_entries'] = activations + entry_diff
        data.at[i, 'newly_queued_exits'] = activations - active_diff
    else:
        # Only take the difference of the exit_queue with respect to the previous row
        data.at[i, 'newly_queued_exits'] = current_row['exit_queue'] - previous_row['exit_queue']

    if data.at[i-1, 'newly_queued_exits'] == MAX_PER_EPOCH_EXIT_CHURN_LIMIT and data.at[i, 'newly_queued_exits'] != MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
        tmp = data.at[i, 'newly_queued_exits']
        j = 1
        while data.at[i-j, 'newly_queued_exits'] == MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
            tmp += data.at[i-j, 'newly_queued_exits']
            j += 1
        data.at[i-j+1, 'newly_queued_cum_exits'] = tmp

    elif data.at[i-1, 'newly_queued_exits'] != MAX_PER_EPOCH_EXIT_CHURN_LIMIT and data.at[i, 'newly_queued_exits'] != MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
        data.at[i, 'newly_queued_cum_exits'] = data.at[i, 'newly_queued_exits']

data.to_csv('modified_file.csv', index=False)

deposits = data[['newly_queued_entries', 'newly_queued_cum_exits']].values

# Plot the data to understand its distribution
nr_bins = 50
plt.figure(figsize=(10, 6))
sns.histplot(deposits, kde=True, stat="density", bins=nr_bins)

plt.title('Histogram of Deposits with KDE')
plt.xlabel('Deposits')
plt.ylabel('Density')
plt.show()

# Fit different distributions and plot them on log scale
plt.figure(figsize=(10, 6))
sns.histplot(deposits, kde=False, stat="density", bins=nr_bins, label='Data')


# Fit a normal distribution
mu, std = stats.norm.fit(deposits)
xmin, xmax = plt.xlim()
x = np.logspace(np.log10(xmin), np.log10(xmax), 100)
p = stats.norm.pdf(x, mu, std)
plt.plot(x, p, 'k', linewidth=2, label='Normal')

# Fit a gamma distribution
a, loc, scale = stats.gamma.fit(deposits)
p = stats.gamma.pdf(x, a, loc, scale)
plt.plot(x, p, 'r', linewidth=2, label='Gamma')

# Fit a lognormal distribution
shape, loc, scale = stats.lognorm.fit(deposits)
p = stats.lognorm.pdf(x, shape, loc, scale)
plt.plot(x, p, 'g', linewidth=2, label='Lognormal')

plt.title('Distribution Fit to Deposits')
plt.xlabel('Deposits')
plt.ylabel('Density')
plt.legend()
plt.show()

# Perform goodness of fit tests
print("Normal distribution fit test results:")
print(stats.kstest(deposits, 'norm', args=(mu, std)))

print("\nGamma distribution fit test results:")
print(stats.kstest(deposits, 'gamma', args=(a, loc, scale)))

print("\nLognormal distribution fit test results:")
print(stats.kstest(deposits, 'lognorm', args=(shape, loc, scale)))