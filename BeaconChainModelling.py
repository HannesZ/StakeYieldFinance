import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# ---------------- Config ----------------
# Set MODE to "count" (old behavior) or "eth" (ETH-weighted analysis).
MODE = "eth"   # "count" or "eth"

# Input from the data-sourcing script:
INPUT_CSV = "beacon_chain_queues_12364282_12464282.csv"   # <-- set to your actual file
OUTPUT_CSV = "modified_file.csv"

# Chain constants (pre-/post-Deneb churn caps)
MIN_PER_EPOCH_CHURN_LIMIT = 4
CHURN_LIMIT_QUOTIENT = 65536
DANEB_ACTIVATION_EPOCH = 269568
MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT = 8
MAX_PER_EPOCH_EXIT_CHURN_LIMIT = 16

# ---------------- Column mapping ----------------
# New data file has:
#   epoch, slot,
#   active_count, active_eth,
#   entry_queue_count, entry_queue_eth,
#   exit_queue_count, exit_queue_eth
if MODE == "count":
    ACTIVE_COL = "active_count"
    ENTRY_COL  = "entry_queue_count"
    EXIT_COL   = "exit_queue_count"
    UNIT = "validators"
elif MODE == "eth":
    ACTIVE_COL = "active_eth"
    ENTRY_COL  = "entry_queue_eth"
    EXIT_COL   = "exit_queue_eth"
    UNIT = "ETH"
else:
    raise ValueError("MODE must be 'count' or 'eth'")

# ---------------- Load data ----------------
data = pd.read_csv(INPUT_CSV)

# Ensure numeric types (ETH columns were written as strings with 9 decimals)
for col in ["active_eth", "entry_queue_eth", "exit_queue_eth"]:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors="coerce")

# ---------------- Churn limit helpers ----------------
def activation_churn_limit_count(epoch: int, nr_validators: int) -> int:
    """Spec churn limit in validators/epoch (pre-/post-Deneb caps)."""
    churn_limit_pre_daneb = max(MIN_PER_EPOCH_CHURN_LIMIT, nr_validators // CHURN_LIMIT_QUOTIENT)
    if epoch < DANEB_ACTIVATION_EPOCH:
        return churn_limit_pre_daneb
    else:
        return min(MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT, churn_limit_pre_daneb)

# We keep both: the canonical validator-based churn limit, and an ETH-estimate.
# ETH-estimate is simply churn_limit_count * 32 (ETH/epoch). This is an approximation
# that remains compatible with your historic series and modeling.
data["Churn_limit_validators"] = data.apply(
    lambda x: activation_churn_limit_count(int(x["epoch"]), int(data.at[x.name, "active_count"])),
    axis=1
)
data["Churn_limit_eth_est"] = data["Churn_limit_validators"] * 32.0  # ETH per epoch (approx)

# ---------------- Initialize new columns ----------------
# We compute the same derived fields as before, but for the chosen MODE.
# These will be in "validators" if MODE="count" or in "ETH" if MODE="eth".
data["activations"] = 0.0
data["newly_queued_entries"] = 0.0
data["newly_queued_exits"] = 0.0
data["newly_queued_cum_exits"] = 0.0

# ---------------- Core loop (epoch/slot diffs) ----------------
# Mirrors your original logic, but applied to either *_count or *_eth columns.
for i in range(1, len(data)):
    current_row = data.iloc[i]
    previous_row = data.iloc[i - 1]

    active_now = current_row[ACTIVE_COL]
    active_prev = previous_row[ACTIVE_COL]
    exit_now = current_row[EXIT_COL]
    exit_prev = previous_row[EXIT_COL]
    entry_now = current_row[ENTRY_COL]
    entry_prev = previous_row[ENTRY_COL]

    # Ensure numeric
    if pd.isna([active_now, active_prev, exit_now, exit_prev, entry_now, entry_prev]).any():
        # Skip if something is NaN (can happen if API hiccup created an empty value)
        continue

    if current_row["slot"] % 32 == 0:
        active_diff = active_now - active_prev
        exit_diff = exit_now - exit_prev
        entry_diff = entry_now - entry_prev

        # As in your original: activations = max(active_diff + max(exit_diff, 0), -entry_diff)
        activations = max(active_diff + max(exit_diff, 0), -entry_diff)

        data.at[i, "activations"] = activations
        data.at[i, "newly_queued_entries"] = activations + entry_diff
        data.at[i, "newly_queued_exits"] = activations - active_diff
    else:
        # Between epochs, only take the difference of exit queue from previous slot
        data.at[i, "newly_queued_exits"] = exit_now - exit_prev

    # Cumulative exit bucketing (unchanged logic)
    prev_exits = data.at[i - 1, "newly_queued_exits"]
    cur_exits = data.at[i, "newly_queued_exits"]

    # Note: MAX_PER_EPOCH_EXIT_CHURN_LIMIT is a validator-based cap.
    # If MODE="eth", this check is not semantically meaningful, but we keep it for parity.
    if prev_exits == MAX_PER_EPOCH_EXIT_CHURN_LIMIT and cur_exits != MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
        tmp = cur_exits
        j = 1
        while data.at[i - j, "newly_queued_exits"] == MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
            tmp += data.at[i - j, "newly_queued_exits"]
            j += 1
        data.at[i - j + 1, "newly_queued_cum_exits"] = tmp
    elif prev_exits != MAX_PER_EPOCH_EXIT_CHURN_LIMIT and cur_exits != MAX_PER_EPOCH_EXIT_CHURN_LIMIT:
        data.at[i, "newly_queued_cum_exits"] = cur_exits

# Persist the enriched frame
data.to_csv(OUTPUT_CSV, index=False)
print(f"Saved: {OUTPUT_CSV} (mode: {MODE}, units: {UNIT})")

# ---------------- Distribution fitting ----------------
# Your original code built a 2D array then tried to fit distributions to it.
# Most scipy fits expect 1D. We'll concatenate into a single 1D sample.
deposits_series = pd.concat(
    [
        data["newly_queued_entries"].astype(float),
        data["newly_queued_cum_exits"].astype(float),
    ],
    ignore_index=True,
)
# Drop NaNs or infs that could break the fits
deposits_series = deposits_series.replace([np.inf, -np.inf], np.nan).dropna().values

nr_bins = 50

# Histogram with KDE (visual check)
plt.figure(figsize=(10, 6))
sns.histplot(deposits_series, kde=True, stat="density", bins=nr_bins)
plt.title(f'Histogram of Deposits ({UNIT}) with KDE')
plt.xlabel(f'Deposits ({UNIT})')
plt.ylabel('Density')
plt.tight_layout()
plt.show()

# Fit and compare distributions
plt.figure(figsize=(10, 6))
sns.histplot(deposits_series, kde=False, stat="density", bins=nr_bins, label='Data')

# Domain for PDFs
xmin = max(np.min(deposits_series), 1e-9)  # avoid zero/negative for log-space domains
xmax = np.max(deposits_series)
if xmax <= xmin:
    xmax = xmin * 10.0

# Normal (use linear space; normal can be <= 0)
mu, std = stats.norm.fit(deposits_series)
x_lin = np.linspace(np.min(deposits_series), np.max(deposits_series), 400)
p_norm = stats.norm.pdf(x_lin, mu, std)
plt.plot(x_lin, p_norm, 'k', linewidth=2, label='Normal')

# Gamma & Lognormal (positive support -> use log-spaced grid)
x_pos = np.logspace(np.log10(xmin), np.log10(xmax), 400)

# Gamma
a, loc_g, scale_g = stats.gamma.fit(deposits_series, floc=0)  # fixing loc=0 often stabilizes the fit
p_gamma = stats.gamma.pdf(x_pos, a, loc=loc_g, scale=scale_g)
plt.plot(x_pos, p_gamma, 'r', linewidth=2, label='Gamma')

# Lognormal
shape_l, loc_l, scale_l = stats.lognorm.fit(deposits_series, floc=0)
p_logn = stats.lognorm.pdf(x_pos, shape_l, loc=loc_l, scale=scale_l)
plt.plot(x_pos, p_logn, 'g', linewidth=2, label='Lognormal')

plt.xscale('log')
plt.title(f'Distribution Fit to Deposits ({UNIT})')
plt.xlabel(f'Deposits ({UNIT}) [log scale]')
plt.ylabel('Density')
plt.legend()
plt.tight_layout()
plt.show()

# Goodness-of-fit (KS tests expect 1D arrays)
print("Normal distribution fit test results:")
print(stats.kstest(deposits_series, 'norm', args=(mu, std)))

print("\nGamma distribution fit test results:")
print(stats.kstest(deposits_series, 'gamma', args=(a, loc_g, scale_g)))

print("\nLognormal distribution fit test results:")
print(stats.kstest(deposits_series, 'lognorm', args=(shape_l, loc_l, scale_l)))
