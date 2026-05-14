#!/usr/bin/env python3
"""Plot the preliminary staking yield curve."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

tenors = [30, 90, 180, 365, 730]
labels = ['30d', '90d', '180d', '1y', '2y']
zero_rates = [3.5547, 3.5381, 3.5361, 3.5396, 3.5461]

fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(tenors, zero_rates, 'o-', color='#4C72B0', linewidth=2.5, markersize=8)
ax.fill_between(tenors, [r - 0.15 for r in zero_rates], [r + 0.15 for r in zero_rates],
                alpha=0.15, color='#4C72B0')
ax.set_xticks(tenors)
ax.set_xticklabels(labels)
ax.set_xlabel('Tenor', fontsize=12)
ax.set_ylabel('Zero Rate (%)', fontsize=12)
ax.set_title('Ethereum Staking Yield Curve (Preliminary)\n46-day calibration · Merton jump-diffusion · 500 MC paths', fontsize=13)
ax.set_ylim(3.0, 4.0)
ax.axhline(y=3.54, color='gray', linestyle='--', alpha=0.4, linewidth=1)
ax.text(600, 3.56, '~3.54%', color='gray', fontsize=10, alpha=0.7)
ax.grid(True, alpha=0.3)
fig.tight_layout()
fig.savefig('/home/hannes/.openclaw/workspace/StakeYieldFinance/yield_curve_preliminary.png', dpi=150)
print('Saved')
