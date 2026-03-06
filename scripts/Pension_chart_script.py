#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 12:29:38 2026

@author: adamchidlow
"""

import matplotlib.pyplot as plt
import csv
import os

# --- Configuration ---
CSV_FILE = "Pension_chart.csv"  # Place this CSV in the same folder as this script

# --- Read the CSV ---
ages = []
values = []

with open(CSV_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        ages.append(int(row["Age"]))
        values.append(float(row["Average monthly percentage change"]))

# --- Create the chart ---
fig, ax = plt.subplots(figsize=(10, 6))

ax.plot(ages, values, marker="o", color="#2B5797", linewidth=2, markersize=7)

# Title and labels
ax.set_title(
    "Growth in people claiming UC by age, showing the\neffect of the state pension threshold",
    fontsize=14,
    fontweight="bold",
    pad=15,
)
ax.set_xlabel("Age", fontsize=12)
ax.set_ylabel("Average monthly percentage change", fontsize=12)

# Axis ticks - show every age
ax.set_xticks(ages)
ax.set_xticklabels(ages)

# Light grid for readability
ax.grid(axis="y", linestyle="--", alpha=0.4)

plt.tight_layout()

# --- Save and show ---
output_path = os.path.splitext(CSV_FILE)[0] + ".png"
fig.savefig(output_path, dpi=200, bbox_inches="tight")
print(f"Chart saved to: {output_path}")
plt.show()