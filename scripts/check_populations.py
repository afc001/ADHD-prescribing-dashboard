#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 27 19:33:39 2026

@author: adamchidlow
"""

import pandas as pd

pop = pd.read_excel(
    '/Users/adamchidlow/Desktop/Spectator/Data/ONS_populations/2011_2022.xlsx',
    sheet_name='Mid-2011 ICB 2024',
    skiprows=3
)
print(pop['ICB 2024 Name'].unique()[:10])

nhsbsa = pd.read_csv(
    '/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed/nhsbsa_regional_annual.csv'
)
print(nhsbsa['icb_name'].unique()[:10])