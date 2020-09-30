#!/usr/bin/env python
""" Global data for all tests"""

random_seed = None                  # Holds the random seed used to seed UTs
cluster = None                      # Holds the fake cluster for UTs
clients = None                      # Holds the fake clients for UTs

all_api_versions = [                # All known endpoint versions
    0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 7.1, 7.2, 7.3, 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, 9.0
]
