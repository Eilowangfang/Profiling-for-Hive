# Profiling-for-Hive
This project is for ''Visualized profiling large-scale query execution''.

Current version supports profiling (SQL) query execution at task-level on Hive.

The updated version will be relased soon.

# Usage
This is for extracting useful info. from log for profiling.

Usage:
Step1: obtain yarn log after Hive execution (e.g., "ds100_query12");

Step2: linux shell "cat ds100_query4.log| grep -E "Fang: Tez|Container: container" > tmp.log"
	then we obtain a log that contains all the statistics info. useful for profiling.

Step3: g++ --std=c++11 main.cpp -o extracter
	then we obtain executable file "a.out".

Step4: ./extracter
	then we obtain the statistics file "Profile.csv".
