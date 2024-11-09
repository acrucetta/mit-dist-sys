
Part 3B: Log
Implement leader and follower code to append new log entries

Tasks:
- Pass TestBasicAgree3B by implementing Start(), then write the code to send and receive new log entries via AppendEntries RPCs, following Figure 2. Send each newly committed entry on applyCh on each peer.
- Implement the election restriction (section 5.4.1 in the paper).
