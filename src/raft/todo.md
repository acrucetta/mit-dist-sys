
Start Election
• On conversion to candidate, start election:
    • Increment currentTerm
    • Vote for self
    • Reset election timer
    • Send RequestVote RPCs to all other servers
• If votes received from majority of servers: become leader
• If AppendEntries RPC received from new leader: convert to
follower
• If election timeout elapses: start new election

Become Leader

Request Votes

Append Entries

---

TestReElection3A

Key things to implement:
- When a server discovers current leader is disconnected (misses heartbeats), it should start election
- Only start election if you're not already a leader
- When old leader reconnects, it should notice higher term and step down
- Without quorum (majority), no server should become leader
- Server should revert to follower when it sees higher term

Focus on making sure your timeouts, term management, and state transitions handle all these cases correctly.
