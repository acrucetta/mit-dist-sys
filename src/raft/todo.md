
Hints
- [] Modify Make() to create a background go-routine that will kick off leader
election periodically by sending RequestVote RPCs when it hasn't heard from another
peer for a while.
- [] The tester requires that the leader sends heartbeat RPCs not more than 10x/second
- [] Implement GetState()

Debugging
- [] Add debugging to each component


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

