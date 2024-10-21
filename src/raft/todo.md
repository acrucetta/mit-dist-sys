
Hints
- [] Modify Make() to create a background go-routine that will kick off leader
election periodically by sending RequestVote RPCs when it hasn't heard from another
peer for a while.
- [] The tester requires that the leader sends heartbeat RPCs not more than 10x/second
- [] Implement GetState()

Debugging
- [] Add debugging to each component