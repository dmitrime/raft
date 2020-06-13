# Raft

## Implementation

I wrote the code for the MIT Distributed Systems online class, and then adapted it to the test setup from [this repository](https://github.com/eliben/raft).
Thus, only the code in `raft.go` is my own, with minor changes to make the tests run.

More details about the implementation in this [blog post](https://bluedive.site/posts/implementing-raft/).


## Running

For basic set of tests, `go test -v -race`.

For test simulating unreliable netwrok, `RAFT_UNRELIABLE_RPC=1 go test -v -race`.

Because my implementation uses long-running goroutines that `Sleep` for fixed intervals, a few tests may fail on some runs
because they expect consensus within some time period. When the message is dropped, it will naturally take longer to reach consensus.
In that case increasing the sleeping time inside the tests should help.

