# What changed after the design

[DESIGN.md](DESIGN.md) was the first commit, before any code. It was shortened afterwards to fit one page,
without changing a decision; `git show b4452b9:DESIGN.md` is the original wording.
What turned out differently in the implementation:

- **Row lock is `FOR NO KEY UPDATE`, not `FOR UPDATE`.** Every insert into `points` takes a
  `KEY SHARE` lock on its run through the foreign key, and `FOR UPDATE` conflicts with that. The
  weaker lock still serializes writers of one run. Found while building the partial-write test.
- **A database connection that dies mid-transaction must not kill the process.** Not in the design
  at all; found by the first test run (see NOTE.md).
- **Requests are logged as method, path, status, time — never the query string.** A client that
  puts its key in the URL by mistake would otherwise write it to the log.
- **Malformed and over-long URLs get the same 404 as any unknown path.** The framework's default
  answer echoed the path back in its own format.
- The stream poll is 2 s ± 25% jitter, so streams opened together do not query in lockstep.
