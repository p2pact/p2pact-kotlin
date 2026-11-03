# p2pact-cli

A small command-line client of the P2PACT engine, built on `p2pact-client`.

```sh
./gradlew :examples:p2pact-cli:installDist
alias p2pact-cli=examples/cli/build/install/p2pact-cli/bin/p2pact-cli

# on the computer: create an invite
nomad invite                        # prints p2pact1.…
p2pact-cli pair p2pact1.…           # the person at the computer approves the grants
p2pact-cli chats                    # id, state, agent, title, folder of every chat
p2pact-cli send <session> "run the tests"   # streams the reply, asks on permission requests
p2pact-cli watch                    # fleet changes, new messages and notifications, until Ctrl-C
```

Keys and paired computers are kept in `$P2PACT_CLI_HOME` (default `~/.p2pact-cli`).
