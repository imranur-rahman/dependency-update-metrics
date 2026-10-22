# Coding standards

The reviewer reads this file. `AGENTS.md` holds the rules for every agent, and the CI checks hold the mechanical rules. This file holds the judgement calls that no check can make.

## Docs follow the behaviour

When a diff changes a behaviour that a user or an operator sees (a CLI flag, a size or a limit, a deploy step, a workflow rule), find each doc that describes that behaviour and check that it is still true. The usual places are `README.md`, `USAGE.md`, `AGENTS.md`, `host/RUNBOOK.md`, `host/wizard.sh`, `host/.env.example`, and the comments at the top of the changed file. Report each doc that the diff leaves stale.

Each fact has one home. When a doc repeats a number or a rule that another file owns, report it, and ask for a pointer to that file in place of the copy. Example: `AGENTS.md` owns the OSV download size, and `webapp/configure.mjs` owns the rule for the Pages variables.
