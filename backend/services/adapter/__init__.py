"""Server-side port of frontend/src/prototype's flow-model rule engines.

This package is the server-side SOURCE OF TRUTH for the placement rules
(R1-R8), validation, and naming logic that the frontend prototype implements
in TypeScript under frontend/src/prototype/{legality,validation,naming}.ts.
Every function here is a line-by-line port of its TS counterpart unless a
docstring says otherwise.

Modules:
  - naming.py      -- tokenize, derived topic/table/dlq names, cron helpers.
  - legality.py     -- the R1-R8 placement-rule engine (add menus, canReparent,
                        isTerminal, rootBlock, isRawBranch, ...) plus
                        validate_placement(), a whole-flow structural check
                        that does not exist as a single TS function (see its
                        docstring).
  - validation.py   -- validateBlock / validateFlow / deployPreflight ports,
                        returning the same `{blockId, where, message}` issue
                        shape the frontend uses (camelCase).
  - common.py       -- now_iso(), new_id(), audit(), and the v2 Mongo
                        COLLECTIONS name constants.

Nothing in this package imports NiFi/Kafka/Mongo clients directly except
common.py's `audit()`, which takes an already-constructed
AsyncIOMotorDatabase — importing this package never pulls in heavy runtime
dependencies just to reuse the pure rule functions.
"""
