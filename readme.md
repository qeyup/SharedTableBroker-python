# Shared table broker

Allows to share the entries of a table between different processes distributed in the network.

- All table entries of a process will be shared with the rest of the connected processes.
- When a process is terminated, all table entries will be deleted.
- When a process updates its entries in the table it will be updated in the rest of the processes.
- Table entries will be shared by all processes, but will only be updated by the owner process.