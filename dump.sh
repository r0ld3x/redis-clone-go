#!/bin/bash

# Insert permanent key
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli SET test:permanent "I never expire"

# Expire in 10 minutes (seconds)
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli SET test:expire_fd "I expire in 10 minutes"
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli EXPIREAT test:expire_fd $(( $(date +%s) + 600 ))

# Expire in 5 minutes (milliseconds)
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli SET test:expire_fc "I expire in 5 minutes"
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli PEXPIREAT test:expire_fc $(( $(date +%s%3N) + 300000 ))


# Many filler keys for RESIZEDB
for i in {1..50}; do
  C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli SET filler:key:$i "val$i"
done

# Select second DB to trigger SELECTDB
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli -n 1 SET db1:key "value from DB1"
C:/Users/R0ld3/OneDrive/Desktop/Redis-8.0.2-Windows-x64-cygwin/redis-cli SAVE
