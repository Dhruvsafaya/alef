#!/bin/sh

paths="${@}"
echo $paths
# git diff will return exit code 1 for changes (failure), or 0 meaning no changes
is_changed=$(git diff --quiet --exit-code origin/develop... ${paths} || echo $?)
if [ ! $is_changed ]
then
  echo "INFO: No changes encountered inside $paths. Skipping running of tasks."
  exit 101
else
  echo "INFO: Changes encountered inside $paths. Proceeding running tasks.."
  exit 0
fi