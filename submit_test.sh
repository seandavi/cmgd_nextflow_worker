#!/bin/bash
#SBATCH --job-name=submit_test
#SBATCH --mem=128mb
#SBATCH --time=00:02:00
#SBATCH --cpus-per-task=1
set -e

env
module load singularity

echo "starting"
echo "sleeping for 30 seconds"
sleep 30
echo "completed!"
