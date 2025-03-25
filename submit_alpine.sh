#!/bin/bash
#SBATCH --mem=64G
#SBATCH --cpus-per-task=16
#SBATCH --time=24:00:00
#SBATCH --output=/scratch/alpine/seda0001_amc/logs/slurm-%j.out

# Usage:
# sbatch submit_alpine.sh <run_ids(; separated)> <sample_id>
set -e

echo "working in $SLURM_SCRATCH"

export GOOGLE_APPLICATION_CREDENTIALS=$HOME/curatedmetagenomicdata-232f4a306d1d.json
module load singularity
module load git
module load nextflow

cd $SLURM_SCRATCH
export NXF_MODE=google
#nextflow run main.nf --run_ids=$1 --sample_id=$2 -profile alpine
nextflow run seandavi/curatedMetagenomicsNextflow --run_ids=$1 --sample_id=$2 -profile alpine
