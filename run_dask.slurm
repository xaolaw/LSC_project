#!/bin/bash
#SBATCH --partition plgrid-gpu-a100
#SBATCH --nodes 2
#SBATCH --ntasks-per-node 4
#SBATCH --mem-per-cpu=4G
#SBATCH --time 0:30:00
#SBATCH --job-name dask-test

cd $SCRATCH

module load GCC/10.3.0
module load OpenMPI/4.1.1
module load dask/2021.9.1

## module load matplotlib/3.4.2

# export SLURM_OVERLAP=1

# mpirun dask-mpi --no-nanny --local-directory $SCRATCH/dask_tmp/workers --scheduler-file $SCRATCH/dask_tmp/scheduler.json &
python $HOME/LSC/pro/site_sonar_dask.py
# mpirun python $HOME/LSC/pro/site_sonar_dask.

