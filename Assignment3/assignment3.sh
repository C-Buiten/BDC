#!/bin/bash
#SBATCH --job-name=BDC_Assignment3
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mail-user=c.van.buiten@st.hanze.nl
#SBATCH --mail-type=ALL
#SBATCH --partition=assemblix


source /commons/conda/conda_load.sh
export data=/data/dataprocessing/MinIONData/all.fq
export ref=/data/dataprocessing/MinIONData/all_bacteria.fna


cores=16

for i in $(seq 1 $cores);
do
  /usr/bin/time -o timings.txt --append -f "${i}\t%e" minimap2 $ref $data -t "${i}" -a > /dev/null
done

