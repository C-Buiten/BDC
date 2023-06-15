#!/bin/bash
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16

source activate /commons/conda/dsls;

export FILE="/commons/Themas/Thema12/HPC/rnaseq.fastq";


python3 assignment3.py -n 16 $FILE

