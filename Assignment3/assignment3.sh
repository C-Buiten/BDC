#!/bin/bash

source activate /commons/conda/dsls;

FILE="/commons/Themas/Thema12/HPC/rnaseq.fastq";

python3 assignment3.py -n 16 FILE
