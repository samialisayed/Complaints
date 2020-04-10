# -*- coding: utf-8 -*-
"""
Created on Mon Mar  9 15:25:00 2020

@author: sami
"""
import sys
from mrjob.job import MRJob, MRStep
import csv
import mapreduce as mr

class MRTask(MRJob):
    
    def mapper1(self, _k1, v1):
        yield ((v1[3],v1[0]), float(v1[4]))
    
    def reducer1(self, k2, v2s):
        yield k2, sum(v2s)
    
    def mapper2(self, k1, v1):
        product_id, customer_id = k1    
        yield (product_id, (v1))
    
    def reducer2(self, k2, v2s):
        yield (k2,len(v2s),sum(v2s))

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2),
        ]
        
        
with open(sys.argv[1], 'r') as fi:
    reader = csv.reader(fi)
    output1 = list(mr.runJob(enumerate(reader), MRTask(args=[])))

with open(sys.argv[2],'w', newline='') as out_file:
    writer = csv.writer(out_file)
    writer.writerow(['Product ID', 'Customer Count', 'Total Revenue'])
    for row in output1:
        writer.writerow(row)