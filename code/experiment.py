#experiment.py: concatenates id, comp and atom using the word "test" for each row passed to it

import sys
import string
import hashlib

while True:
  line = sys.stdin.readline()
  if not line:
    break

  line = string.strip(line, "\n ")
  pdbid, label_comp_id, label_atom_id  = string.split(line, "\t")
  res = pdbid + 'TEST' + label_comp_id + 'TEST' + label_atom_id
  print "\t".join([pdbid, res])
  
  

# SELECT TRANSFORM(pdbid,label_comp_id,label_atom_id)
# USING 'python experiment.py' AS (pdbid string, res string)
# from coordinates;

# 11AS 	11ASTESTALATESTN
# 11AS 	11ASTESTALATESTCA
# 11AS 	11ASTESTALATESTC
# 11AS 	11ASTESTTYRTESTN
# 11AS 	11ASTESTTYRTESTCA

# SELECT TRANSFORM(pdbid,label_comp_id,label_atom_id)
# USING 'python experiment.py' AS (pdbid string, res string)
# from coordinates
# where label_atom_id in("N","CA","C");

# user cluster by or distribute by to avoid things only going to a single mapper/making sure keys go to right reducer