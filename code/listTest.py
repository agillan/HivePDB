__author__ = 'Ana Gillan 2014'
import sys
import string


while True:
    #read in line from Hadoop stream
    line = sys.stdin.readline()

    #break when no (more) lines
    if not line:
        break

    #get rid of newline character, tokenize line on tab character and assign variable names
    line = string.strip(line, "\n ")
    pdbid, pdbx_PDB_model_num, label_asym_id, label_seq_id, label_comp_id, atoms = string.split(line, "\t")
    
    #evaluate atoms map into a python dictionary type
    atoms = eval(atoms)

	r1 = atoms.get("N")


    #convert any number of returnable results to strings
    result = str(r1)
    
    #print desired columns separated by tabs
    print "\t".join([pdbid, result])


# Example Hive query using python script #

# ADD FILE /home/local/zslf023/listTest.py;    
# SELECT TRANSFORM(*)
# USING "python listTest.py" as (id, result)
# FROM coordinates_grouped
# CLUSTER BY id;

