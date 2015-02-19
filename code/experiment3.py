__author__ = 'ana'
import sys
import string

counter = 0

while True:
    #read in line from Hadoop stream
    line = sys.stdin.readline()

    #break when no(more) lines
    if not line:
        break

    #get rid of newline character, tokenize line on tab character and assign variable names
    line = string.strip(line, "\n ")
    pdbid,label_asym_id,label_seq_id, label_comp_id, atoms = string.split(line, "\t")

    #evaluate atoms map into a python dictionary type
    atoms = eval(atoms)

    counter += 1

    #convert any number of returnable results to strings
    result = str(counter)

    #print desired columns separated by tabs
    print "\t".join([pdbid, result])