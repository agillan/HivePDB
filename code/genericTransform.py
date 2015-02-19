__author__ = 'Ana Gillan, Royal Holloway, University of London 2014'

import sys
import string

'''
Takes in one row per PDB entry
col1 = "pdbid-extatom"
col2 = array of atom record strings

atom string format: "ATOM    1   A    A      1      1  ?  .    C    C   O  O5'  O5'    23.308    21.309    18.480    1.00   17.20     1    ?"
split on whitespace to obtain required fields

SELECT TRANSFORM(*)
USING "python objectTransform.py" as (id string, atom string)
FROM pdb_xml_load
CLUSTER BY id;

'''
##### PROCESSING METHODS #####
def processline(line):
    #get rid of newline character
    line = string.strip(line, "\n ")

    #tokenize line on tab character and assign variable names
    pdbid, atoms = string.split(line, "\t")

    pdbid, tmp = string.split(pdbid, "-")
    atoms = eval(atoms)

    return pdbid, atoms


def ismalformed(atom):
    if len(atom) != 20:
        return True
    if atom[0] != "HETATM" and (atom[2] == "." or atom[2] == "?" or atom[4] == "." or atom[8] == "." or atom[8] == "?" or atom[11] == "." or atom[11] == "?" or atom[13] == "." or atom[13] == "?" or atom[14] == "." or atom[14] == "?" or atom[15] == "." or atom[15] == "?"):
        return True
    return False


def main(line):
    pdbid, atoms = processline(line)

    for atom in atoms:
        atom = string.split(atom)
        if ismalformed(atom):
            return
        else:
            print "\t".join([pdbid, str(atom)])


## SCRIPT ##
while True:

    # read in line from Hadoop stream
    line = sys.stdin.readline()

    # break when no (more) lines
    if not line:
        break

    main(line)



