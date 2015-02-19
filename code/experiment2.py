import sys
import string
import decimal

while True:
    line = sys.stdin.readline()
    if not line:
        break

    line = string.strip(line, "\n ")
    pdbid, pdbx_PDB_model_num, label_asym_id, label_seq_id, label_comp_id, atoms = string.split(line, "\t")
    atoms = eval(atoms)
    carbon = atoms.get("CA")
    res = str(carbon[0])
    	
    print "\t".join([pdbid, res])