__author__ = 'Ana Gillan, Royal Holloway, University of London 2014'

'''
This is a template script for transforming queries from the atom_coordinates table

'''
#import any necessary modules
import sys
import string

'''
Example Hive query using python script


ADD FILE /<local_path_to_script>/<SCRIPT_NAME>.py;

SELECT TRANSFORM(*)
USING "python <SCRIPT_NAME>.py" as (id string, col1 col_data_type, col2 col_data_type, col3... etc)
FROM atom_coordinates
CLUSTER BY id, col1, col1, etc;

As example, with output from outputline() function:
SELECT TRANSFORM(*)
USING "python templateTransformScript.py" as (id string, model int, chain string, resid int, res string, atom string, x double, y double, z double)
FROM atom_coordinates
CLUSTER BY id, model, chain, resid;



INPUT
example input from atom_coordinates (10 columns, tab separated)

1MZK	1	ATOM	N	LEU	A	3	-17.224	-11.09	-2.949
1MZK	1	ATOM	CA	LEU	A	3	-16.453	-10.263	-3.868
1MZK	1	ATOM	C	LEU	A	3	-17.234	-10.006	-5.155
1MZK	1	ATOM	O	LEU	A	3	-18.298	-10.587	-5.373
1MZK	1	ATOM	CB	LEU	A	3	-15.105	-10.918	-4.188
1MZK	1	ATOM	CG	LEU	A	3	-14.44	-11.651	-3.02
1MZK	1	ATOM	CD1	LEU	A	3	-14.714	-13.145	-3.095
1MZK	1	ATOM	CD2	LEU	A	3	-12.942	-11.386	-3.013
1MZK	1	ATOM	HA	LEU	A	3	-16.271	-9.318	-3.38
1MZK	1	ATOM	HB2	LEU	A	3	-15.254	-11.623	-4.993
1MZK	1	ATOM	HB3	LEU	A	3	-14.431	-10.146	-4.527
1MZK	1	ATOM	HG	LEU	A	3	-14.85	-11.282	-2.091
1MZK	1	ATOM	N	GLY	A	4	-16.706	-9.121	-5.998
1MZ2	1	ATOM	N	LEU	A	3	-17.224	-11.09	-2.949





OUTPUT
convert any number of returnable results to strings

result1 = str(EXPRESSION)
result2 = str(EXPRESSION)
etc...

print desired columns separated by tabs to return to Hadoop stream

print "\t".join([pdbid, result1, result2, result3,...,etc])

'''


##### CLASS DEFINITIONS #####
class PDBEntry:
    def __init__(self, pdbid):
        self.pdbid = pdbid
        self.models = []

    def addmodel(self, mdl):
        if mdl.pdbid == self.pdbid:
            if len(self.models) == 0:
                self.models.append(mdl)
            else:
                for model in self.models:
                    if model.modelid == mdl.modelid:
                        break
                    else:
                        self.models.append(mdl)

    def addchain(self, chn):
        if chn.pdbid == self.pdbid:
            for model in self.models:
                if model.modelid == chn.modelid:
                    if len(model.chains) == 0:
                        model.chains.append(chn)
                    else:
                        for chain in model.chains:
                            if chain.chainid == chn.chainid:
                                break
                            else:
                                model.chains.append(chn)

    def addres(self, res):
        if res.pdbid == self.pdbid:
            for model in self.models:
                if model.modelid == res.modelid:
                    for chain in model.chains:
                        if chain.chainid == res.chainid:
                            if len(chain.residues) == 0:
                                chain.residues.append(res)
                            else:
                                for residue in chain.residues:
                                    if residue.resseq == res.resseq:
                                        break
                                    else:
                                        chain.residues.append(res)

    def addatom(self, atm):
        if atm.pdbid == self.pdbid:
            for model in self.models:
                if model.modelid == atm.modelid:
                    for chain in model.chains:
                        if chain.chainid == atm.chainid:
                            for residue in chain.residues:
                                if residue.resseq == atm.resseq:
                                    residue.atoms.append(atm)


class Model:
    def __init__(self, pdbid, modelid):
        self.pdbid = pdbid

        self.modelid = modelid
        self.chains = []


class Chain:
    def __init__(self, pdbid, modelid, label_asym_id):
        self.pdbid = pdbid
        self.modelid = modelid

        self.chainid = label_asym_id
        self.residues = []


class Residue:
    def __init__(self, pdbid, modelid, label_asym_id, label_comp_id, label_seq_id):
        self.pdbid = pdbid
        self.modelid = modelid
        self.chainid = label_asym_id

        self.residue = label_comp_id
        self.resseq = label_seq_id
        self.atoms = []

    def getatom(self, atomtype):
        atomdict = {}
        for atom in self.atoms:
            if atom.id == atomtype:
                atomdict = {"id": atom.id, "coordinates": [atom.x, atom.y, atom.z]}
            else:
                atomdict = {"id": None, "coordinates": [0, 0, 0]}
        return atomdict


class Atom:
    def __init__(self, pdbid, modelid, label_asym_id, label_seq_id, atom):
        self.pdbid = pdbid
        self.modelid = modelid
        self.chainid = label_asym_id
        self.resseq = label_seq_id

        self.group = atom.get("group")
        self.id = atom.get("id")
        self.x = atom.get("x")
        self.y = atom.get("y")
        self.z = atom.get("z")


##### PROCESSING METHODS #####

#process the line to break it up into its constituent parts
def processline(line):
    #get rid of newline character
    line = string.strip(line, "\n ")

    #tokenize line on tab character and assign variable names
    pdbid, pdbx_PDB_model_num, group_pdb, label_atom_id, label_comp_id, label_asym_id, label_seq_id, x, y, z = string.split(line, "\t")

    #create atom dict
    atom = {"group": group_pdb, "id": label_atom_id, "x": x, "y": y, "z": z}

    return pdbid, pdbx_PDB_model_num, label_comp_id, label_asym_id, label_seq_id, atom


#example for creating output
def outputline(entry):
    #convert each element in PDBEntry object to a string and then output one line per atom to hive
    for model in entry.models:
        for chain in model.chains:
            for residue in chain.residues:
                for atom in residue.atoms:
                    mdl = str(model.modelid)
                    chn = str(chain.chainid)
                    resid = str(residue.resseq)
                    restype = str(residue.residue)
                    atmid = str(atom.id)
                    x = str(atom.x)
                    y = str(atom.y)
                    z = str(atom.z)

                    #print out desired columns separated by tabs. Number of columns output MUST be the same as in Hive query.
                    print "\t".join([entry.pdbid, mdl, chn, resid, restype, atmid, x, y, z])



#####################################
#                                   #
#                                   #
#                                   #
#   PUT ANY CUSTOM METHODS HERE     #
#                                   #
#                                   #
#                                   #
# ###################################




##### "Main" Method #####

#set up placeholder values for current PDBEntry object and previous PDB id
pdb = None
lastpdb = None

while True:

    # read in line from Hadoop stream
    line = sys.stdin.readline()

    # break when no (more) lines
    if not line:
        break

    try:
        # otherwise process line into its constituent parts
        pdbid, pdbx_PDB_model_num, label_comp_id, label_asym_id, label_seq_id, atom = processline(line)

        # create objects for model, chain, residue and atom referred to in line
        mdl = Model(pdbid, pdbx_PDB_model_num)
        chn = Chain(pdbid, pdbx_PDB_model_num, label_asym_id)
        res = Residue(pdbid, pdbx_PDB_model_num, label_asym_id, label_comp_id, label_seq_id)
        atm = Atom(pdbid, pdbx_PDB_model_num, label_asym_id, label_seq_id, atom)

        # assemble PDBEntry object with associated models, chains, residues and atoms
        if pdbid != lastpdb:
            if pdb:
                outputline(pdb)  # pass the entire PDBEntry object to desired functions for processing here. Output to Hive when finished calculations.

            #create new pdbentry with latest pdb
            pdb = PDBEntry(pdbid)
            #link together
            pdb.addmodel(mdl)
            pdb.addchain(chn)
            pdb.addres(res)
            pdb.addatom(atm)

            lastpdb = pdbid  # keep track of PDB ID of previous line
        else:  # if current line refers to current PDB
            pdb.addmodel(mdl)
            pdb.addchain(chn)
            pdb.addres(res)
            pdb.addatom(atm)
    except ValueError:
        continue